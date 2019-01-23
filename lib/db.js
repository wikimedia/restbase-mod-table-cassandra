'use strict';

const P = require('bluebird');
const cass = require('cassandra-driver');
const extend = require('extend');
const dbu = require('./dbutils');
const cassID = dbu.cassID;
const SchemaMigrator = require('./schemaMigration');

/** @const */
const validTextConsistencies = { all: 1, localOne: 1, localQuorum: 1 };

/**
 * Wrap common internal request state
 */
class InternalRequest {
    constructor(opts) {
        this.domain = opts.domain;
        this.table = opts.table;
        this.keyspace = opts.keyspace;
        this.query = opts.query || null;
        this.consistency = opts.consistency;
        this.schema = opts.schema || null;
        this.columnfamily = opts.columnfamily || 'data';
        this.ttl = opts.ttl || null;
    }

    /**
     * Construct a new InternalRequest based on an existing one, optionally
     * overriding existing properties.
     * @param  {Object} opts  overriding settings
     * @return {Object}       new request
     */
    extend(opts) {
        const req = new InternalRequest(this);
        Object.keys(opts).forEach((key) => {
            req[key] = opts[key];
        });
        return req;
    }
}

class DB {
    constructor(client, options) {
        this.conf = options.conf;
        this.log = options.log;

        this.defaultConsistency = cass.types.consistencies[this.conf.defaultConsistency] ||
            cass.types.consistencies.localOne;

        // cassandra client
        this.client = client;

        this._initCaches();

        /* Process the array of storage groups declared in the config */
        this.storageGroups = this._buildStorageGroups(options.conf.storage_groups);
        /* The cache holding the already-resolved domain-to-group mappings */
        this.storageGroupsCache = {};

        this.cassandraVersion = this._getCassandraVersion();

        if (!this.conf.version) {
            this.conf.version = dbu.DEFAULT_CONFIG_VERSION;
        }

        this.infoSchema = dbu.validateAndNormalizeSchema({
            table: 'meta',
            attributes: {
                key: 'string',
                value: 'json'
            },
            index: [
                { attribute: 'key', type: 'hash' }
            ]
        });
        this.infoSchemaInfo = dbu.makeSchemaInfo(this.infoSchema, true);
    }

    _initCaches() {
        // keyspace -> schema
        this.schemaCache = {};
        // JSON.stringify([domain, table]) -> keyspace
        this.keyspaceNameCache = {};
    }

    /**
     * Set up internal request-related information and wrap it into an
     * InternalRequest instance.
     * @param  {string}  domain      in dot notation
     * @param  {string}  table       logical table name
     * @param  {Object}  query       query object
     * @param  {Object}  consistency consistency level
     * @return {Object}
     */
    _makeInternalRequest(domain, table, query, consistency) {
        consistency = consistency || this.defaultConsistency;
        if (query.consistency && query.consistency in validTextConsistencies) {
            consistency = cass.types.consistencies[query.consistency];
        }
        const keyspace = this.keyspaceName(domain, table);
        const opts = {
            domain,
            table,
            keyspace,
            query,
            consistency,
            columnfamily: 'data',
            schema: this.schemaCache[keyspace]
        };
        if (query && query.attributes && query.attributes._ttl) {
            opts.ttl = query.attributes._ttl;
            delete query.attributes._ttl;
        }
        const req = new InternalRequest(opts);
        if (req.schema) {
            return P.resolve(req);
        } else {
            return this._fetchSchema(req);
        }
    }

    /**
     * Fetch a logical table schema from <keyspace>.meta, key 'schema'.
     * @param {InternalRequest} req
     * @return {Object} schema
     */
    _fetchSchema(req) {
        const schemaQuery = {
            attributes: {
                key: 'schema'
            },
            limit: 1
        };
        const schemaReq = req.extend({
            query: schemaQuery,
            columnfamily: 'meta',
            schema: this.infoSchemaInfo
        });
        return this._getRaw(schemaReq)
        .then((res) => {
            if (res.items.length) {
                // Need to parse the JSON manually here as we are using the
                // internal _getRaw(), which doesn't apply transforms.
                let schema = JSON.parse(res.items[0].value);
                schema = dbu.validateAndNormalizeSchema(schema);
                this.schemaCache[req.keyspace] = req.schema = dbu.makeSchemaInfo(schema);
            }
            return req;
        }, (err) => {
            if (/^Keyspace .* does not exist$/.test(err.message)) {
                return req;
            } else {
                // Check if the meta column family exists
                return this.client.execute('SELECT table_name FROM system_schema.tables ' +
                    'WHERE keyspace_name=? and table_name=?', [req.keyspace, 'meta'])
                .then((res) => {
                    if (res && res.rows.length === 0) {
                        // meta column family doesn't exist yet
                        return req;
                    } else {
                        // re-throw error
                        throw err;
                    }
                });
            }
        });
    }

    /**
     * Derive a valid keyspace name from a random bucket name. Try to use valid
     * chars from the requested name as far as possible, but fall back to a sha1
     * if not possible. Also respect Cassandra's limit of 48 or fewer alphanum
     * chars & first char being an alpha char.
     * @param  {string} domain  in dot notation
     * @param  {string} table   the logical table name
     * @return {string}         Valid Cassandra keyspace key
     */
    keyspaceName(domain, table) {
        const cacheKey = JSON.stringify([domain, table]);
        const cachedName = this.keyspaceNameCache[cacheKey];
        if (cachedName) {
            return cachedName;
        }

        const name = this._resolveStorageGroup(domain).name;
        const reversedName = name.toLowerCase().split('.').reverse().join('.');
        const prefix = dbu.makeValidKey(reversedName, Math.max(26, 48 - table.length - 3));
        // 6 chars _hash_ to prevent conflicts between domains & table names
        const res = `${prefix}_T_${dbu.makeValidKey(table, 48 - prefix.length - 3)}`;
        this.keyspaceNameCache[cacheKey] = res;
        return res;
    }

    /**
     * Finds the storage group for a given domain.
     * @param  {string} domain  the domain's name
     * @return {Object}         the group object matching the domain
     */
    _resolveStorageGroup(domain) {
        let group = this.storageGroupsCache[domain];
        let idx;
        if (group) {
            return group;
        }
        // not found in cache, find it
        for (idx = 0; idx < this.storageGroups.length; idx++) {
            const curr = this.storageGroups[idx];
            let domIdx;
            for (domIdx = 0; domIdx < curr.domains.length; domIdx++) {
                const dom = curr.domains[domIdx];
                if (((dom instanceof RegExp) && dom.test(domain)) ||
                        (typeof dom === 'string' && dom === domain)) {
                    group = curr;
                    break;
                }
            }
            if (group) {
                break;
            }
        }
        if (!group) {
            // no group found, assume the domain is to
            // be grouped by itthis
            group = {
                name: domain,
                domain: [domain]
            };
        }
        // save it in the cache
        this.storageGroupsCache[domain] = group;
        return group;
    }

    get(domain, query) {
        return this._makeInternalRequest(domain, query.table, query)
        .then((req) => {
            const options = query.withTTL ? { withTTL: true } : undefined;
            return this._getRaw(req, options)
            .then((res) => {
                // Apply value conversions
                res.items = dbu.convertRows(res.items, req.schema);
                return res;
            });
        });
    }

    _getRaw(req, options) {
        options = options || {};

        if (!req.schema) {
            throw new Error('restbase-mod-table-cassandra: ' +
                `No schema for ${req.keyspace}, table: ${req.columnfamily}`);
        }

        if (!req.schema.iKeyMap) {
            this.log('error/cassandra/no_iKeyMap', req.schema);
        }

        // Index queries are currently handled in buildGetQuery. See
        // https://phabricator.wikimedia.org/T78722 for secondary index TODOs.
        // if (req.index) {
        //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
        // }

        // Paging request:
        const cassOpts = { consistency: req.consistency, prepare: true };

        if (req.query.limit) {
            cassOpts.fetchSize = req.query.limit;

            if (req.query.next) {
                cassOpts.pageState = new Buffer(req.query.next, 'base64');
            }
        }

        const buildResult = dbu.buildGetQuery(req, options);
        return this.client.execute(buildResult.cql, buildResult.params, cassOpts)
        .then((result) => {
            const rows = result.rows;
            // Decorate the row result with the _ttl attribute.
            if (options.withTTL) {
                rows.forEach(dbu.assignMaxTTL);
            }
            if (result.meta && result.meta.pageState) {
                const token = result.meta.pageState.toString('base64');
                return {
                    items: rows,
                    next: token
                };
            } else {
                return {
                    items: rows
                };
            }
        });
    }

    put(domain, query) {
        return this._makeInternalRequest(domain, query.table, query)
        .bind(this)
        .then(this._put);
    }

    _put(req) {
        if (!req.schema) {
            throw new Error('Table not found!');
        }
        const queryOptions = { consistency: req.consistency, prepare: true };
        const queryInfo = dbu.buildPutQuery(req);

        return this.client.execute(queryInfo.cql, queryInfo.params, queryOptions).thenReturn({
            // XXX: check if condition failed!
            status: 201
        });
    }

    delete(domain, query) {
        return this._makeInternalRequest(domain, query.table, query)
        .bind(this)
        .then(this._delete);
    }

    _delete(req) {
        const queryOptions = { consistency: req.consistency, prepare: true };
        const queryInfo = dbu.buildDeleteQuery(req);
        return this.client.execute(queryInfo.cql, queryInfo.params, queryOptions).thenReturn({
            status: 204
        });
    }

    /**
     * Evaluate, and if neccessary, perform a migration from one back-end version
     * to another.
     * @param  {Object} req  the current request object
     * @param  {Object} from schema info object representing current state
     * @return {boolean} a promise that resolves to true if a back-end migration
     *         occurred.
     */
    _migrateBackend(req, from) {
        // Perform a backend migration, as-needed.
        /* eslint-disable indent */
        switch (from._backend_version) {
            case 0:
                return this._dropDomainIndex(req);
            default:
                return P.resolve();
        }
        /* eslint-enable indent */
    }

    /**
     * Conditionally performs a table schema and/or back-end migration.
     * @param  {Object} req               the current request object
     * @param  {Object} currentSchemaInfo schema info object for current schema
     * @param  {Object} newSchema         the proposed schema
     * @param  {Object} newSchemaInfo     schema info object for proposed schema
     * @return {Object} HTTP response
     */
    _migrateIfNecessary(req, currentSchemaInfo, newSchema, newSchemaInfo) {
        if (currentSchemaInfo.hash === newSchemaInfo.hash) {
            // The fast & standard case. Hashes match, nothing changed.
            return {
                status: 201
            };
        } else {
            // Carry out any backend, config or schema migrations.
            this.log('warn/cassandra/schema_hash_mismatch',
                `Schema hash mismatch: ${currentSchemaInfo.hash} != ${newSchemaInfo.hash}`);
            const migrator = new SchemaMigrator({
                db: this,
                client: this.client,
                log: this.log,
                skip_schema_update: this.conf.skip_schema_update
            });
            return P.try(() => migrator.migrate(req, currentSchemaInfo, newSchemaInfo))
            .then((migrated) => {
                if (migrated) {
                    // Force a cache update on subsequent requests
                    this.schemaCache[req.keyspace] = null;
                    // Update the stored schema if it changed.
                    const putReq = req.extend({
                        columnfamily: 'meta',
                        schema: this.infoSchemaInfo,
                        query: {
                            attributes: {
                                key: 'schema',
                                value: newSchema
                            }
                        }
                    });
                    return this._put(putReq);
                } else {
                    // Yield to avoid stack overflow when there are many
                    // migrations that aren't applied.
                    return P.delay(0);
                }
            })
            .then(() => ({
                status: 201
            }))
            .catch((error) => {
                const newErr = new dbu.HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        title: 'The table already exists, and it cannot ' +
                            `be upgraded to the requested schema (${error}).`,
                        keyspace: req.keyspace,
                        schema: newSchema,
                        stack: error.stack
                    }
                });
                this.log('error/cassandra/table_update', newErr);
                throw newErr;
            });
        }
    }

    createTable(domain, query) {
        if (!query.table) {
            throw new Error('Table name required.');
        }

        return this._makeInternalRequest(domain, query.table, query)
        .catch((err) => {
            this.log('error/cassandra/table_creation', err);
            throw err;
        })
        .then((req) => {
            const currentSchemaInfo = req.schema;

            // Validate and normalize the schema
            const newSchema = dbu.validateAndNormalizeSchema(req.query, this.conf.version);
            const newSchemaInfo = dbu.makeSchemaInfo(newSchema);

            if (currentSchemaInfo) {
                // Table already exists
                return this._migrateIfNecessary(req, currentSchemaInfo, newSchema, newSchemaInfo);
            }

            // Cassandra does not like concurrent keyspace creation. This is
            // especially significant on the first restbase startup, when many workers
            // compete to create the system tables. It is also relevant for complex
            // bucket creation, which also often involves the concurrent creation of
            // several sub-buckets backed by keyspaces and tables.
            //
            // The typical issue is getting errors like this:
            // org.apache.cassandra.exceptions.ConfigurationException: Column family
            // ID mismatch
            //
            // See https://issues.apache.org/jira/browse/CASSANDRA-8387 for
            // background.
            //
            // So, our work-around is to retry a few times before giving up.  Our
            // table creation code is idempotent, which makes this a safe thing to do.
            let retries = 100; // We try really hard.
            let delay = 100; // Start with a 1ms delay
            const doCreateTables = () => {
                return this._createKeyspace(req)
                .then(() => this._createTable(req, newSchemaInfo, 'data'))
                // TODO: create indexes here rather than implicitly in
                // _createTable?
                .then(() => this._createTable(req, this.infoSchemaInfo, 'meta'))
                .then(() => {
                    // Only store the schema after everything else was created
                    const putReq = req.extend({
                        columnfamily: 'meta',
                        schema: this.infoSchemaInfo,
                        query: {
                            attributes: {
                                key: 'schema',
                                value: newSchema
                            }
                        }
                    });
                    return this._put(putReq)
                    .then(() => ({
                        status: 201
                    }));
                })
                .catch((e) => {
                    // TODO: proper error reporting:
                    if (retries--) {
                        // Increase the delay by a factor of 2 on average
                        delay *= 1.5 + Math.random();
                        return P.delay(delay).then(doCreateTables);
                    } else {
                        this.log('error/cassandra/table_creation', e);
                        throw e;
                    }
                });

            };

            return doCreateTables();
        });
    }

    _getCassandraVersion() {
        try {
            return this.client.controlConnection.host.cassandraVersion;
        } catch (e) {
            this.log('error/cassandraVersion', e);
        }
    }

    _createTable(req, schema, columnfamily) {
        if (!schema.attributes) {
            throw new Error(`No attribute definitions for table ${columnfamily}`);
        }

        const statics = {};
        schema.index.forEach((elem) => {
            if (elem.type === 'static') {
                statics[elem.attribute] = true;
            }
        });

        // Finally, create the main data table
        let cql = `create table if not exists ${cassID(req.keyspace)}.${cassID(columnfamily)} (`;
        Object.keys(schema.attributes).forEach((attr) => {
            const type = schema.attributes[attr];
            cql += `${cassID(attr)} ${dbu.schemaTypeToCQLType(type)}`;
            if (statics[attr]) {
                cql += ' static';
            }
            cql += ', ';
        });

        const hashBits = [];
        const rangeBits = [];
        const orderBits = [];
        schema.index.forEach((elem) => {
            const cassName = cassID(elem.attribute);
            if (elem.type === 'hash') {
                hashBits.push(cassName);
            } else if (elem.type === 'range') {
                rangeBits.push(cassName);
                orderBits.push(`${cassName} ${elem.order}`);
            }
        });

        cql += 'primary key (';
        cql += `${[`(${hashBits.join(',')})`].concat(rangeBits).join(',')}))`;

        let clusteringCQL;
        if (orderBits.length) {
            clusteringCQL = `clustering order by (${orderBits.join(',')})`;
        }
        // Add options for default_time_to_live
        const additionalOptions = [
            clusteringCQL,
            dbu.getOptionCQL(schema.options || {}, this)
        ].filter(Boolean);
        if (additionalOptions.length) {
            cql += ` WITH ${additionalOptions.join(' and ')}`;
        }

        // TODO: If the table already exists, check that the schema actually
        // matches / can be upgraded!
        // See https://phabricator.wikimedia.org/T75808.
        this.log('warn/table/cassandra/createTable', {
            message: `Creating CF ${columnfamily} in keyspace ${req.keyspace}`,
            columnfamily,
            keyspace: req.keyspace
        });

        // Execute the table creation query
        return this.client.execute(cql, [], { consistency: req.consistency });
    }

    // Drop the native secondary indexes we used to create on the "_domain" column.
    _dropDomainIndex(req) {
        const cql = 'select index_name from system.schema_columns where keyspace_name = ? ' +
            " and columnfamily_name = ? and column_name = '_domain';";
        return this.client.execute(cql, [req.keyspace, req.columnfamily], { prepare: true })
        .then((res) => {
            if (res.rows.length && res.rows[0].index_name) {
                // drop the index
                return this.client.execute('drop index if exists ' +
                    `${cassID(req.keyspace)}.${cassID(res.rows[0].index_name)}`);
            }
        });
    }

    _createKeyspace(req) {
        const cql = `create keyspace if not exists ${cassID(req.keyspace)} ` +
            `WITH REPLICATION = ${this._createReplicationOptionsCQL(req.query.options)}`;
        return this.client.execute(cql, [],
            { consistency: req.consistency || this.defaultConsistency });
    }

    _createReplicationOptionsCQL(options) {
        let cql = "{ 'class': 'NetworkTopologyStrategy'";
        const replicas = this._replicationPolicy(options);

        Object.keys(replicas).forEach((dc) => {
            cql += `, '${dc}': ${replicas[dc]}`;
        });

        cql += '}';
        return cql;
    }

    _replicationPolicy(options) {
        const durability = (options && options.durability === 'low') ? 1 : 3;
        const replicas = {};
        this.conf.datacenters.forEach((dc) => {
            replicas[dc] = durability;
        });
        return replicas;
    }

    dropTable(domain, table) {
        const keyspace = this.keyspaceName(domain, table);
        this.schemaCache[keyspace] = null;
        return this.client.execute(`drop keyspace ${cassID(keyspace)}`, [],
            { consistency: this.defaultConsistency });
    }

    getTableSchema(domain, table) {
        const req = new InternalRequest({
            domain,
            table,
            keyspace: this.keyspaceName(domain, table),
            query: { attributes: { key: 'schema' }, limit: 1 },
            consistency: this.defaultConsistency,
            columnfamily: 'meta',
            schema: this.infoSchemaInfo
        });
        return this._getRaw(req)
        .then((response) => {
            if (!response.items.length) {
                throw new dbu.HTTPError({
                    status: 404,
                    body: {
                        type: 'notfound',
                        title: 'the requested table schema was not found'
                    }
                });
            }
            const item = response.items[0];
            return { schema: JSON.parse(item.value) };
        });
    }

    /**
     * Retrieves the current replication options for a keyspace.
     * @param  {string} domain  the domain name
     * @param  {string} table   the table name
     * @return {Object} promise that yields an associative array of datacenters with
     *                  corresponding replication counts
     */
    _getReplication(domain, table) {
        const keyspace = this.keyspaceName(domain, table);
        const ks = this.client.metadata.keyspaces[keyspace];
        const datacenters = {};
        if (!ks) {
            return datacenters;
        }
        Object.keys(ks.strategyOptions).forEach((dc) => {
            datacenters[dc] = parseInt(ks.strategyOptions[dc], 10);
        });
        return P.resolve(datacenters);
    }

    /**
     * ALTERs a Cassandra keyspace to match the replication policy, (a function of the
     * configured datacenters, and the requested durability).
     * @param  {string} domain  the domain name
     * @param  {string} table   the table name
     * @param  {Object} options query options from the initiating request
     * @return {Object} promise that resolves when complete
     */
    _setReplication(domain, table, options) {
        const keyspace = this.keyspaceName(domain, table);
        const cql = `ALTER KEYSPACE ${dbu.cassID(keyspace)} WITH ` +
            `replication = ${this._createReplicationOptionsCQL(options)}`;
        this.log('warn/cassandra/replication', {
            message: `Updating replication for ${keyspace}`,
            replicas: this._replicationPolicy(options),
            durability: options && options.durability || null
        });
        this.log('trace/alter_schema', cql);
        if (this.conf.skip_schema_update) {
            return P.resolve();
        }
        return this.client.execute(cql, [], { consistency: this.defaultConsistency });
    }

    /**
     * Evaluates whether current keyspace replication matches the policy (a function of
     * the configured datacenters, and the requested durability); Updates replication
     * if necessary.
     *
     * NOTE: All this does is ALTER the underlying Cassandra keyspace, a repair (or
     * cleanup) is still necessary.
     * @param  {string} domain  the domain name
     * @param  {string} table   the table name
     * @param  {Object} options query options from the initiating request
     * @return {Object} promise that resolves when complete
     */
    updateReplicationIfNecessary(domain, table, options) {
        // returns true if two objects have matching keys and values
        const matching = (current, expected) => {
            if (Object.keys(current).length !== Object.keys(expected).length) {
                return false;
            }
            return Object.keys(current).every((a) => current[a] === expected[a]);
        };

        return this._getReplication(domain, table)
        .then((current) => {
            if (!matching(current, this._replicationPolicy(options))) {
                return this._setReplication(domain, table, options);
            }
        });
    }

    /**
     * Process the storage group configuration.
     * @param {Array} groups the array of group objects to read, each must contain
     *                at least the name and domains keys
     * @return {Array} Array of storage group objects
     */
    _buildStorageGroups(groups) {
        const storageGroups = [];
        if (!Array.isArray(groups)) {
            return storageGroups;
        }
        groups.forEach((group) => {
            const grp = extend(true, {}, group);
            if (!Array.isArray(grp.domains)) {
                grp.domains = [grp.domains];
            }
            grp.domains = grp.domains.map((domain) => {
                if (/^\/.*\/$/.test(domain)) {
                    return new RegExp(domain.slice(1, -1));
                }
                return domain;
            });
            storageGroups.push(grp);
        });
        return storageGroups;
    }
}

module.exports = DB;
