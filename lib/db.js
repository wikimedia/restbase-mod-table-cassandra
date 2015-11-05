"use strict";

var P = require('bluebird');
var cass = require('cassandra-driver');
var TimeUuid = cass.types.TimeUuid;
var extend = require('extend');
var dbu = require('./dbutils');
var cassID = dbu.cassID;
var revPolicy = require('./revisionPolicy');
var SchemaMigrator = require('./schemaMigration');
var secIndexes = require('./secondaryIndexes');

/** @const */
var validTextConsistencies = {all:1, localOne: 1, localQuorum:1};

function DB (client, options) {
    this.conf = options.conf;
    this.log = options.log;

    this.defaultConsistency = cass.types.consistencies[this.conf.defaultConsistency]
        || cass.types.consistencies.localOne;

    // cassandra client
    this.client = client;

    this._initCaches();

    /* Process the array of storage groups declared in the config */
    this.storageGroups = this._buildStorageGroups(options.conf.storage_groups);
    /* The cache holding the already-resolved domain-to-group mappings */
    this.storageGroupsCache = {};
}

DB.prototype._initCaches = function() {
    // keyspace -> schema
    this.schemaCache = {};
    // JSON.stringify([domain, table]) -> keyspace
    this.keyspaceNameCache = {};
    // keyspace -> boolean: replication factors checked / updated
    this.replicationUpdateCache = {};
};


/**
 * Set up internal request-related information and wrap it into an
 * InternalRequest instance.
 */
DB.prototype._makeInternalRequest = function (domain, table, query, consistency) {
    consistency = consistency || this.defaultConsistency;
    if (query.consistency && query.consistency in validTextConsistencies) {
        consistency = cass.types.consistencies[query.consistency];
    }
    var keyspace = this._keyspaceName(domain, table);
    var opts = {
        domain: domain,
        table: table,
        keyspace: keyspace,
        query: query,
        consistency: consistency,
        columnfamily: 'data',
        schema: this.schemaCache[keyspace]
    };
    if (query && query.attributes && query.attributes._ttl) {
        opts.ttl = query.attributes._ttl;
        delete query.attributes._ttl;
    }
    var req = new InternalRequest(opts);
    if (req.schema) {
        return P.resolve(req);
    } else {
        return this._fetchSchema(req);
    }
};

/**
 * Fetch a logical table schema from <keyspace>.meta, key 'schema'.
 * @param {InternalRequest} req
 * @return {object} schema
 */
DB.prototype._fetchSchema = function(req) {
    var self = this;

    var schemaQuery = {
        attributes: {
            key: 'schema'
        },
        limit: 1
    };
    var schemaReq = req.extend({
        query: schemaQuery,
        columnfamily: 'meta',
        schema: this.infoSchemaInfo
    });
    return this._getRaw(schemaReq)
    .then(function(res) {
        if (res.items.length) {
            // Need to parse the JSON manually here as we are using the
            // internal _getRaw(), which doesn't apply transforms.
            var schema = JSON.parse(res.items[0].value);
            schema = dbu.validateAndNormalizeSchema(schema);
            self.schemaCache[req.keyspace] = req.schema = dbu.makeSchemaInfo(schema);
        }
        return req;
    }, function(err) {
        if (/^Keyspace .* does not exist$/.test(err.message)) {
            return req;
        } else {
            // Check if the meta column family exists
            return self.client.execute_p('SELECT columnfamily_name FROM '
                + 'system.schema_columnfamilies WHERE keyspace_name=? '
                + 'and columnfamily_name=?', [req.keyspace, 'meta'])
            .then(function (res) {
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
};

/**
 * Process the storage group configuration.
 *
 * @param {Array} the array of group objects to read, each must contain
 *                at least the name and domains keys
 * @return {Array} Array of storage group objects
 */
DB.prototype._buildStorageGroups = function (groups) {
    var storageGroups = [];
    if(!Array.isArray(groups)) {
        return storageGroups;
    }
    groups.forEach(function(group) {
        var grp = extend(true, {}, group);
        if(!Array.isArray(grp.domains)) {
            grp.domains = [grp.domains];
        }
        grp.domains = grp.domains.map(function(domain) {
            if(/^\/.*\/$/.test(domain)) {
                return new RegExp(domain.slice(1, -1));
            }
            return domain;
        });
        storageGroups.push(grp);
    });
    return storageGroups;
};

/**
 * Derive a valid keyspace name from a random bucket name. Try to use valid
 * chars from the requested name as far as possible, but fall back to a sha1
 * if not possible. Also respect Cassandra's limit of 48 or fewer alphanum
 * chars & first char being an alpha char.
 *
 * @param {string} domain in dot notation
 * @param {string} table, the logical table name
 * @return {string} Valid Cassandra keyspace key
 */
DB.prototype._keyspaceName = function (domain, table) {
    var cacheKey = JSON.stringify([domain,table]);
    var cachedName = this.keyspaceNameCache[cacheKey];
    if (cachedName) {
        return cachedName;
    }

    var name = this._resolveStorageGroup(domain).name;
    var reversedName = name.toLowerCase().split('.').reverse().join('.');
    var prefix = dbu.makeValidKey(reversedName, Math.max(26, 48 - table.length - 3));
    var res = prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
        + '_T_' + dbu.makeValidKey(table, 48 - prefix.length - 3);
    this.keyspaceNameCache[cacheKey] = res;
    return res;
};

/**
 * Finds the storage group for a given domain.
 *
 * @param {String} domain the domain's name
 * @return {Object} the group object matching the domain
 */
DB.prototype._resolveStorageGroup = function (domain) {
    var group = this.storageGroupsCache[domain];
    var idx;
    if(group) {
        return group;
    }
    // not found in cache, find it
    for(idx = 0; idx < this.storageGroups.length; idx++) {
        var curr = this.storageGroups[idx];
        var domIdx;
        for(domIdx = 0; domIdx < curr.domains.length; domIdx++) {
            var dom = curr.domains[domIdx];
            if(((dom instanceof RegExp) && dom.test(domain)) ||
                    (typeof dom === 'string' && dom === domain)) {
                group = curr;
                break;
            }
        }
        if(group) {
            break;
        }
    }
    if(!group) {
        // no group found, assume the domain is to
        // be grouped by itself
        group = {
            name: domain,
            domain: [domain]
        };
    }
    // save it in the cache
    this.storageGroupsCache[domain] = group;
    return group;
};


// Info table schema
DB.prototype.infoSchema = dbu.validateAndNormalizeSchema({
    table: 'meta',
    attributes: {
        key: 'string',
        value: 'json',
        tid: 'timeuuid'
    },
    index: [
        { attribute: 'key', type: 'hash' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ],
    secondaryIndexes: {}
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema, true);

DB.prototype.get = function (domain, query) {
    var self = this;
    return this._makeInternalRequest(domain, query.table, query)
    .then(function(req) {
        return self._getRaw(req)
        .then(function(res) {
            // Apply value conversions
            res.items = dbu.convertRows(res.items, req.schema);
            return res;
        });
    });
};

DB.prototype._getRaw = function (req, options) {
    var self = this;
    options = options || {};

    if (!req.schema) {
        throw new Error("restbase-mod-table-cassandra: No schema for " + req.keyspace
                + ', table: ' + req.columnfamily);
    }

    if (!req.schema.iKeyMap) {
        self.log('error/cassandra/no_iKeyMap', req.schema);
    }

    // Index queries are currently handled in buildGetQuery. See
    // https://phabricator.wikimedia.org/T78722 for secondary index TODOs.
    //if (req.index) {
    //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    //}

    // Paging request:
    var cassOpts = {consistency: req.consistency, prepare: true};

    if (req.query.limit) {
        cassOpts.fetchSize = req.query.limit;

        if (req.query.next) {
            cassOpts.pageState = new Buffer( req.query.next, 'base64');
        }
    }

    var buildResult = dbu.buildGetQuery(req, options);
    return self.client.execute_p(buildResult.cql, buildResult.params, cassOpts)
    .then(function(result){
        var rows = result.rows;
        // Decorate the row result with the _ttl attribute.
        if (options.withTTLs) {
            rows.forEach(dbu.assignMaxTTL);
        }
        var length = rows.length;
        for (var i = 0; i < length; i++) {
            if (rows[i]._del) {
                rows.splice(i,1);
                i--;
                length--;
            }
        }
        if (result.meta && result.meta.pageState) {
            var token = result.meta.pageState.toString('base64');
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
};

/*
    Handler for request GET requests on secondary indexes.
    This is currently not used. TODO: fix.
*/
//DB.prototype._getSecondaryIndex = function(keyspace, domain, req,
//        consistency, table, buildResult){
//
//    // TODO: handle '_tid' cases
//    var self = this;
//    return self.client.execute_p(buildResult.cql, buildResult.params,
//            {consistency: consistency, prepare: true})
//    .then(function(results) {
//        var queries = [];
//        var cachedSchema = self.schemaCache[keyspace];
//
//        // convert the result values
//        results.rows.forEach(function (row) {
//            dbu.convertRow(row, cachedSchema);
//        });
//
//        var newReq = {
//            table: table,
//            attributes: {},
//            limit: req.limit + Math.ceil(req.limit/4)
//        };
//
//        // build main data queries
//        for ( var rowno in results.rows ) {
//            for ( var attr in cachedSchema.iKeyMap ) {
//                newReq.attributes[attr] = results.rows[rowno][attr];
//            }
//            queries.push(dbu.buildGetQuery(keyspace, domain, newReq, consistency, table, cachedSchema));
//            newReq.attributes = {};
//        }
//
//        // prepare promises for batch execution
//        var batchPromises = [];
//        queries.forEach(function(item) {
//            batchPromises.push(self.client.execute_p(item.cql, item.params,
//                        item.options || {consistency: consistency, prepare: true}));
//        });
//
//        // execute batch and check if limit is fulfilled
//        return P.all(batchPromises).then(function(batchResults){
//            var finalRows = [];
//            batchResults.forEach(function(item){
//                if (finalRows.length < req.limit) {
//                    finalRows.push(dbu.convertRow(item.rows[0], cachedSchema));
//                }
//            });
//            return [finalRows, results.rows[rowno]];
//        });
//    })
//    .then(function(rows){
//        //TODO: handle case when limit > no of entries in table
//        if (rows[0].length<req.limit) {
//            return self.indexReads(keyspace, domain, req, consistency, table, rows[1], rows[0]);
//        }
//        return rows[0];
//    }).then(function(rows){
//        // hide the columns property added by node-cassandra-cql
//        // XXX: submit a patch to avoid adding it in the first place
//        for (var row in rows) {
//            row.columns = undefined;
//        }
//        return {
//            items: rows
//        };
//    });
//};

/*
    Fetch index entries and compare them against data row for false positives
    - if limit is fullfilled return
    - else fetch more entries and compare again
*/
DB.prototype.indexReads = function(keyspace, domain, req, consistency, table, startKey, finalRows) {

    // create new index query
    var newIndexReq = {
        table: table,
        index: req.index,
        attributes: {},
        limit: req.limit + Math.ceil(req.limit/4)
    };

    var internalColumns = {
        _del: true,
        _tid: true
    };

    var cachedSchema = this.schemaCache[keyspace].secondaryIndexes[req.index];
    for(var item in startKey) {
        if ( !internalColumns[item] && cachedSchema._attributeIndexes[item]) {
            if (cachedSchema.attributes[item] === 'timeuuid' ) {
                // TODO : change 'le' to requested range conditions
                newIndexReq.attributes[item] = {'le': startKey[item]};
            } else {
                newIndexReq.attributes[item] = startKey[item];
            }
        }
    }

    var buildResult = dbu.buildGetQuery(keyspace, newIndexReq, consistency,
            table, cachedSchema);

    var self = this;
    var queries = [];
    var lastrow;
    return new P(function(resolve, reject){
        // stream  the main data table
        var stream = self.client.stream(buildResult.cql, buildResult.params,
                    {
                        autoPage: true,
                        fetchSize: req.limit + Math.ceil(req.limit/4),
                        prepare: true,
                        consistency: consistency
                    })
        .on('readable', function(){
            var row = dbu.convertRow(this.read(), cachedSchema);
            for (row; row !== null; row=dbu.convertRow(this.read(), cachedSchema)) {
                lastrow = row;
                var attributes = {};
                var proj = {};
                for (var attr in self.schemaCache[keyspace].iKeyMap) {
                    attributes[attr] = row[attr];
                }
                queries.push(dbu.buildGetQuery(keyspace, {
                                                table: table,
                                                attributes: attributes,
                                                proj: proj,
                                                limit: req.limit + Math.ceil(req.limit/4)},
                                                consistency, table, cachedSchema));
                var finalRows = [];
                if (finalRows.length<req.limit) {
                    return self.indexReads(keyspace, req, consistency, table, lastrow, finalRows);
                } else {
                    return self.client.execute_p(item.cql, item.params,
                            item.options || {consistency: consistency, prepare: true})
                    .then(function(results){
                        if (finalRows.length < req.limit) {
                            finalRows.push(dbu.convertRow(results.rows[0], cachedSchema));
                        }
                    });
                }
            }
        });
    });
};

DB.prototype.put = function (domain, query) {
    return this._makeInternalRequest(domain, query.table, query)
    .bind(this)
    .then(this._put);
};


DB.prototype._put = function(req) {
    var self = this;

    if (!req.schema) {
        throw new Error('Table not found!');
    }
    var schema = req.schema;
    var query = req.query;

    var tid = query.attributes[schema.tid];
    if (!tid) {
        query.attributes[schema.tid] = TimeUuid.now();
    } else if (tid.constructor === String) {
        query.attributes[schema.tid] = TimeUuid.fromString(query.attributes[schema.tid]);
    }

    query.timestamp = dbu.tidNanoTime(query.attributes[schema.tid]);

    // insert into secondary Indexes first
    var batch = [];
    if (schema.secondaryIndexes) {
        for ( var idx in schema.secondaryIndexes) {
            var secondarySchema = schema.secondaryIndexes[idx];
            if (!secondarySchema) {
                throw new Error('Table not found!');
            }
            //if (query.attributes.uri) { console.log(query.attributes.uri, query.timestamp); }
            var idxReq = req.extend({
                columnfamily: dbu.idxColumnFamily(idx),
                schema: secondarySchema
            });

            // Don't send over all attributes, only those existing in a secondary index table
            var newQueryAttributes = {};
            Object.keys(idxReq.query.attributes).forEach(function(attrName) {
                if (secondarySchema.attributes[attrName]) {
                    newQueryAttributes[attrName] = idxReq.query.attributes[attrName];
                }
            });
            idxReq.query = Object.assign({}, idxReq.query);
            idxReq.query.attributes = newQueryAttributes;

            batch.push(dbu.buildPutQuery(idxReq));
        }
    }

    batch.push(dbu.buildPutQuery(req));

    //console.log(batch, schema);
    var queryOptions = {consistency: req.consistency, prepare: true};
    var update;
    if (batch.length === 1) {
        // Single query only (no secondary indexes): no need for a batch.
        var queryInfo = batch[0];
        update = this.client.execute_p(queryInfo.cql, queryInfo.params, queryOptions);
    } else {
        var driverBatch = batch.map(function(queryInfo) {
            return {
                query: queryInfo.cql,
                params: queryInfo.params
            };
        });
        update = this.client.batch_p(driverBatch, queryOptions);
    }

    if (self._shouldRunBackgroundUpdates(schema))
    {
        update = update.then(function() {
            return self._backgroundUpdates(req, 3);
        });
    }

    return update.then(function(result) {
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
};

/*
 * Predicate: should we run background updates?
 *
 * @param {object} schema; the table schema
 * @param {array} (optional) indexes to process
 * @return {boolean} whether to apply background updates
 */
DB.prototype._shouldRunBackgroundUpdates = function(schema, indexes) {
    if (!indexes) {
        indexes = Object.keys(schema.secondaryIndexes);
    }
    // If there are no indexes, and the retention policy is 'all' (i.e.
    // there are no revisions to cull), then there is no need to run
    // background updates.
    return indexes.length
        || schema.revisionRetentionPolicy && schema.revisionRetentionPolicy.type !== 'all';
};

/*
 * Post-put background updates
 *
 * Queries for sibling revisions (1 newer and up to 'limit' older), and applies
 * both secondary index updates (IndexRebuilder), and the table's revision
 * retention policy (RevisionPolicyManager).
 *
 * @param {object} InternalRequest; pass in an empty query to match / update
 *        all entries
 * @param {number} (optional) limit; The maximum number of entries to include
 *        in the updates
 * @param  {array} (optional) indexes; an array of index names to update;
 *        Default: all indexes in the schema
 */
DB.prototype._backgroundUpdates = function(req, limit, indexes) {
    var self = this;
    var schema = req.schema;
    var query = req.query;

    indexes = indexes || Object.keys(schema.secondaryIndexes);
    if (!self._shouldRunBackgroundUpdates(schema, indexes)) {
        // nothing to do.
        return P.resolve();
    }

    var consistency = cass.types.consistencies.localOne;
    var tidKey = schema.tid;

    // Build a new request for the main data table
    var dataQuery = {
        table: query.table,
        attributes: {},
    };

    var newerProj = ['_del'];

    // Narrow down the update to the original request's primary key. If
    // that's empty, the entire index (within the numerical limits) will be updated.
    schema.iKeys.forEach(function(att) {
        if (att !== tidKey) {
            dataQuery.attributes[att] = query.attributes[att];
            newerProj.push(att);
        }
    });

    // Select indexed attributes for all indexes to rebuild
    var secondaryKeySet = {};
    indexes.forEach(function(idx) {
        // console.log(idx, JSON.stringify(schema.secondaryIndexes));
        Object.keys(schema.attributeIndexes).forEach(function(att) {
            if (!schema.iKeyMap[att] && !secondaryKeySet[att]) {
                secondaryKeySet[att] = true;
                newerProj.push(att);
            }
        });
    });
    var secondaryKeys = Object.keys(secondaryKeySet);

    if (!secondaryKeySet[tidKey]) {
        newerProj.push(tidKey);
    }

    // XXX: handle the case where reqTid is not defined!
    var reqTid = query.attributes[schema.tid];
    var reqTime = dbu.tidNanoTime(reqTid);

    // Clone the query, and create le & gt variants
    var newerDataQuery = extend(true, {}, dataQuery);
    // 1) select one newer index entry
    newerDataQuery.attributes[schema.tid] = { 'ge': reqTid };
    newerDataQuery.order = {};
    newerDataQuery.order[schema.tid] = 'asc'; // select sibling entries
    newerDataQuery.limit = 2; // data entry + newer entry
    newerDataQuery.proj = newerProj;
    var newerRebuildRequest = req.extend({
        query: newerDataQuery
    });

    // XXX: handle the case where reqTid is not defined?
    var indexRebuilder = new secIndexes.IndexRebuilder(self, req, secondaryKeys, reqTime);
    var policyManager = new revPolicy.RevisionPolicyManager(self, req, schema, reqTime);
    var handler = new UpdateHandler([indexRebuilder, policyManager]);

    // Query for a window that includes 1 newer record (if any exists), and up
    // to 'limit' later records.  Run the list of update handlers across each
    // matching row, in descending order.
    return self._getRaw(newerRebuildRequest, { withTTLs: true })
    .then(function(res) {    // Query for one record previous
        return P.try(function() {
            return P.each(res.items.reverse(), indexRebuilder.handleRow.bind(indexRebuilder))
            .then(function() {
                if (res.items.length) {
                    // Send just added data to policyManager to handle cases with 0 count
                    return policyManager.handleRow(res.items[0]);
                }
            });
        });
    })
    .catch(function(e) {
        // Should always find something here
        self.log('error/cassandra/backgroundUpdates', e);
        throw e;
    })
    .then(function() {       // Query for 'limit' subsequent records
        dataQuery.attributes[schema.tid] = {'lt': reqTid};

        // Traverse the bulk of the data, in timestamp descending order
        // (reverse chronological)
        var dataGetReq = req.extend({
            query: dataQuery,
            columnfamily: 'data',
        });
        var dataGetInfo = dbu.buildGetQuery(dataGetReq, { withTTLs: true, limit: limit });

        return dbu.eachRow(
            self.client,
            dataGetInfo.cql,
            dataGetInfo.params,
            {retries: 3, fetchSize: 5, log: self.log, withTTLs: true},
            handler.handleRow.bind(handler)
        );
    })
    .catch(function(e) {
        // We might not always have older versions, so a 404 is okay here.
        if (e.status !== 404) {
            self.log('error/cassandra/backgroundUpdates', e);
            throw e;
        }
    });
};

DB.prototype.delete = function (domain, query) {
    return this._makeInternalRequest(domain, query.table, query)
    .bind(this)
    .then(this._delete);
};

DB.prototype._delete = function (req) {

    // Mark _del with current timestamp and update the row.
    req.query.attributes._del = TimeUuid.now();

    return this._put(req);
};

/**
 * Evaluate, and if neccessary, perform a migration from one back-end version
 * to another.
 *
 * @param  {object} req;  the current request object
 * @param  {object} from; schema info object representing current state
 * @param  {object} to;   schema info object representing proposed state
 * @return {boolean} a promise that resolves to true if a back-end migration
 *         occurred.
 */
DB.prototype._migrateBackend = function(req, from, to) {
    // Perform a backend migration, as-needed.
    switch(from._backend_version) {
    case 0:
        return this._dropDomainIndex(req);
    default:
        return P.resolve();
    }
};

/**
 * Conditionally performs a table schema and/or back-end migration.
 *
 * @param  {object} req;               the current request object
 * @param  {object} currentSchemaInfo; schema info object for current schema
 * @param  {object} newSchema;         the proposed schema
 * @param  {object} newSchemaInfo;     schema info object for proposed schema
 * @return {object} HTTP response
 */
DB.prototype._migrateIfNecessary = function(req, currentSchemaInfo, newSchema, newSchemaInfo) {
    var self = this;

    var migrationPromise = P.resolve();
    var noopPromise = migrationPromise;
    var schemaWriteNeeded = false;

    // First, check if the replication factors need to be updated in line with
    // the cassandra module configuration.
    if (!this.replicationUpdateCache[req.keyspace]) {
        self.log('info/cassandra/replication_update_check',
                'Checking replication factor: ' + req.keyspace);
        migrationPromise = self._updateReplicationIfNecessary(req.domain,
                req.query.table, req.query.options)
        .then(function() {
            // Remember the successful replication check / update
            self.replicationUpdateCache[req.keyspace] = true;
        });
    }

    // The _backend_version attribute is excluded from the schema hash
    // calculation so that we can evaluate these different classes of
    // change (_backend_version versus table schema) separately.  If
    // backend versions differ, a backend migration will need to be
    // performed first.  Afterward, if and only if there has been a
    // change to the schema (that is if the hashes do not match), will
    // a schema migration occur. These schema changes must also include
    // a monotonically increasing version number.  Finally, if either
    // (or both) of a backend or table schema migration occur, the new
    // JSON blob will be persisted.

    // Then, carry out any back-end migration (if needed).
    if (currentSchemaInfo._backend_version !== newSchemaInfo._backend_version) {
        self.log('info/cassandra/backend_version_mismatch',
                'Backend version mismatch: ' + req.keyspace);
        // A downgrade (unsupported)!
        if (newSchemaInfo._backend_version < currentSchemaInfo._backend_version) {
            throw new dbu.HTTPError({
                status: 400,
                body: {
                    type: 'bad_request',
                    title: 'Unable to downgrade storage backend to version '+newSchemaInfo._backend_version,
                    keyspace: req.keyspace,
                    schema: newSchema
                }
            });
        }

        migrationPromise = migrationPromise.then(function() {
            return self._migrateBackend(req, currentSchemaInfo, newSchemaInfo);
        });
        schemaWriteNeeded = true;
    }

    // Next carry out any table schema migration (if needed).
    if (currentSchemaInfo.hash !== newSchemaInfo.hash) {
        self.log('info/cassandra/schema_hash_mismatch',
                'Schema hash mismatch: ' + currentSchemaInfo.hash
                + ' != ' + newSchemaInfo.hash);
        var migrator;
        try {
            migrator = new SchemaMigrator(
                self,
                req,
                currentSchemaInfo,
                newSchemaInfo);
        }
        catch (error) {
            throw new dbu.HTTPError({
                status: 400,
                body: {
                    type: 'bad_request',
                    title: 'The table already exists, and it cannot be upgraded to the requested schema (' + error + ').',
                    keyspace: req.keyspace,
                    schema: newSchema,
                    stack: error.stack,
                }
            });
        }
        migrationPromise = migrationPromise.then(migrator.migrate.bind(migrator));
        schemaWriteNeeded = true;
    }

    // Finally, update the stored schema if it changed.
    if (schemaWriteNeeded) {
        // Force a cache update on subsequent requests
        self.schemaCache[req.keyspace] = null;
        migrationPromise = migrationPromise
        .then(function() {
            var putReq = req.extend({
                columnfamily: 'meta',
                schema: self.infoSchemaInfo,
                query: {
                    attributes: {
                        key: 'schema',
                        value: newSchema
                    }
                }
            });
            return self._put(putReq);
        });
    }

    if (migrationPromise !== noopPromise) {
        return migrationPromise
        .then(function() {
            return {
                status: 201
            };
        })
        .catch(function(error) {
            self.log('error/cassandra/table_update', error);
            throw error;
        });
    } else {
        return {
            status: 201
        };
    }
};

DB.prototype.createTable = function (domain, query) {
    var self = this;
    if (!query.table) {
        throw new Error('Table name required.');
    }

    return this._makeInternalRequest(domain, query.table, query)
    .catch(function(err) {
        self.log('error/cassandra/table_creation', err);
        throw err;
    })
    .then(function(req) {
        var currentSchemaInfo = req.schema;

        // Validate and normalize the schema
        var newSchema = dbu.validateAndNormalizeSchema(req.query);
        var newSchemaInfo = dbu.makeSchemaInfo(newSchema);

        if (currentSchemaInfo) {
            // Table already exists
            return self._migrateIfNecessary(req, currentSchemaInfo, newSchema, newSchemaInfo);
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
        var retries = 100; // We try really hard.
        var delay = 100; // Start with a 1ms delay
        function doCreateTables() {
            return self._createKeyspace(req)
            .then(function() {
                return self._createTable(req, newSchemaInfo, 'data');
            })
            // TODO: create indexes here rather than implicitly in
            // _createTable?
            .then(function() {
                return self._createTable(req, self.infoSchemaInfo, 'meta');
            })
            .then(function() {
                // Only store the schema after everything else was created
                var putReq = req.extend({
                    columnfamily: 'meta',
                    schema: self.infoSchemaInfo,
                    query: {
                        attributes: {
                            key: 'schema',
                            value: newSchema
                        }
                    }
                });
                return self._put(putReq)
                .then(function() {
                    // No need to update replication settings for just-created
                    // table.
                    self.replicationUpdateCache[req.keyspace] = true;
                    return {
                        status: 201
                    };
                });
            })
            .catch(function(e) {
                // TODO: proper error reporting:
                if (retries--) {
                    //console.error('Retrying..', retries, e);
                    // Increase the delay by a factor of 2 on average
                    delay = delay * (1.5 + Math.random());
                    return P.delay(delay).then(doCreateTables);
                } else {
                    self.log('error/cassandra/table_creation', e);
                    throw e;
                }
            });

        }

        return doCreateTables();
    });
};

DB.prototype._createTable = function (req, schema, columnfamily) {
    var self = this;

    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + columnfamily);
    }

    var tasks = P.resolve();
    if (schema.secondaryIndexes) {
        // Create secondary indexes
        Object.keys(schema.secondaryIndexes).forEach(function(idx) {
            var indexSchema = schema.secondaryIndexes[idx];
            tasks = tasks.then(function() {
                return self._createTable(req, indexSchema, dbu.secondaryIndexTableName(idx));
            });
        });
    }

    var statics = {};
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        }
    });

    // Finally, create the main data table
    var cql = 'create table if not exists '
        + cassID(req.keyspace) + '.' + cassID(columnfamily) + ' (';
    for (var attr in schema.attributes) {
        var type = schema.attributes[attr];
        cql += cassID(attr) + ' ' + dbu.schemaTypeToCQLType(type);
        if (statics[attr]) {
            cql += ' static';
        }
        cql += ', ';
    }

    var hashBits = [];
    var rangeBits = [];
    var orderBits = [];
    schema.index.forEach(function(elem) {
        var cassName = cassID(elem.attribute);
        if (elem.type === 'hash') {
            hashBits.push(cassName);
        } else if (elem.type === 'range') {
            rangeBits.push(cassName);
            orderBits.push(cassName + ' ' + elem.order);
        }
    });

    cql += 'primary key (';
    cql += ['(' + hashBits.join(',') + ')'].concat(rangeBits).join(',') + '))';

    // Add options for compression & compaction
    cql += " WITH " + dbu.getOptionCQL(schema.options || {});

    if (orderBits.length) {
        cql += ' and clustering order by ( ' + orderBits.join(',') + ' )';
    }

    //console.log(cql);

    // TODO: If the table already exists, check that the schema actually
    // matches / can be upgraded!
    // See https://phabricator.wikimedia.org/T75808.
    this.log('warn/table/cassandra/createTable', {
        message: 'Creating CF ' + columnfamily + ' in keyspace ' + req.keyspace,
        columnfamily: columnfamily,
        keyspace : req.keyspace
    });

    // Execute the table creation query
    return tasks.then(function() {
        return self.client.execute_p(cql, [], {consistency: req.consistency});
    });
};

// Drop the native secondary indexes we used to create on the "_domain" column.
DB.prototype._dropDomainIndex = function(req) {
    var self = this;
    var cql = "select index_name from system.schema_columns where keyspace_name = ? "
        + " and columnfamily_name = ? and column_name = '_domain';";
    return self.client.execute_p(cql, [req.keyspace, req.columnfamily], {prepare: true})
    .then(function(res) {
        if (res.rows.length && res.rows[0].index_name) {
            // drop the index
            return self.client.execute_p('drop index if exists ' + cassID(req.keyspace)
                    + '.' + cassID(res.rows[0].index_name));
        }
    });
};

DB.prototype._createKeyspace = function (req) {
    var cql = 'create keyspace if not exists ' + cassID(req.keyspace)
        + ' WITH REPLICATION = ' + this._createReplicationOptionsCQL(req.query.options);
    return this.client.execute_p(cql, [],
            {consistency: req.consistency || this.defaultConsistency});
};

DB.prototype._createReplicationOptionsCQL = function(options) {
    var cql = "{ 'class': 'NetworkTopologyStrategy'";
    var replicas = this._replicationPolicy(options);

    Object.keys(replicas).forEach(function(dc) {
        cql += ", '" + dc + "': " + replicas[dc];
    });

    cql += '}';
    return cql;
};

DB.prototype._replicationPolicy = function(options) {
    var durability = (options && options.durability === 'low') ? 1 : 3;
    var replicas = {};
    this.conf.datacenters.forEach(function(dc) {
        replicas[dc] = durability;
    });
    return replicas;
};

DB.prototype.dropTable = function (domain, table) {
    var keyspace = this._keyspaceName(domain, table);
    this.schemaCache[keyspace] = null;
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [],
            {consistency: this.defaultConsistency});
};

DB.prototype.getTableSchema = function(domain, table) {
    var req = new InternalRequest({
        domain: domain,
        table: table,
        keyspace: this._keyspaceName(domain, table),
        query: { attributes: { key: 'schema' }, limit: 1 },
        consistency: this.defaultConsistency,
        columnfamily: 'meta',
        schema: this.infoSchemaInfo
    });
    return this._getRaw(req)
    .then(function(response) {
        if (!response.items.length) {
            throw new dbu.HTTPError({
                status: 404,
                body: {
                    type: 'notfound',
                    title: 'the requested table schema was not found'
                }
            });
        }
        var item = response.items[0];
        return { tid: item.tid, schema: JSON.parse(item.value) };
    });
};

/**
 * Retrieves the current replication options for a keyspace.
 *
 * @param  {string} domain; the domain name
 * @param  {string} table;  the table name
 * @return {object} promise that yields an associative array of datacenters with
 *                  corresponding replication counts
 */
DB.prototype._getReplication = function(domain, table) {
    var keyspace = this._keyspaceName(domain, table);
    var ks = this.client.metadata.keyspaces[keyspace];
    var datacenters = {};
    if (!ks) {
        return datacenters;
    }
    Object.keys(ks.strategyOptions).forEach(function (dc) {
        datacenters[dc] = parseInt(ks.strategyOptions[dc]);
    });
    return P.resolve(datacenters);
};

/**
 * ALTERs a Cassandra keyspace to match the replication policy, (a function of the
 * configured datacenters, and the requested durability).
 *
 * @param  {string} domain;  the domain name
 * @param  {string} table;   the table name
 * @param  {object} options; query options from the initiating request
 * @return {object} promise that resolves when complete
 */
DB.prototype._setReplication = function(domain, table, options) {
    var keyspace = this._keyspaceName(domain, table);
    var cql = "ALTER KEYSPACE " + dbu.cassID(keyspace) + " WITH replication = " + this._createReplicationOptionsCQL(options);
    this.log('warn/cassandra/replication', {
        message: 'Updating replication for ' + keyspace,
        replicas: this._replicationPolicy(options),
        durability: options && options.durability || null
    });
    return this.client.execute_p(cql, [], {consistency: this.defaultConsistency});
};

/**
 * Evaluates whether current keyspace replication matches the policy (a function of
 * the configured datacenters, and the requested durability); Updates replication
 * if necessary.
 *
 * NOTE: All this does is ALTER the underlying Cassandra keyspace, a repair (or
 * cleanup) is still necessary.
 *
 * @param  {string} domain;  the domain name
 * @param  {string} table;   the table name
 * @param  {object} options; query options from the initiating request
 * @return {object} promise that resolves when complete
 */
DB.prototype._updateReplicationIfNecessary = function(domain, table, options) {
    // returns true if two objects have matching keys and values
    function matching(current, expected) {
        if (Object.keys(current).length !== Object.keys(expected).length) {
            return false;
        }
        return Object.keys(current).every(function(a) {
            return current[a] === expected[a];
        });
    }

    var self = this;
    return self._getReplication(domain, table)
    .then(function(current) {
        if (!matching(current, self._replicationPolicy(options))) {
            return self._setReplication(domain, table, options);
        }
    });
};

/**
 * Wrap common internal request state
 */
function InternalRequest (opts) {
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
 */
InternalRequest.prototype.extend = function(opts) {
    var req = new InternalRequest(this);
    Object.keys(opts).forEach(function(key) {
        req[key] = opts[key];
    });
    return req;
};

/**
 * Convenience class for wrapping objects that perform by-row updates.
 *
 * @param {array} handlers; a list of objects, each of which must have a
 *        handleRow method that accepts a row object, and returns a promise.
 */
function UpdateHandler(handlers) {
    this.handlers = handlers;
}

/**
 * Invokes handleRow on each of the child handlers with the supplied row.
 *
 * @param  {object} row; a row object.
 * @return a promise that resolves when the constituent promises do.
 */
UpdateHandler.prototype.handleRow = function(row) {
    return P.map(this.handlers, function(handler) {
        return handler.handleRow(row);
    });
};

module.exports = DB;
