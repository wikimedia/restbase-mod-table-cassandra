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


function DB (client, options) {
    this.conf = options.conf;
    this.log = options.log;

    this.defaultConsistency = cass.types.consistencies[this.conf.defaultConsistency]
        || cass.types.consistencies.one;

    // cassandra client
    this.client = client;

    this._initSchemaCache();

    /* Process the array of storage groups declared in the config */
    this.storageGroups = this._buildStorageGroups(options.conf.storage_groups);
    /* The cache holding the already-resolved domain-to-group mappings */
    this.storageGroupsCache = {};
}

DB.prototype._initSchemaCache = function() {
    // cache keyspace -> schema
    this.schemaCache = {};
    this.keyspaceSchemaCache = {};
    this.keyspaceNameCache = {};
};

/**
 * Set up internal request-related information and wrap it into an
 * InternalRequest instance.
 */
DB.prototype._makeInternalRequest = function (domain, table, query, consistency) {
    var self = this;
    consistency = consistency || this.defaultConsistency;
    if (query.consistency && query.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[query.consistency];
    }
    var cacheKey = JSON.stringify([domain,table]);
    var req = new InternalRequest({
        domain: domain,
        table: table,
        keyspace: this.keyspaceNameCache[cacheKey]
            || this._keyspaceName(domain, table),
        query: query,
        consistency: consistency,
        columnfamily: 'data',
        schema: this.schemaCache[cacheKey]
    });
    if (!req.schema) {
        // Share the schema across domains that map to the same keyspace
        req.schema = this.keyspaceSchemaCache[req.keyspace];
    }
    if (req.schema) {
        return P.resolve(req);
    } else {
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
                self.keyspaceNameCache[cacheKey] = req.keyspace;
                self.schemaCache[cacheKey] = req.schema = dbu.makeSchemaInfo(schema);
                self.keyspaceSchemaCache[req.keyspace] = req.schema;
            }
            return req;
        }, function(err) {
            // Check if the keyspace & meta column family exists
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
        });
    }
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
    var name = this._resolveStorageGroup(domain).name;
    var reversedName = name.toLowerCase().split('.').reverse().join('.');
    var prefix = dbu.makeValidKey(reversedName, Math.max(26, 48 - table.length - 3));
    return prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
        + '_T_' + dbu.makeValidKey(table, 48 - prefix.length - 3);
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

DB.prototype._getRaw = function (req) {
    var self = this;

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
    var options = {consistency: req.consistency, prepare: true};

    if (req.query.limit) {
        options.fetchSize = req.query.limit;

        if (req.query.next) {
            options.pageState = new Buffer( req.query.next, 'base64');
        }
    }

    var buildResult = dbu.buildGetQuery(req);
    return self.client.execute_p(buildResult.cql, buildResult.params, options)
    .then(function(result){
        var rows = result.rows;
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
            batch.push(dbu.buildPutQuery(idxReq));
        }
    }

    batch.push(dbu.buildPutQuery(req));

    //console.log(batch, schema);
    var queryOptions = {consistency: req.consistency, prepare: true};
    var mainUpdate;
    if (batch.length === 1) {
        // Single query only (no secondary indexes): no need for a batch.
        var queryInfo = batch[0];
        mainUpdate = this.client.execute_p(queryInfo.cql, queryInfo.params, queryOptions);
    } else {
        var driverBatch = batch.map(function(queryInfo) {
            return {
                query: queryInfo.cql,
                params: queryInfo.params
            };
        });
        mainUpdate = this.client.batch_p(driverBatch, queryOptions);
    }

    return mainUpdate

    .then(function(result) {
        // Kick off asynchronous local index rebuild
        if (schema.secondaryIndexes) {
            self._backgroundUpdates(req, 3)
            .catch(function(err) {
                self.log('error/cassandra/backgroundUpdates', err);
            });
        }

        // But don't wait for it. Return success straight away.
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
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

    // If there are no indexes, and the retention policy is 'all' (i.e.
    // there are no revisions to cull), then there is no need to go further.
    if (!indexes.length && schema.revisionRetentionPolicy.type === 'all') {
        return P.resolve();
    }

    var consistency = cass.types.consistencies.one;
    var tidKey = schema.tid;

    // Build a new request for the main data table
    var dataQuery = {
        table: query.table,
        attributes: {},
    };

    // Narrow down the update to the original request's primary key. If
    // that's empty, the entire index (within the numerical limits) will be updated.
    schema.iKeys.forEach(function(att) {
        if (att !== tidKey) {
            dataQuery.attributes[att] = query.attributes[att];
        }
    });

    // Select indexed attributes for all indexes to rebuild
    var secondaryKeySet = {};
    indexes.forEach(function(idx) {
        // console.log(idx, JSON.stringify(schema.secondaryIndexes));
        Object.keys(schema.attributeIndexes).forEach(function(att) {
            if (!schema.iKeyMap[att] && !secondaryKeySet[att]) {
                secondaryKeySet[att] = true;
            }
        });
    });
    var secondaryKeys = Object.keys(secondaryKeySet);

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
    return self._getRaw(newerRebuildRequest)
    .then(function(res) {    // Query for one record previous
        return P.try(function() {
            return P.each(res.items.reverse(), handler.handleRow.bind(handler));
        }).catch(function(err) {
            // just log it
            self.log('error/cassandra/backgroundUpdates', err);
        });
    })
    .then(function() {       // Query for 'limit' subsequent records
        dataQuery.attributes[schema.tid] = {'lt': reqTid};
        dataQuery.limit = limit; // typically something around 3, or unlimited

        // Traverse the bulk of the data, in timestamp descending order
        // (reverse chronological)
        var dataGetReq = req.extend({
            query: dataQuery,
            columnfamily: 'data',
        });
        var dataGetInfo = dbu.buildGetQuery(dataGetReq);

        return dbu.eachRow(
            self.client,
            dataGetInfo.cql,
            dataGetInfo.params,
            {retries: 3, fetchSize: 5, log: self.log},
            handler.handleRow.bind(handler)
        );
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
            // Use JSON.stringify to avoid object equality on functions
            if (currentSchemaInfo.hash === newSchemaInfo.hash) {
                if (!currentSchemaInfo._domainIndexDropped) {
                    // Hacky flag to avoid dropping index on each request.
                    // TODO: Properly keep track of internal schema versions!
                    currentSchemaInfo._domainIndexDropped= true;
                    // Asynchronously drop native secondary index on _domain column
                    self._dropDomainIndex(req);
                }

                // all good & nothing to do.
                return {
                    status: 201
                };
            } else {
                var migrator;
                try {
                    migrator = new SchemaMigrator(self, req, currentSchemaInfo, newSchemaInfo);
                }
                catch (error) {
                    throw new dbu.HTTPError({
                        status: 400,
                        body: {
                            type: 'bad_request',
                            title: 'The table already exists, and its schema cannot be upgraded to the requested schema ('+error+').',
                            keyspace: req.keyspace,
                            schema: newSchema
                        }
                    });
                }
                return migrator.migrate()
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
                    return self._put(putReq)
                    .then(function() {
                        // Force a cache update on subsequent requests
                        self._initSchemaCache();
                        return { status: 201 };
                    });
                })
                .catch(function(error) {
                    self.log('error/cassandra/table_update', error);
                    throw error;
                });
            }
        }

        // TODO:2014-11-09:gwicke use info from system.{peers,local} to
        // automatically set up DC replication
        //
        // Always use NetworkTopologyStrategy with default 'datacenter1' for easy
        // extension to cross-DC replication later.
        var localDc = self.conf.localDc;
        var replicationOptions = "{ 'class': 'NetworkTopologyStrategy', '" + localDc + "': 3 }";

        if (req.query.options) {
            if (req.query.options.durability === 'low') {
                replicationOptions = "{ 'class': 'NetworkTopologyStrategy', '" + localDc + "': 1 }";
            }
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
            return self._createKeyspace(req, replicationOptions)
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
                return self._createTable(req, indexSchema, 'idx_' + idx +"_ever");
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

    // Default to leveled compaction strategy
    cql += " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }";

    if (orderBits.length) {
        cql += ' and clustering order by ( ' + orderBits.join(',') + ' )';
    }

    if (schema.options && schema.options.compression) {
        // check if we support one of the desired ones
        cql += dbu.getTableCompressionCQL(schema.options.compression);
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

DB.prototype._createKeyspace = function (req, options) {
    var cql = 'create keyspace if not exists ' + cassID(req.keyspace)
        + ' WITH REPLICATION = ' + options;
    return this.client.execute_p(cql, [],
            {consistency: req.consistency || this.defaultConsistency});
};


DB.prototype.dropTable = function (domain, table) {
    var keyspace = this._keyspaceName(domain, table);
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [],
            {consistency: this.defaultConsistency});
};

DB.prototype.getTableSchema = function(domain, table) {
    var cacheKey = JSON.stringify([domain,table]);
    var req = new InternalRequest({
        domain: domain,
        table: table,
        keyspace: this.keyspaceNameCache[cacheKey] || this._keyspaceName(domain, table),
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
