"use strict";

var P = require('bluebird');
var cass = require('cassandra-driver');
var TimeUuid = cass.types.TimeUuid;
var extend = require('extend');
var dbu = require('./dbutils');
var cassID = dbu.cassID;
var secIndexes = require('./secondaryIndexes');


function DB (client, options) {
    this.conf = options.conf;
    this.log = options.log;

    this.defaultConsistency = cass.types.consistencies[this.conf.defaultConsistency]
        || cass.types.consistencies.one;

    // cassandra client
    this.client = client;


    // cache keyspace -> schema
    this.schemaCache = {};
    this.keyspaceSchemaCache = {};
    this.keyspaceNameCache = {};

    /* Process the array of storage groups declared in the config */
    this.storageGroups = this._buildStorageGroups(options.conf.storage_groups);
    /* The cache holding the already-resolved domain-to-group mappings */
    this.storageGroupsCache = {};
}


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
        return this._get(schemaReq)
        .then(function(res) {
            if (res.items.length) {
                // Need to parse the JSON manually here as we are using the
                // internal _get(), which doesn't apply transforms.
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
        return self._get(req)
        .then(function(res) {
            // Apply value conversions
            res.items = dbu.convertRows(res.items, req.schema);
            return res;
        });
    });
};

DB.prototype._get = function (req) {
    var self = this;

    if (!req.schema) {
        throw new Error("restbase-mod-table-cassandra: No schema for " + req.keyspace
                + ', table: ' + req.columnfamily);
    }

    if (!req.schema.iKeyMap) {
        self.log('error/cassandra/no_iKeyMap', req.schema);
    }
    var buildResult = dbu.buildGetQuery(req);

    return self.client.execute_p(buildResult.cql, buildResult.params,
            {consistency: req.consistency, prepare: true})
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
        return {
            items: rows
        };
    });

    // Index queries are currently handled in buildGetQuery. See
    // https://phabricator.wikimedia.org/T78722 for secondary index TODOs.
    //if (req.index) {
    //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    //}

    // Paging request: Currently disabled until this is made safe & sane.
    // See https://phabricator.wikimedia.org/T85640.
    //
    //var maxLimit = self.conf.maxLimit ? self.conf.maxLimit : 250;
    //if (req.pageSize || req.limit > maxLimit) {
    //    var rows = [];
    //    var options = {
    //        consistency: consistency,
    //        fetchSize: req.pageSize? req.pageSize : maxLimit,
    //        prepare: true
    //    };
    //    if (req.next) {
    //        var token = dbu.hashKey(this.conf.salt_key);
    //        token = req.next.substring(0,req.next.indexOf(token)).replace(/_/g,'/').replace(/-/g,'+');
    //        options.pageState = new Buffer(token, 'base64');
    //    }
    //    return new P(function(resolve, reject) {
    //        try {
    //            self.client.eachRow(buildResult.cql, buildResult.params, options,
    //                function(n, result){
    //                    dbu.convertRow(result, req.schema);
    //                    if (!result._del) {
    //                        rows.push(result);
    //                    }
    //                }, function(err, result){
    //                    if (err) {
    //                        reject(err);
    //                    } else {
    //                        var token = null;
    //                        if (result.meta.pageState) {
    //                            token = result.meta.pageState.toString('base64')
    //                                .replace(/\//g,'_').replace(/\+/g,'-')
    //                                // FIXME: use proper hashing - this is
    //                                // nonsense.
    //                                // See  https://phabricator.wikimedia.org/T85640
    //                                + dbu.hashKey(self.conf.salt_key
    //                                        && self.conf.salt_key.toString()
    //                                        || 'deadbeef');
    //                        }
    //                        resolve({
    //                            items: rows,
    //                            next: token
    //                        });
    //                   }
    //                }
    //            );
    //        } catch (e) {
    //            reject (e);
    //        }
    //    });
    //}

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
            self._rebuildIndexes(req, 3)
            .catch(function(err) {
                self.log('error/cassandra/rebuildIndexes', err);
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
 * Index update algorithm
 *
 * look at sibling revisions to update the index with values that no longer match
 *   - select sibling revisions
 *   - walk results in ascending order and diff each row vs. preceding row
 *      - if diff: for each index affected by that diff, update _deleted for old value
 *        using that revision's TIMESTAMP.
 * @param {object} InternalRequest; pass in an empty query to match / rebuild
 *        all entries
 * @param {number} limit [optional] The maximum number of entries to include in
 *      the index update.
 * @param {array} (optional) indexes, an array of index names to update;
 *      Default: all indexes in the schema
 */
DB.prototype._rebuildIndexes = function (req, limit, indexes) {
    var self = this;
    var schema = req.schema;
    var query = req.query;
    if (!indexes) {
        indexes = Object.keys(schema.secondaryIndexes);
    }
    if (indexes.length) {
        // Don't need more than consistency one for background index updates
        var consistency = cass.types.consistencies.one;
        var tidKey = schema.tid;

        // Build a new request for the main data table
        var dataQuery = {
            table: query.table,
            attributes: {},
            proj: []
        };

        // Narrow down the update to the original request's primary key. If
        // that's empty, the entire index (within the numerical limits) will be updated.
        schema.iKeys.forEach(function(att) {
            if (att !== tidKey) {
                dataQuery.attributes[att] = query.attributes[att];
                dataQuery.proj.push(att);
            }
        });

        // Select indexed attributes for all indexes to rebuild
        var secondaryKeySet = {};
        indexes.forEach(function(idx) {
            // console.log(idx, JSON.stringify(schema.secondaryIndexes));
            Object.keys(schema.attributeIndexes).forEach(function(att) {
                if (!schema.iKeyMap[att] && !secondaryKeySet[att]) {
                    dataQuery.proj.push(att);
                    secondaryKeySet[att] = true;
                }
            });
        });
        var secondaryKeys = Object.keys(secondaryKeySet);
        // Include the data table's _del column, so that we can deal with
        // deleted rows there
        dataQuery.proj.push('_del');
        if (!secondaryKeySet[tidKey]) {
            dataQuery.proj.push(tidKey);
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
        var newerRebuildRequest = req.extend({
            query: newerDataQuery
        });
        var newerRebuild = self._get(newerRebuildRequest)
        .then(function(res) {
            var newerRebuilder = new secIndexes.IndexRebuilder(self, req, secondaryKeys, reqTime);
            // XXX: handle the case where reqTid is not defined?
            for (var i = res.items.length - 1; i >= 0; i--) {
                // Process rows in reverse chronological order
                var row = res.items[i];
                newerRebuilder.handleRow(null, row);
            }
        });

        var mainRebuild = new P(function(resolve, reject) {
            try {
                dataQuery.attributes[schema.tid] = {'le': reqTid};
                dataQuery.limit = limit; // typically something around 3, or unlimited
                var reqOptions = {
                    prepare : true,
                    fetchSize : 1000,
                    autoPage: true
                };
                // Traverse the bulk of the data, in timestamp descending order
                // (reverse chronological)
                var dataGetReq = req.extend({
                    query: dataQuery,
                    columnfamily: 'data'
                });
                var dataGetInfo = dbu.buildGetQuery(dataGetReq);
                var mainRebuilder = new secIndexes.IndexRebuilder(self, req, secondaryKeys, reqTime);
                self.client.eachRow(dataGetInfo.cql, dataGetInfo.params, reqOptions,
                    // row callback
                    mainRebuilder.handleRow.bind(mainRebuilder),
                    // end callback
                    function (err, result) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    }
                );
            } catch (e) {
                reject (e);
            }
        });

        return P.all([newerRebuild, mainRebuild]);
    } else {
        return P.resolve();
    }
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
                // all good & nothing to do.
                return {
                    status: 201
                };
            } else {
                throw new dbu.HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        title: 'The table already exists, and its schema cannot be upgraded to the requested schema.',
                        keyspace: req.keyspace,
                        schema: newSchema
                    }
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

module.exports = DB;
