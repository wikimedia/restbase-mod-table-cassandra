"use strict";

var crypto = require('crypto');
var cass = require('cassandra-driver');
var uuid = require('node-uuid');
var extend = require('extend');
var dbu = require('./dbutils');
var cassID = dbu.cassID;
var secIndexes = require('./secondaryIndexes');

// XXX: Use config only
var defaultConsistency = cass.types.consistencies.one;

function DB (client) {
    // cassandra client
    this.client = client;

    // cache keyspace -> schema
    this.schemaCache = {};
}

// Info table schema
DB.prototype.infoSchema = {
    table: 'meta',
    attributes: {
        key: 'string',
        value: 'json'
    },
    index: [
        { attribute: 'key', type: 'hash' }
    ],
    iKeys: ['key'],
    iKeyMap: {
        key: { attribute: 'key', type: 'hash' }
    },
    attributeIndexes: {}
};


DB.prototype.getSchema = function (reverseDomain, tableName) {
    var keyspace = dbu.keyspaceName(reverseDomain, tableName);

    // consistency
    var consistency = defaultConsistency;
    var query = {
        attributes: {
            key: 'schema'
        }
    };
    return this._getSchema(keyspace, consistency);
};

DB.prototype._getSchema = function (keyspace, consistency) {
    var query = {
        attributes: {
            key: 'schema'
        }
    };
    return this._get(keyspace, {}, consistency, 'meta', this.infoSchema)
    .then(function(res) {
        if (res.items.length) {
            var schema = JSON.parse(res.items[0].value);
            return dbu.makeSchemaInfo(schema);
        } else {
            return null;
        }
    });
};

DB.prototype.get = function (reverseDomain, req) {
    var self = this;
    var keyspace = dbu.keyspaceName(reverseDomain, req.table);

    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }

    var schema = this.schemaCache[keyspace];
    if (!schema) {
        return this._getSchema(keyspace, defaultConsistency)
        .then(function(schema) {
            //console.log('schema', schema);
            self.schemaCache[keyspace] = schema;
            return self._get(keyspace, req, consistency, schema);
        });
    } else {
        return this._get(keyspace, req, consistency, 'data', schema);
    }
};

DB.prototype._get = function (keyspace, req, consistency, table, schema) {

    if (!table) {
        table = 'data';
    }

    if (!schema) {
        throw new Error("restbase-cassandra: No schema for " + keyspace
                + ', table: ' + table);
    }

    if (!schema.iKeyMap) {
        console.error('No iKeyMap!', JSON.stringify(schema, null, 2));
    }
    var buildResult = dbu.buildGetQuery(keyspace, req, consistency, table, schema);

    //if (req.index) {
    //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    //}

    var self = this;
    return self.client.execute_p(buildResult.query, buildResult.params, {consistency: consistency, prepared: true})
    .then(function(result){
        // hide the columns property added by node-cassandra-cql
        // XXX: submit a patch to avoid adding it in the first place
        var rows = [];
        result.rows.forEach(function(row) {
            // Filter rows that don't match any more
            // XXX: Refine this for queries in the past:
            // - compare to query time for index entries
            // - compare to tid for main data table entries, or use tids there
            //   as well
            if (!row._del) {
                if (row.__columns) {
                    delete row.__columns;
                }
                rows.push(row);
            }
        });

        return {
            count: rows.length,
            items: rows
        };
    });
};

/*
    Handler for request GET requests on secondary indexes.
*/
DB.prototype._getSecondaryIndex = function(keyspace, req, consistency, table, buildResult){

    // TODO: handle '_tid' cases
    var self = this;
    return self.client.execute_p(buildResult.query, buildResult.params, {consistency: consistency, prepared: true})
    .then(function(results) {
        var queries = [];
        var cachedSchema = self.schemaCache[keyspace];

        var newReq = {
            table: table,
            attributes: {},
            limit: req.limit + Math.ceil(req.limit/4)
        };

        // build main data queries
        for ( var rowno in results.rows ) {
            for ( var attr in cachedSchema.iKeyMap ) {
                newReq.attributes[attr] = results.rows[rowno][attr];
            }
            queries.push(dbu.buildGetQuery(keyspace, newReq, consistency, table, cachedSchema));
            newReq.attributes = {};
        }

        // prepare promises for batch execution
        var batchPromises = [];
        queries.forEach(function(item) {
            batchPromises.push(self.client.execute_p(item.query, item.params, item.options || {consistency: consistency, prepared: true}));
        });

        // execute batch and check if limit is fulfilled
        return Promise.all(batchPromises).then(function(batchResults){
            var finalRows = [];
            batchResults.forEach(function(item){
                if (finalRows.length < req.limit) {
                    finalRows.push(item.rows[0]);
                }
            });
            return [finalRows, results.rows[rowno]];
        });
    })
    .then(function(rows){
        //TODO: handle case when limit > no of entries in table
        if (rows[0].length<req.limit) {
            return self.indexReads(keyspace, req, consistency, table, rows[1], rows[0]);
        }
        return rows[0];
    }).then(function(rows){
        // hide the columns property added by node-cassandra-cql
        // XXX: submit a patch to avoid adding it in the first place
        for (var row in rows) {
            row.columns = undefined;
        }
        return {
            count: rows.length,
            items: rows
        };
    });
};

/*
    Fetch index entries and compare them against data row for false positives
    - if limit is fullfilled return
    - else fetch more entries and compare again
*/
DB.prototype.indexReads = function(keyspace, req, consistency, table, startKey, finalRows) {

    // create new index query
    var newIndexReq = {
        table: table,
        index: req.index,
        attributes: {},
        limit: req.limit + Math.ceil(req.limit/4)
    };

    var internalColumns = {
        _deleted: true,
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
    return new Promise(function(resolve, reject){
        // stream  the main data table
        var stream = self.client.stream(buildResult.query, buildResult.params, {autoPage: true,
                                                fetchSize: req.limit + Math.ceil(req.limit/4),
                                                prepare: 1,
                                                consistency: consistency})
        .on('readable', function(){
            var row = this.read();
            for (row; row !== null; row=this.read()) {
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
                    return self.client.execute_p(item.query, item.params, item.options || {consistency: consistency, prepared: true})
                    .then(function(results){
                        if (finalRows.length < req.limit) {
                            finalRows.push(results.rows[0]);
                        }
                    });
                }
            }
        });
    });
};

DB.prototype.put = function (reverseDomain, req) {
    var keyspace = dbu.keyspaceName(reverseDomain, req.table);


    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }

    // Get the type info for the table & verify types & ops per index
    var self = this;
    if (!this.schemaCache[keyspace]) {
        return this._getSchema(keyspace, defaultConsistency)
        .then(function(schema) {
            self.schemaCache[keyspace] = schema;
            return self._put(keyspace, req, consistency);
        });
    } else {
        return this._put(keyspace, req, consistency);
    }
};


DB.prototype._put = function(keyspace, req, consistency, table ) {

    if (!table) {
        table = 'data';
    }

    var schema;
    if (table === 'meta') {
        schema = this.infoSchema;
    } else if ( table === "data" ) {
        schema = this.schemaCache[keyspace];
    }

    if (!schema) {
        throw new Error('Table not found!');
    }

    if (!req.attributes[schema.tid]) {
        req.attributes[schema.tid] = uuid.v1();
    }

    req.timestamp = uuid.v1time(req.attributes[schema.tid]);

    // insert into secondary Indexes first
    var batch = [];
    if (schema.secondaryIndexes) {
        for ( var idx in schema.secondaryIndexes) {
            var secondarySchema = schema.secondaryIndexes[idx];
            if (!secondarySchema) {
                throw new Error('Table not found!');
            }
            //if (req.attributes.uri) { console.log(req.attributes.uri, req.timestamp); }
            batch.push(dbu.buildPutQuery(req, keyspace, dbu.idxTable(idx), secondarySchema));
        }
    }

    batch.push(dbu.buildPutQuery(req, keyspace, table, schema));

    //console.log(batch, schema);
    var self = this;
    return this.client.batch_p(batch, {consistency: consistency, prepared: true})
    .then(function(result) {
        // Kick off asynchronous local index rebuild
        if (schema.secondaryIndexes) {
            self._rebuildIndexes(keyspace, req, schema, 3);
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
 * @param {string} keyspace
 * @param {object} req, the original update request; pass in empty attributes
 *        to match / rebuild all entries
 * @param {object} schema, the table schema
 * @param {array} (optional) indexes, an array of index names to update;
 *        default: all indexes in the schema
 */
DB.prototype._rebuildIndexes = function (keyspace, req, schema, limit, indexes) {
    var self = this;
    if (!indexes) {
        indexes = Object.keys(schema.secondaryIndexes);
    }
    if (indexes.length) {
        // Don't need more than consistency one for background index updates
        var consistency = cass.types.consistencies.one;
        var tidKey = schema.tid;

        // Build a new request for the main data table
        var dataReq = {
            table: req.table,
            attributes: {},
            proj: []
        };

        // Narrow down the update to the original request's primary key. If
        // that's empty, the entire index (within the numerical limits) will be updated.
        schema.iKeys.forEach(function(att) {
            if (att !== tidKey) {
                dataReq.attributes[att] = req.attributes[att];
                dataReq.proj.push(att);
            }
        });

        // Select indexed attributes for all indexes to rebuild
        var secondaryKeySet = {};
        indexes.forEach(function(idx) {
            // console.log(idx, JSON.stringify(schema.secondaryIndexes));
            Object.keys(schema.attributeIndexes).forEach(function(att) {
                if (!schema.iKeyMap[att] && !secondaryKeySet[att]) {
                    dataReq.proj.push(att);
                    secondaryKeySet[att] = true;
                }
            });
        });
        var secondaryKeys = Object.keys(secondaryKeySet);
        // Include the data table's _del column, so that we can deal with
        // deleted rows there
        dataReq.proj.push('_del');
        if (!secondaryKeySet[tidKey]) {
            dataReq.proj.push(tidKey);
        }

        // XXX: handle the case where reqTid is not defined!
        var reqTid = req.attributes[schema.tid];
        var reqTime = uuid.v1time(reqTid);

        // Clone the query, and create le & gt variants
        var newerDataReq = extend(true, {}, dataReq);
        // 1) select one newer index entry
        newerDataReq.attributes[schema.tid] = { 'ge': reqTid };
        newerDataReq.order = {};
        newerDataReq.order[schema.tid] = 'asc'; // select sibling entries
        newerDataReq.limit = 2; // data entry + newer entry
        var newerRebuild = self._get(keyspace, newerDataReq, defaultConsistency, 'data', schema)
        .then(function(res) {
            var newerRebuilder = new secIndexes.IndexRebuilder(self, keyspace,
                    schema, secondaryKeys, reqTime);
            // XXX: handle the case where reqTid is not defined?
            for (var i = res.items.length - 1; i >= 0; i--) {
                // Process rows in reverse chronological order
                var row = res.items[i];
                newerRebuilder.handleRow(null, row);
            }
        });

        var mainRebuild = new Promise(function(resolve, reject) {
            try {
                dataReq.attributes[schema.tid] = {'le': reqTid};
                dataReq.limit = limit; // typically something around 3, or unlimited
                var reqOptions = {
                    prepare : 1,
                    fetchSize : 1000,
                    autoPage: true
                };
                // Traverse the bulk of the data, in timestamp descending order
                // (reverse chronological)
                var dataQuery = dbu.buildGetQuery(keyspace, dataReq, consistency, 'data', schema);
                var mainRebuilder = new secIndexes.IndexRebuilder(self, keyspace,
                        schema, secondaryKeys, reqTime);
                self.client.eachRow(dataQuery.query, dataQuery.params, reqOptions,
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

        return Promise.all([newerRebuild, mainRebuild])
        .then(function() { return; });
    } else {
        return Promise.resolve();
    }
};


DB.prototype.delete = function (reverseDomain, req) {
    var keyspace = dbu.keyspaceName(reverseDomain, req.table);

    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }
    return this._delete(keyspace, req, consistency);
};

DB.prototype._delete = function (keyspace, req, consistency, table) {
    if (!table) {
        table = 'data';
    }
    var cql = 'delete from '
        + cassID(keyspace) + '.' + cassID(table);

    var params = [];
    // Build up the condition
    if (req.attributes) {
        cql += ' where ';
        var condResult = dbu.buildCondition(req.attributes);
        cql += condResult.query;
        params = condResult.params;
    }

    // TODO: delete from indexes too!
    //console.log(cql, params);
    return this.client.execute_p(cql, params, {consistency: consistency});
};

DB.prototype.createTable = function (reverseDomain, req) {
    var self = this;
    if (!req.table) {
        throw new Error('Table name required.');
    }
    var keyspace = dbu.keyspaceName(reverseDomain, req.table);

    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }

    var infoSchema = this.infoSchema;

    // Validate and normalize the schema
    var schema = dbu.validateAndNormalizeSchema(req);

    var internalSchema = dbu.makeSchemaInfo(schema);

    // console.log(JSON.stringify(internalSchema, null, 2));

    if (!req.options) {
        req.options = "{ 'class': 'SimpleStrategy', 'replication_factor': 3 }";
    } else {
        req.options = "{ 'class': '"+ req.options.storageClass + "', 'replication_factor': " + req.options.durabilityLevel + "}";
    }

    return this._createKeyspace(keyspace, consistency, req.options)
    .then(function() {
        return Promise.all([
            self._createTable(keyspace, internalSchema, 'data', consistency),
            self._createTable(keyspace, infoSchema, 'meta', consistency)
        ]);
    })
    .then(function() {
        self.schemaCache[keyspace] = internalSchema;
        return self._put(keyspace, {
            attributes: {
                key: 'schema',
                value: JSON.stringify(schema)
            }
        }, consistency, 'meta');
    });
};

DB.prototype._createTable = function (keyspace, schema, tableName, consistency) {
    var self = this;

    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + tableName);
    }

    var tasks = [];
    if (schema.secondaryIndexes) {
        // Create secondary indexes
        for (var idx in schema.secondaryIndexes) {
            var indexSchema = schema.secondaryIndexes[idx];
            tasks.push(this._createTable(keyspace, indexSchema, 'idx_' + idx +"_ever"));
        }
    }

    var statics = {};
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        }
    });

    // Finally, create the main data table
    var cql = 'create table '
        + cassID(keyspace) + '.' + cassID(tableName) + ' (';
    for (var attr in schema.attributes) {
        var type = schema.attributes[attr];
        cql += cassID(attr) + ' ';
        switch (type) {
        case 'blob': cql += 'blob'; break;
        case 'set<blob>': cql += 'set<blob>'; break;
        case 'decimal': cql += 'decimal'; break;
        case 'set<decimal>': cql += 'set<decimal>'; break;
        case 'double': cql += 'double'; break;
        case 'set<double>': cql += 'set<double>'; break;
        case 'boolean': cql += 'boolean'; break;
        case 'set<boolean>': cql += 'set<boolean>'; break;
        case 'int': cql += 'varint'; break;
        case 'set<int>': cql += 'set<varint>'; break;
        case 'varint': cql += 'varint'; break;
        case 'set<varint>': cql += 'set<varint>'; break;
        case 'string': cql += 'text'; break;
        case 'set<string>': cql += 'set<text>'; break;
        case 'timeuuid': cql += 'timeuuid'; break;
        case 'set<timeuuid>': cql += 'set<timeuuid>'; break;
        case 'uuid': cql += 'uuid'; break;
        case 'set<uuid>': cql += 'set<uuid>'; break;
        case 'timestamp': cql += 'timestamp'; break;
        case 'set<timestamp>': cql += 'set<timestamp>'; break;
        case 'json': cql += 'text'; break;
        case 'set<json>': cql += 'set<text>'; break;
        default: throw new Error('Invalid type ' + type
                     + ' for attribute ' + attr);
        }
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

    // console.log(cql);

    // Execute the table creation query
    tasks.push(this.client.execute_p(cql, [], {consistency: consistency}));
    return Promise.all(tasks);
};

DB.prototype._createKeyspace = function (keyspace, consistency, options) {
    var cql = 'create keyspace ' + cassID(keyspace)
        + ' WITH REPLICATION = ' + options;
    return this.client.execute_p(cql, [],  {consistency: consistency || defaultConsistency});
};


DB.prototype.dropTable = function (reverseDomain, table) {
    var keyspace = dbu.keyspaceName(reverseDomain, table);
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [], {consistency: defaultConsistency});
};


module.exports = DB;
