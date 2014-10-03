"use strict";

var crypto = require('crypto');
var cass = require('cassandra-driver');
var uuid = require('node-uuid');

var defaultConsistency = cass.types.consistencies.one;

function cassID (name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
}

function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
}

function buildCondition (pred) {
    var params = [];
    var conjunctions = [];
    for (var predKey in pred) {
        var cql = '';
        var predObj = pred[predKey];
        cql += cassID(predKey);
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey)
                    + ' is undefined');
        } else if (predObj.constructor !== Object) {
            // Default to equality
            cql += ' = ?';
            params.push(predObj);
        } else {
            var predKeys = Object.keys(predObj);
            if (predKeys.length === 1) {
                var predOp = predKeys[0];
                var predArg = predObj[predOp];
                switch (predOp.toLowerCase()) {
                case 'eq': cql += ' = ?'; params.push(predArg); break;
                case 'lt': cql += ' < ?'; params.push(predArg); break;
                case 'gt': cql += ' > ?'; params.push(predArg); break;
                case 'le': cql += ' <= ?'; params.push(predArg); break;
                case 'ge': cql += ' >= ?'; params.push(predArg); break;
                // Also support 'neq' for symmetry with 'eq' ?
                case 'ne': cql += ' != ?'; params.push(predArg); break;
                case 'between':
                        cql += ' >= ?' + ' AND '; params.push(predArg[0]);
                        cql += cassID(predKey) + ' <= ?'; params.push(predArg[1]);
                        break;
                default: throw new Error ('Operator ' + predOp + ' not supported!');
                }
            } else {
                throw new Error ('Invalid predicate ' + JSON.stringify(pred));
            }
        }
        conjunctions.push(cql);
    }
    return {
        query: conjunctions.join(' AND '),
        params: params
    };
}

// Hash a key into a valid Cassandra key name
function hashKey (key) {
    return crypto.Hash('sha1')
        .update(key)
        .digest()
        .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
        .replace(/[+\/]/g, '_')
        // Remove base64 padding, has no entropy
        .replace(/=+$/, '');
}

function getValidPrefix (key) {
    var prefixMatch = /^[a-zA-Z0-9_]+/.exec(key);
    if (prefixMatch) {
        return prefixMatch[0];
    } else {
        return '';
    }
}

function makeValidKey (key, length) {
    var origKey = key;
    key = key.replace(/_/g, '__')
                .replace(/\./g, '_');
    if (!/^[a-zA-Z0-9_]+$/.test(key)) {
        // Create a new 28 char prefix
        var validPrefix = getValidPrefix(key).substr(0, length * 2 / 3);
        return validPrefix + hashKey(origKey).substr(0, length - validPrefix.length);
    } else if (key.length > length) {
        return key.substr(0, length * 2 / 3) + hashKey(origKey).substr(0, length / 3);
    } else {
        return key;
    }
}


/**
 * Derive a valid keyspace name from a random bucket name. Try to use valid
 * chars from the requested name as far as possible, but fall back to a sha1
 * if not possible. Also respect Cassandra's limit of 48 or fewer alphanum
 * chars & first char being an alpha char.
 *
 * @param {string} reverseDomain, a domain in reverse dot notation
 * @param {string} key, the bucket name to derive the key of
 * @return {string} Valid Cassandra keyspace key
 */
function keyspaceName (reverseDomain, key) {
    var prefix = makeValidKey(reverseDomain, Math.max(26, 48 - key.length - 3));
    return prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
        + '_T_' + makeValidKey(key, 48 - prefix.length - 3);
}

function validateSchema(req) {

    if (!req.index || !req.index.hash) {
        //console.log(req);
        throw new Error("Missing index or hash key in table schema");
    }

    // Normalize the range index to an array
    var rangeIndex = req.index.range || [];
    if (!Array.isArray(rangeIndex)) {
        rangeIndex = [req.index.range];
    }

    return rangeIndex;
}

function generateIndexSchema (req, indexName, lastTid) {

    var index = req.secondaryIndexes[indexName],
    hasTid = false;

    if (!index.hash) {
        throw new Error ('Index not defined properly');
    }

    var rangeIndex = validateSchema(req);

    // Make sure we have an array for the range part of the index
    var range = [];
    if (index.range) {
        if (!Array.isArray(index.range)) {
            range = [index.range];
        } else {
            range = index.range;
        }
    }

    // Build up attributes
    var attributes = {
    };

    // copy over type info
    attributes[index.hash] = req.attributes[index.hash];

    // build index attributes for the schema
    var _indexAttributes = {};
    _indexAttributes[index.hash] = true;

    range.forEach(function(items){
        _indexAttributes[items] = true;
    });

    // TODO: Support indexes without a hash, by substituting an int column that defaults to 0 or the like.
    // This is useful for smallish indexes that need to be sorted / support range queries.

    // Make sure the main index keys are included in the new index
    // First, the hash key.
    if (!attributes[req.index.hash] && range.indexOf(req.index.hash) === -1) {
        // Add in the original hash key as an additional range key
        range.push(req.index.hash);
    }

    // Now the range key(s).
    rangeIndex.forEach(function(att) {
        if (!attributes[att] && range.indexOf(att) === -1) {
            // Add in the original hash key(s) as additional range
            // key(s)
            range.push(att);
            _indexAttributes[att] = true;
        }
    });

    // Now make sure that all range keys are also included in the
    // attributes.
    range.forEach(function(att) {
        if (!attributes[att]) {
            attributes[att] = req.attributes[att];
        }
    });

    // if the last range index on the primary table is not a timeuuid
    if (!lastTid || !_indexAttributes[lastTid]) {
        // if last range index is not yet part of the secondary index key.
        if (lastTid && !_indexAttributes[lastTid]) {
            attributes._tid = 'timeuuid';
            range.push('_tid');
            _indexAttributes._tid = true;
            if (!req.secondaryIndexes[indexName].range) {
                req.secondaryIndexes[indexName].range = ['_tid'];
            } else if (Array.isArray(req.secondaryIndexes[indexName].range)) {
                req.secondaryIndexes[indexName].range.push('_tid');
            } else {
                req.secondaryIndexes[indexName].range = [req.secondaryIndexes[indexName].range, 'tid'];
            }
        }

        attributes._deleted = 'timeuuid';
    }

    // Finally, deal with projections
    if (index.proj && Array.isArray(index.proj)) {
        index.proj.forEach(function(attr) {
            if (!attributes[attr]) {
                attributes[attr] = req.attributes[attr];
            }
        });
    }

    index.range = range;
    var indexSchema = {
        name: indexName,
        attributes: attributes,
        index: index,
        consistency: defaultConsistency,
        _indexAttributes: _indexAttributes
    };

    return indexSchema;
}

function DB (client) {
    // cassandra client
    this.client = client;

    // cache keyspace -> schema
    this.schemaCache = {};

    // Attribute <--> Index Map
    this.indexAttrMap = {};
}

// Info table schema
DB.prototype.infoSchema = {
    name: 'meta',
    attributes: {
        key: 'string',
        value: 'json'
    },
    index: {
        hash: 'key'
    },
    _restbase: { _indexAttributes: {'key': true} }
};


DB.prototype.buildPutQuery = function(req, keyspace, table, schema) {

    var keys = [];
    var params = [];
    var indexKVMap = {};
    var placeholders = [];
    var _indexAttributes;

    if (table === 'meta') {
        _indexAttributes = schema._restbase._indexAttributes;
    } else if ( table === "data" ) {
        _indexAttributes = schema._restbase._indexAttributes;
    } else {
        _indexAttributes = schema._indexAttributes;
        table = "idx_" + table + "_ever";
    }

    if (!schema) {
        throw new Error('Table not found!');
    }

    for (var key in _indexAttributes) {
        if (!req.attributes[key]) {
            throw new Error("Index attribute " + key + " missing");
        } else {
            indexKVMap[key] = req.attributes[key];
        }
    }
    for (key in req.attributes) {
        var val = req.attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (val && val.constructor === Object) {
                val = JSON.stringify(val);
            }
            if (!_indexAttributes[key]) {
                keys.push(key);
                params.push(val);
            }
            placeholders.push('?');
        }
    }

    // switch between insert & update / upsert
    // - insert for 'if not exists', or when no non-primary-key attributes are
    //   specified
    // - update when any non-primary key attributes are supplied
    //   - Need to verify that all primary key members are supplied as well,
    //     else error.

    var cql = '', condResult;

    if (req.if && req.if.constructor === String) {
        req.if = req.if.trim().split(/\s+/).join(' ').toLowerCase();
    }

    var condRes = buildCondition(indexKVMap);

    if (!keys.length || req.if === 'not exists') {
        var proj = Object.keys(_indexAttributes).concat(keys).map(cassID).join(',');
        cql = 'insert into ' + cassID(keyspace) + '.' + cassID(table)
                + ' (' + proj + ') values (';
        cql += placeholders.join(',') + ')';
        params = condRes.params.concat(params);
    } else if ( keys.length ) {
        var updateProj = keys.map(cassID).join(' = ?,') + ' = ? ';
        cql += 'update ' + cassID(keyspace) + '.' + cassID(table) +
               ' set ' + updateProj + ' where ';
        cql += condRes.query;
        params = params.concat(condRes.params);
    } else {
        throw new Error("Can't Update or Insert");
    }

    // Build up the condition
    if (req.if) {
        if (req.if === 'not exists') {
            cql += ' if not exists ';
        } else {
            cql += ' if ';
            condResult = buildCondition(req.if);
            cql += condResult.query;
            params = params.concat(condResult.params);
        }
    }

    return {query: cql, params: params};
};

DB.prototype.executeCql = function(batch, consistency) {
    var req;
    if (batch.length === 1) {
        req = this.client.execute_p(batch[0].query, batch[0].params, {consistency: consistency, prepared: true});
    } else {
        req = this.client.batch_p(batch, {consistency: consistency, prepared: true});
    }
    return req.catch(function(e) {
        //console.log(batch);
        e.stack += '\n' + JSON.stringify(batch);
        throw e;
    });
};

DB.prototype.getSchema = function (reverseDomain, tableName) {
    var keyspace = keyspaceName(reverseDomain, tableName);

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
    return this._get(keyspace, {}, consistency, 'meta')
    .then(function(res) {
        if (res.items.length) {
            var schema = res.items[0].value,
                _indexAttributes = {},
                rangeColumn;

            schema = JSON.parse(schema);
            schema._restbase = {};
            schema._restbase.indexSchema = {};

            if (schema.secondaryIndexes) {
                for (var indexName in schema.secondaryIndexes) {
                    var indexSchema = generateIndexSchema(schema, indexName);
                    schema._restbase.indexSchema[indexName] = indexSchema;
                }
            }

            _indexAttributes[schema.index.hash] = true;
            rangeColumn = schema.index.range;
            if (Array.isArray(rangeColumn)) {
                rangeColumn.forEach(function(items){
                    _indexAttributes[items] = true;
                });
            } else if (rangeColumn) {
                _indexAttributes[rangeColumn] = true;
            }
            schema._restbase._indexAttributes = _indexAttributes;
            return schema;
        } else {
            return null;
        }
    });
};

DB.prototype.buildGetQuery = function(keyspace, req, consistency, table) {

    var proj = '*';
    var cachedSchema = this.schemaCache[keyspace];

    if (req.proj) {
        if (Array.isArray(req.proj)) {
            proj = req.proj.map(cassID).join(',');
        } else if (req.proj.constructor === String) {
            proj = cassID(req.proj);
        }
    } else if (req.order) {
        // Work around 'order by' bug in cassandra when using *
        // Trying to change the natural sort order only works with a
        // projection in 2.0.9
        if (cachedSchema) {
            proj = Object.keys(cachedSchema.attributes).map(cassID).join(',');
        }
    }

    if (req.limit && req.limit.constructor !== Number) {
        req.limit = 2;
    }

    var item, newlimit;
    if (!req.index) {
        // req should not have non primary key attr
        for ( item in req.attributes ) {
            if (!cachedSchema._restbase._indexAttributes[item]) {
                throw new Error("Request attributes should contain only primary key attributes");
            }
        }
    } else {
        if (!cachedSchema.secondaryIndexes[req.index]) {
            throw new Error("Index attributes is invalid");
        }

        newlimit = req.limit + Math.ceil(req.limit/4);
        table = 'idx_' + req.index + "_ever";
    }

    if (req.distinct) {
        proj = 'distinct ' + proj;
    }

    var cql = 'select ' + proj + ' from '
        + cassID(keyspace) + '.' + cassID(table);

    var params = [];
    // Build up the condition
    if (req.attributes) {
        cql += ' where ';
        var condResult = buildCondition(req.attributes);
        cql += condResult.query;
        params = condResult.params;
    }

    if (req.order) {
        // need to know range column
        var schema = this.schemaCache[keyspace];
        var rangeColumn;
        if (schema) {
            rangeColumn = schema.index.range;
            if (Array.isArray(rangeColumn)) {
                rangeColumn = rangeColumn[0];
            }
        } else {
            // fake it for now
            rangeColumn = '_tid';
        }
        var dir = req.order.toLowerCase();
        if (rangeColumn && dir in {'asc':1, 'desc':1}) {
            cql += ' order by ' + cassID(rangeColumn) + ' ' + dir;
        }
    }

    if (req.limit) {
        cql += ' limit ' + req.limit||newlimit;
    }

    return {query: cql, params: params};
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


    var cachedSchema = this.schemaCache[keyspace]._restbase.indexSchema[req.index];
    for(var item in startKey) {
        if ( !internalColumns[item] && cachedSchema._indexAttributes[item]) {
            if (cachedSchema.attributes[item] === 'timeuuid' ) {
                // TODO : change 'le' to requested range conditions
                newIndexReq.attributes[item] = {'le': startKey[item]};
            } else {
                newIndexReq.attributes[item] = startKey[item];
            }
        }
    }

    var buildResult = this.buildGetQuery(keyspace, newIndexReq, consistency, table);
    
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
                if (!row._deleted || row._deleted > req._tid
                    || row._tid > req._tid) {
                    var attributes = {};
                    var proj = {};

                    for (var attr in self.schemaCache[keyspace]._restbase._indexAttributes) {
                        attributes[attr] = row[attr];
                    }
                    queries.push(self.buildGetQuery(keyspace, {
                                                    table: table,
                                                    attributes: attributes,
                                                    proj: proj,
                                                    limit: req.limit + Math.ceil(req.limit/4)},
                                                    consistency, table));
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
            }
        });
    });
};

/*
    Handler for request GET requests on secondary indexes.
*/
DB.prototype._getSecondaryIndex = function(keyspace, req, consistency, table, buildResult){

    var self = this;
    return self.client.execute_p(buildResult.query, buildResult.params, {consistency: consistency, prepared: true})
    .then(function(results) {
        var queries = [], needCheck=true;
        var cachedSchema = self.schemaCache[keyspace];
        
        var newReq = {
            table: table,
            attributes: {},
            limit: req.limit + Math.ceil(req.limit/4)
        };

        //do a query against the main data table if some of the requested attributes 
        //are not directly found in the index table. 
        Object.keys(req.attributes).forEach(function(item){
            if (!cachedSchema._restbase.indexSchema[req.index][item]) {
                needCheck = false;
            }
        });

        // build main data queries
        if (needCheck) {
            for ( var rowno in results.rows ) {
                if (!results.rows[rowno]._deleted || results.rows[rowno]._deleted > req._tid
                    || results.rows[rowno]._tid > req._tid) {
                    for ( var attr in cachedSchema._restbase._indexAttributes ) {
                        newReq.attributes[attr] = results.rows[rowno][attr];
                    }
                    queries.push(self.buildGetQuery(keyspace, newReq, consistency, table));
                    newReq.attributes = {};
                }
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
                return [needCheck, finalRows, results.rows[rowno]];
            });
        } else {
            return Promise.resolve([needCheck, results.rows]);
        }
    })
    .then(function(rows){
        if (rows[0] && rows[1].length<req.limit) {
            return self.indexReads(keyspace, req, consistency, table, rows[2], rows[1]);
        }
        return rows[1];
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

DB.prototype.get = function (reverseDomain, req) {
    var self = this;
    var keyspace = keyspaceName(reverseDomain, req.table);

    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }

    if (!this.schemaCache[keyspace]) {
        return this._getSchema(keyspace, defaultConsistency)
        .then(function(schema) {
            //console.log('schema', schema);
            self.schemaCache[keyspace] = schema;
            return self._get(keyspace, req, consistency);
        });
    } else {
        return this._get(keyspace, req, consistency);
    }
};

DB.prototype._get = function (keyspace, req, consistency, table) {

    if (!table) {
        table = 'data';
    }

    var buildResult = this.buildGetQuery(keyspace, req, consistency, table);

    if (req.index) {
        return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    }

    var self = this;
    return self.client.execute_p(buildResult.query, buildResult.params, {consistency: consistency, prepared: true})
    .then(function(result){
        var rows = result.rows;
        // hide the columns property added by node-cassandra-cql
        // XXX: submit a patch to avoid adding it in the first place
        rows.forEach(function(row) {
            row.__columns = undefined;
        });
        return {
            count: rows.length,
            items: rows
        };
    });
};

DB.prototype.put = function (reverseDomain, req) {
    var keyspace = keyspaceName(reverseDomain, req.table);


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
    if (schema.attributes._tid) {
        req.attributes._tid = tidFromDate(new Date());
    }

    // insert into secondary Indexes first
    var batch = [];
    var queryResult;
    if (schema.secondaryIndexes) {
        for ( var item in schema.secondaryIndexes) {
            schema = this.schemaCache[keyspace]._restbase.indexSchema[item];
            if (!schema) {
                throw new Error('Table not found!');
            }
            queryResult = this.buildPutQuery(req, keyspace, item, schema);
            batch.push(queryResult);
        }
    }

    // insert into meta/data table
    if (table === 'meta') {
        schema = this.infoSchema;
    } else if ( table === "data" ) {
        schema = this.schemaCache[keyspace];
    }

    queryResult = this.buildPutQuery(req, keyspace, table, schema);
    batch.push(queryResult);

    //console.log(batch, schema);
    var self = this;
    return this.client.batch_p(batch, {consistency: consistency, prepared: true})
    .then(function(result) {
        /* look at sibling revisions to update the index with values that no longer match
        *   - select sibling revisions
        *   - walk results in ascending order and diff each row vs. preceding row
        *      - if diff: for each index affected by that diff, update _deleted for old value 
        *        using that revision's TIMESTAMP.
        */
        if (schema.secondaryIndexes && schema.attributes._tid) {
            // build new requests
            var rows = result.rows;

            var newReq1 = {
                table: req.table,
                attributes: {},
                proj: {}
            };
            var newReq2 = {
                table: req.table,
                attributes: {},
                proj: {}
            };

            // Data table _indexAttributes <--intersection--> secondary Indexes indexAttributes
            for(var item in schema._restbase._indexAttributes) {
                newReq1.attributes[item] = req.attributes[item];
                newReq2.attributes[item] = req.attributes[item];
            }

            for (var secIndex in schema.secondaryIndexes) {
                for (item in schema._restbase.indexSchema[secIndex]._indexAttributes) {
                    if (!schema._restbase._indexAttributes[item]) {
                        newReq1.proj[item] = req.attributes[item];
                        newReq2.proj[item] = req.attributes[item];
                    }
                }
            }

            // select sibling revisions 
            batch = [];
            newReq1.attributes._tid = {'le': req.attributes._tid};
            newReq1.limit = 3;
            batch.push(self.buildGetQuery(keyspace, newReq1, consistency, table));
            newReq2.attributes._tid = {'gt': req.attributes._tid};
            newReq2.limit = 1;
            batch.push(self.buildGetQuery(keyspace, newReq2, consistency, table));

            var batchPromises = [];
            batch.forEach(function(item) {
                batchPromises.push(self.client.execute_p(item.query, item.params, item.options || {consistency: consistency, prepared: true}));
            });
            return Promise.all(batchPromises)
            .then(function(results) {
                // sort rows in ascending order
                results[0].rows.sort(function(a, b){
                    if(a._tid > b._tid) {
                        return -1;
                    }
                });
                var rows = results[0].rows.concat(results[1].rows);

                var queue, hasDiff=false;
                batch = [];
                batchPromises = [];
                // compare one row with another
                for (var rowNo=0; rowNo<Object.keys(rows).length; rowNo++) {
                    var row1 = rows[rowNo];
                    var row2 = rows[rowNo + 1];
                    queue = Object.keys(schema.secondaryIndexes);
                    if (row1 && row2) {
                        // diff both rows
                        for (var item in schema.attributes) {
                            if (row1[item] !== row2[item]) {
                                // if diff: build a put query for requiered secondary indexes and push it to a batch
                                if (self.indexAttrMap[keyspace][item]) {
                                    self.indexAttrMap[keyspace][item].forEach(function(secIndex){
                                        if (queue.indexOf(secIndex) !== -1) {
                                            newReq1 = {
                                                table: req.table,
                                                attributes: {},
                                            };
                                            for(var attr in schema._restbase.indexSchema[secIndex].attributes) {
                                                newReq1.attributes[attr] = row1[attr];
                                            }
                                            newReq1.attributes._deleted = req.attributes._tid;
                                            newReq1.index = "idx_" + secIndex + "_ever";
                                            batch.push(
                                                // generate put query with _deleted = tuuid
                                                self.buildPutQuery(newReq1, keyspace, secIndex, schema._restbase.indexSchema[secIndex])
                                            );
                                            queue.pop(secIndex);
                                        }
                                    });
                                }
                                hasDiff = true;
                            }
                        }
                        if (hasDiff) {
                            newReq1 = {
                                table: req.table,
                                attributes: { _deleted: req.attributes._tid }
                            };
                            for(item in req.attributes) {
                                newReq1.attributes[item] = row1[item];
                            }
                            batch.push(
                                // generate put query with _deleted = tuuid
                                self.buildPutQuery(newReq1, keyspace, table, schema)
                            );
                        }
                    }
                }
                // execute the batch
                batch.forEach(function(item) {
                    batchPromises.push(self.client.execute_p(item.query, item.params, item.options || {consistency: consistency, prepared: true}));
                });
                return Promise.all(batchPromises)
                        .then(function(){
                            return {
                                // XXX: check if condition failed!
                                status: 201
                            };
                        });
            });
        }
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
};


DB.prototype.delete = function (reverseDomain, req) {
    var keyspace = keyspaceName(reverseDomain, req.table);

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
        var condResult = buildCondition(req.attributes);
        cql += condResult.query;
        params = condResult.params;
    }

    // TODO: delete from indexes too!
    //console.log(cql, params);
    return this.client.execute_p(cql, params, {consistency: consistency});
};

DB.prototype._createKeyspace = function (keyspace, consistency, options) {
    var cql = 'create keyspace ' + cassID(keyspace)
        + ' WITH REPLICATION = ' + options;
    return this.client.execute_p(cql, [],  {consistency: consistency || defaultConsistency});
};

DB.prototype.createTable = function (reverseDomain, req) {
    var self = this;
    if (!req.table) {
        throw new Error('Table name required.');
    }
    var keyspace = keyspaceName(reverseDomain, req.table);

    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }

    var infoSchema = this.infoSchema;

    if (!req.options) {
        req.options = "{ 'class': 'SimpleStrategy', 'replication_factor': 3 }";
    } else {
        req.options = "{ 'class': '"+ req.options.storageClass + "', 'replication_factor': " + req.options.durabilityLevel + "}";
    }

    return this._createKeyspace(keyspace, consistency, req.options)
    .then(function() {
        return Promise.all([
            self._createTable(keyspace, req, 'data', consistency),
            self._createTable(keyspace, infoSchema, 'meta', consistency)
        ]);
    })
    .then(function() {
        return self._put(keyspace, {
            attributes: {
                key: 'schema',
                value: JSON.stringify(req)
            }
        }, consistency, 'meta');
    });
};

DB.prototype._createTable = function (keyspace, req, tableName, consistency) {
    var self = this;

    if (!req.attributes) {
        throw new Error('No AttributeDefinitions for table!');
    }

    // Figure out which columns are supposed to be static
    var statics = {};
    var tasks = [], hasTid=false;
    if (req.index && req.index.static) {
        var s = req.index.static;
        if (Array.isArray(s)) {
            s.forEach(function(k) {
                statics[k] = true;
            });
        } else {
            statics[s] = true;
        }
    }

    // Validate and Normalize the range index to an array
    var rangeIndex = validateSchema(req);

    // XXX: Handle secondary indexes
    if (req.secondaryIndexes) {

        this.indexAttrMap[keyspace] = {};
        for ( var item in req.attributes) {
            this.indexAttrMap[keyspace][item] = [];
        }

        // check if last key of index.range is a tid
        if (Array.isArray(req.range) && req.attributes[req.index.range[req.index.range.length-1]]=== "timeuuid") {
            hasTid = req.index.range[req.index.range.length-1];
        } else if (req.attributes[req.index.range] === "timeuuid") {
            hasTid = req.index.range;
        }

        if (!hasTid) {
            req.attributes._tid = 'timeuuid';
            req.attributes._deleted = 'timeuuid';
            req.index.range = rangeIndex;
            req.index.range.push("_tid");
        }

        for (var indexName in req.secondaryIndexes) {
            var indexSchema = generateIndexSchema(req, indexName, hasTid);
            for (item in indexSchema.attributes) {
                if (this.indexAttrMap[keyspace][item]) {
                    this.indexAttrMap[keyspace][item].push(indexName);
                }
            }
            tasks.push(this._createTable(keyspace, indexSchema, 'idx_' + indexName +"_ever"));
        }
    }

    var cql = 'create table '
        + cassID(keyspace) + '.' + cassID(tableName) + ' (';
    for (var attr in req.attributes) {
        var type = req.attributes[attr];
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

    cql += 'primary key (';
    var indexBits = [cassID(req.index.hash)];
    rangeIndex.forEach(function(att) {
        indexBits.push(cassID(att));
    });

    cql += indexBits.join(',') + '))';

    // Default to leveled compaction strategy
    cql += " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }";

    if (req.index.order && rangeIndex.length) {
        var orders = req.index.order;
        if (!Array.isArray(orders)) {
            orders = [orders];
        }
        var orderBits = [];
        for (var i = 0; i < rangeIndex.length; i++) {
            var attName = rangeIndex[i];
            var dir = orders[i];
            if (dir) {
                if (dir.constructor !== String
                        || ! {'asc':1, 'desc':1}[dir.toLowerCase()])
                {
                    throw new Error('Invalid order direction in schema:\n' + req);
                } else {
                    orderBits.push(cassID(attName) + ' ' + dir.toLowerCase());
                }
            }
        }
        if (orderBits) {
            cql += ' and clustering order by ( ' + orderBits.join(',') + ' )';
        }
    }

    //console.log(cql);
    tasks.push(this.client.execute_p(cql, [], {consistency: consistency}));
    return Promise.all(tasks);
};

DB.prototype.dropTable = function (reverseDomain, table) {
    var keyspace = keyspaceName(reverseDomain, table);
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [], {consistency: defaultConsistency});
};


module.exports = DB;
