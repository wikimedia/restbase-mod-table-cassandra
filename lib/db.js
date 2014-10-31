"use strict";

var crypto = require('crypto');
var cass = require('cassandra-driver');
var uuid = require('node-uuid');
var extend = require('extend');

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

function validateIndexSchema(schema, index) {

    if (!Array.isArray(index) || !index.length) {
        //console.log(req);
        throw new Error("Invalid index " + JSON.stringify(index));
    }

    var haveHash = false;

    index.forEach(function(elem) {
        if (!schema.attributes[elem.attribute]) {
            throw new Error('Index element ' + JSON.stringify(elem)
                    + ' is not in attributes!');
        }

        switch (elem.type) {
        case 'hash':
            haveHash = true;
            break;
        case 'range':
            if (elem.order !== 'asc' && elem.order !== 'desc') {
                // Default to ascending sorting.
                //
                // Normally you should specify the sorting explicitly. In
                // particular, you probably always want to use descending
                // order for time series data (timeuuid) where access to the
                // most recent data is most common.
                elem.order = 'desc';
            }
            break;
        case 'static':
        case 'proj':
            break;
        default:
            throw new Error('Invalid index element encountered! ' + JSON.stringify(elem));
        }
    });

    if (!haveHash) {
        throw new Error("Indexes without hash are not yet supported!");
    }

    return index;
}

function validateAndNormalizeSchema(schema) {
    if (!schema.version) {
        schema.version = 1;
    } else if (schema.version !== 1) {
        throw new Error("Schema version 1 expected, got " + schema.version);
    }

    // Normalize & validate indexes
    schema.index = validateIndexSchema(schema, schema.index);
    if (!schema.secondaryIndexes) {
        schema.secondaryIndexes = {};
    }
    //console.dir(schema.secondaryIndexes);
    for (var index in schema.secondaryIndexes) {
        schema.secondaryIndexes[index] = validateIndexSchema(schema, schema.secondaryIndexes[index]);
    }

    // XXX: validate attributes
    return schema;
}

// Simple array to set conversion
function arrayToSet(arr) {
    var o = {};
    arr.forEach(function(key) {
        o[key] = true;
    });
    return o;
}


// Extract the index keys from a table schema
function indexKeys (index) {
    var res = [];
    index.forEach(function(elem) {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
}

function makeIndexSchema (dataSchema, indexName) {

    var index = dataSchema.secondaryIndexes[indexName];
    var s = {
        name: indexName,
        attributes: {},
        index: index,
        iKeys: [],
        iKeySet: {}
    };

    // Build index attributes for the index schema
    index.forEach(function(elem) {
        var name = elem.attribute;
        s.attributes[name] = dataSchema.attributes[name];
        if (elem.type === 'hash' || elem.type === 'range') {
            s.iKeys.push(name);
            s.iKeySet[name] = true;
        }
    });

    // Make sure the main index keys are included in the new index
    dataSchema.iKeys.forEach(function(att) {
        if (!s.attributes[att]) {
            s.attributes[att] = dataSchema.attributes[att];
            var indexElem = { type: 'range', order: 'desc' };
            indexElem.attribute = att;
            index.push(indexElem);
            s.iKeys.push(att);
            s.iKeySet[att] = true;
        }
    });

    // Add the _deleted field
    s.attributes._deleted = 'timeuuid';

    return s;
}

/*
 * Derive additional schema info from the public schema
 */
function makeSchemaInfo(schema) {
    // Private schema information
    // Start with a deep clone of the schema
    var psi = extend(true, {}, schema);
    // Then add some private properties
    psi.versioned = false;

    // Check if the last index entry is a timeuuid, which we take to mean that
    // this table is versioned
    var lastElem = schema.index[schema.index.length - 1];
    var lastKey = lastElem.attribute;
    if (lastKey && lastElem.type === 'range'
            && lastElem.order === 'desc'
            && schema.attributes[lastKey] === 'timeuuid') {
        psi.tid = lastKey;
    } else {
        // Add a hidden _tid timeuuid attribute
        psi.attributes._tid = 'timeuuid';
        psi.index.push({ _tid: 'desc' });
        psi.tid = '_tid';
    }

    // Create summary data on the primary data index
    psi.iKeys = indexKeys(psi.index);
    psi.iKeySet = arrayToSet(psi.iKeys);


    // Now create secondary index schemas
    // Also, create a map from attribute to indexes
    var indexAttributes = {};
    for (var si in psi.secondaryIndexes) {
        psi.secondaryIndexes[si] = makeIndexSchema(psi, si);
        var idx = psi.secondaryIndexes[si];
        idx.iKeys.forEach(function(att) {
            if (!indexAttributes[att]) {
                indexAttributes[att] = [si];
            } else {
                indexAttributes[att].push(si);
            }
        });
    }
    psi.indexAttributes = indexAttributes;

    return psi;
}



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
    iKeySet: { key: true },
    indexAttributes: {}
};


DB.prototype.buildPutQuery = function(req, keyspace, table, schema) {

    //table = schema.table;

    if (!schema) {
        throw new Error('Table not found!');
    }

    // XXX: should we require non-null secondary index entries too?
    var indexKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (!req.attributes[key]) {
            throw new Error("Index attribute " + key + " missing");
        } else {
            indexKVMap[key] = req.attributes[key];
        }
    });

    var keys = [];
    var params = [];
    var placeholders = [];
    for (var key in req.attributes) {
        var val = req.attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (val && val.constructor === Object) {
                val = JSON.stringify(val);
            }
            if (!schema.iKeySet[key]) {
                keys.push(key);
                params.push(val);
            }
            placeholders.push('?');
        }
    }

    // switch between insfert & update / upsert
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
        var proj = schema.iKeys.concat(keys).map(cassID).join(',');
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
            var schema = JSON.parse(res.items[0].value);
            return makeSchemaInfo(schema);
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
        req.limit = undefined;
    }

    var item, newlimit;
    if (!req.index) {
        // req should not have non primary key attr
        for ( item in req.attributes ) {
            if (!cachedSchema.iKeySet[item]) {
                throw new Error("Request attributes should contain only primary key attributes");
            }
        }
    } else {
        if (!cachedSchema.secondaryIndexes[req.index]) {
            // console.dir(cachedSchema);
            throw new Error("Index not found: " + req.index);
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


    var cachedSchema = this.schemaCache[keyspace].secondaryIndexes[req.index];
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
                var attributes = {};
                var proj = {};
                for (var attr in self.schemaCache[keyspace].iKeySet) {
                    attributes[attr] = row[attr];
                }
                queries.push(self.buildGetQuery(keyspace, {
                                                table: table,
                                                attributes: attributes,
                                                proj: proj,
                                                limit: req.limit + Math.ceil(req.limit/4)},
                                                consistency, table));
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
            for ( var attr in cachedSchema.iKeySet ) {
                newReq.attributes[attr] = results.rows[rowno][attr];
            }
            queries.push(self.buildGetQuery(keyspace, newReq, consistency, table));
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

    //if (req.index) {
    //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    //}

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
    if (schema.tid === '_tid') {
        req.attributes._tid = tidFromDate(new Date());
    }

    // insert into secondary Indexes first
    var batch = [];
    if (schema.secondaryIndexes) {
        for ( var item in schema.secondaryIndexes) {
            var secondarySchema = schema.secondaryIndexes[item];
            if (!secondarySchema) {
                throw new Error('Table not found!');
            }
            var idxTable = 'idx_' + item + '_ever';
            batch.push(this.buildPutQuery(req, keyspace, idxTable, secondarySchema));
        }
    }

    batch.push(this.buildPutQuery(req, keyspace, table, schema));

    //console.log(batch, schema);
    var self = this;
    return this.client.batch_p(batch, {consistency: consistency, prepared: true})
    .then(function(result) {
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
};

DB.prototype._updateIndexes = function (keyspace, req, consistency, table, schema) {
    /* look at sibling revisions to update the index with values that no longer match
    *   - select sibling revisions
    *   - walk results in ascending order and diff each row vs. preceding row
    *      - if diff: for each index affected by that diff, update _deleted for old value
    *        using that revision's TIMESTAMP.
    */
    if (schema.secondaryIndexes && Object.keys(schema.secondaryIndexes).length) {
        // Build queries to select sibling revisions
        var rows = result.rows;

        var newReq = {
            table: req.table,
            attributes: {},
            proj: []
        };

        // Include all primary index attributes
        schema.iKeys.forEach(function(att) {
            newReq1.attributes[att] = req.attributes[att];
        });

        // Include all other indexed attributes in the query
        for (var secIndex in schema.secondaryIndexes) {
            schema.secondaryIndexes[secIndex].iKeys.forEach(function(att) {
                if (!schema.iKeySet[att]) {
                    newReq.proj.push(att);
                }
            });
        }

        // Clone the query, and create le & gt variants
        var gets = [];
        var newReq2 = extend(true, {}, newReq);
        var tidKey = schema.tid;
        var tid = req.attributes[tidKey];
        newReq.attributes[tidKey] = {'le': tid};
        newReq.limit = 3;
        gets.push(self._get(keyspace, newReq, consistency, table));

        newReq2.attributes[tidKey] = {'gt': tid};
        newReq2.limit = 2;
        gets.push(self._get(keyspace, newReq2, consistency, table));
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
            for (var rowNo=0; rowNo < Object.keys(rows).length; rowNo++) {
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
                                        for(var attr in schema.secondaryIndexes[secIndex].attributes) {
                                            newReq1.attributes[attr] = row1[attr];
                                        }
                                        newReq1.attributes._deleted = req.attributes._tid;
                                        newReq1.index = "idx_" + secIndex + "_ever";
                                        batch.push(
                                            // generate put query with _deleted = tuuid
                                            self.buildPutQuery(newReq1, keyspace, secIndex, schema.secondaryIndexes[secIndex])
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

    // Validate and normalize the schema
    var schema = validateAndNormalizeSchema(req);

    var internalSchema = makeSchemaInfo(schema);

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

DB.prototype.dropTable = function (reverseDomain, table) {
    var keyspace = keyspaceName(reverseDomain, table);
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [], {consistency: defaultConsistency});
};


module.exports = DB;
