"use strict";
var uuid = require('node-uuid');
var crypto = require('crypto');
var extend = require('extend');

/*
 * Various static database utility methods
 */

var dbu = {};

dbu.cassID = function cassID (name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
};

dbu.idxTable = function idxTable (name, bucket) {
    var idx = 'idx_' + name;
    if (bucket) {
        return idx + '_' + bucket;
    } else {
        return idx + '_ever';
    }
};

dbu.tidFromDate = function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
};


dbu.buildCondition = function buildCondition (pred) {
    var params = [];
    var conjunctions = [];
    for (var predKey in pred) {
        var cql = '';
        var predObj = pred[predKey];
        cql += dbu.cassID(predKey);
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey)
                    + ' is undefined');
        } else if (predObj === null || predObj.constructor !== Object) {
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
                        cql += dbu.cassID(predKey) + ' <= ?'; params.push(predArg[1]);
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
};

// Hash a key into a valid Cassandra key name
dbu.hashKey = function hashKey (key) {
    return crypto.Hash('sha1')
        .update(key)
        .digest()
        .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
        .replace(/[+\/]/g, '_')
        // Remove base64 padding, has no entropy
        .replace(/=+$/, '');
};

dbu.getValidPrefix = function getValidPrefix (key) {
    var prefixMatch = /^[a-zA-Z0-9_]+/.exec(key);
    if (prefixMatch) {
        return prefixMatch[0];
    } else {
        return '';
    }
};

dbu.makeValidKey = function makeValidKey (key, length) {
    var origKey = key;
    key = key.replace(/_/g, '__')
                .replace(/\./g, '_');
    if (!/^[a-zA-Z0-9_]+$/.test(key)) {
        // Create a new 28 char prefix
        var validPrefix = dbu.getValidPrefix(key).substr(0, length * 2 / 3);
        return validPrefix + dbu.hashKey(origKey).substr(0, length - validPrefix.length);
    } else if (key.length > length) {
        return key.substr(0, length * 2 / 3) + dbu.hashKey(origKey).substr(0, length / 3);
    } else {
        return key;
    }
};


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
dbu.keyspaceName = function keyspaceName (reverseDomain, key) {
    var prefix = dbu.makeValidKey(reverseDomain, Math.max(26, 48 - key.length - 3));
    return prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
        + '_T_' + dbu.makeValidKey(key, 48 - prefix.length - 3);
};

dbu.validateIndexSchema = function validateIndexSchema(schema, index) {

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
};

dbu.validateAndNormalizeSchema = function validateAndNormalizeSchema(schema) {
    if (!schema.version) {
        schema.version = 1;
    } else if (schema.version !== 1) {
        throw new Error("Schema version 1 expected, got " + schema.version);
    }

    // Normalize & validate indexes
    schema.index = dbu.validateIndexSchema(schema, schema.index);
    if (!schema.secondaryIndexes) {
        schema.secondaryIndexes = {};
    }
    //console.dir(schema.secondaryIndexes);
    for (var index in schema.secondaryIndexes) {
        schema.secondaryIndexes[index] = dbu.validateIndexSchema(schema, schema.secondaryIndexes[index]);
    }

    // XXX: validate attributes
    return schema;
};

// Simple array to set conversion
dbu.arrayToSet = function arrayToSet(arr) {
    var o = {};
    arr.forEach(function(key) {
        o[key] = true;
    });
    return o;
};


// Extract the index keys from a table schema
dbu.indexKeys = function indexKeys (index) {
    var res = [];
    index.forEach(function(elem) {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};

dbu.makeIndexSchema = function makeIndexSchema (dataSchema, indexName) {

    var index = dataSchema.secondaryIndexes[indexName];
    var s = {
        name: indexName,
        attributes: {},
        index: index,
        iKeys: [],
        iKeyMap: {}
    };

    // Build index attributes for the index schema
    index.forEach(function(elem) {
        var name = elem.attribute;
        s.attributes[name] = dataSchema.attributes[name];
        if (elem.type === 'hash' || elem.type === 'range') {
            s.iKeys.push(name);
            s.iKeyMap[name] = elem;
        }
    });

    // Make sure the main index keys are included in the new index
    dataSchema.iKeys.forEach(function(att) {
        if (!s.attributes[att] && att !== dataSchema.tid) {
            s.attributes[att] = dataSchema.attributes[att];
            var indexElem = { type: 'range', order: 'desc' };
            indexElem.attribute = att;
            index.push(indexElem);
            s.iKeys.push(att);
            s.iKeyMap[att] = indexElem;
        }
    });

    // Add the data table's tid as a plain attribute, if not yet included
    if (!s.attributes[dataSchema.tid]) {
        var tidKey = dataSchema.tid;
        s.attributes[tidKey] = dataSchema.attributes[tidKey];
        s.tid = tidKey;
    }

    return s;
};

/*
 * Derive additional schema info from the public schema
 */
dbu.makeSchemaInfo = function makeSchemaInfo(schema) {
    // Private schema information
    // Start with a deep clone of the schema
    var psi = extend(true, {}, schema);
    // Then add some private properties
    psi.versioned = false;

    // Check if the last index entry is a timeuuid, which we take to mean that
    // this table is versioned
    var lastElem = schema.index[schema.index.length - 1];
    var lastKey = lastElem.attribute;

    // Add a non-index _del flag to track deletions
    // This is normally null, but will be set on an otherwise empty row to
    // mark the row as deleted.
    psi.attributes._del = 'boolean';

    if (lastKey && lastElem.type === 'range'
            && lastElem.order === 'desc'
            && schema.attributes[lastKey] === 'timeuuid') {
        psi.tid = lastKey;
    } else {
        // Add a hidden _tid timeuuid attribute
        psi.attributes._tid = 'timeuuid';
        psi.index.push({ attribute: '_tid', type: 'range', order: 'desc' });
        psi.tid = '_tid';
    }

    // Create summary data on the primary data index
    psi.iKeys = dbu.indexKeys(psi.index);
    psi.iKeyMap = {};
    psi.index.forEach(function(elem) {
        psi.iKeyMap[elem.attribute] = elem;
    });


    // Now create secondary index schemas
    // Also, create a map from attribute to indexes
    var attributeIndexes = {};
    for (var si in psi.secondaryIndexes) {
        psi.secondaryIndexes[si] = dbu.makeIndexSchema(psi, si);
        var idx = psi.secondaryIndexes[si];
        idx.iKeys.forEach(function(att) {
            if (!attributeIndexes[att]) {
                attributeIndexes[att] = [si];
            } else {
                attributeIndexes[att].push(si);
            }
        });
    }
    psi.attributeIndexes = attributeIndexes;

    return psi;
};

dbu.buildPutQuery = function(req, keyspace, table, schema) {

    //table = schema.table;

    if (!schema) {
        throw new Error('Table not found!');
    }

    // XXX: should we require non-null secondary index entries too?
    var indexKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (req.attributes[key] === undefined) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                    + JSON.stringify(req) + "; schema: " + JSON.stringify(schema, null, 2));
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
            if (!schema.iKeyMap[key]) {
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

    var condRes = dbu.buildCondition(indexKVMap);

    if (!keys.length || req.if === 'not exists') {
        var proj = schema.iKeys.concat(keys).map(dbu.cassID).join(',');
        cql = 'insert into ' + dbu.cassID(keyspace) + '.' + dbu.cassID(table)
                + ' (' + proj + ') values (';
        cql += placeholders.join(',') + ')';
        params = condRes.params.concat(params);
    } else if ( keys.length ) {
        var updateProj = keys.map(dbu.cassID).join(' = ?,') + ' = ? ';
        cql += 'update ' + dbu.cassID(keyspace) + '.' + dbu.cassID(table) +
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
            condResult = dbu.buildCondition(req.if);
            cql += condResult.query;
            params = params.concat(condResult.params);
        }
    }

    return {query: cql, params: params};
};

module.exports = dbu;
