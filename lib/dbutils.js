"use strict";
var crypto = require('crypto');
var extend = require('extend');
var cass = require('cassandra-driver');
var Uuid = cass.types.Uuid;
var TimeUuid = cass.types.TimeUuid;
var Integer = cass.types.Integer;
var BigDecimal = cass.types.BigDecimal;
var util = require('util');

/*
 * Various static database utility methods
 *
 * Three main sections:
 * 1) low-level helpers
 * 2) schema handling
 * 3) CQL query building
 */

var dbu = {};

/*
 * # Section 1: low-level helpers
 */


/*
 * Error instance wrapping HTTP error responses
 *
 * Has the same properties as the original response.
 */
function HTTPError(response) {
    Error.call(this);
    Error.captureStackTrace(this, HTTPError);
    this.name = this.constructor.name;
    this.message = JSON.stringify(response);

    for (var key in response) {
        this[key] = response[key];
    }
}
util.inherits(HTTPError, Error);
dbu.HTTPError = HTTPError;

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


// Create a deterministic TimeUuid from a date. Don't use outside of tests, use
// TimeUuid.fromDate(date) with proper entropy instead.
dbu.testTidFromDate = function testTidFromDate(date, useCassTicks) {
    var tidNode = new Buffer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]);
    var tidClock = new Buffer([0x12, 0x34]);
    return new TimeUuid(date, useCassTicks ? null : 0, tidNode, tidClock);
};

dbu.tidNanoTime = function(tid) {
    var datePrecision = tid.getDatePrecision();
    return datePrecision.date.getTime() + datePrecision.ticks / 1000;
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


/*
 * # Section 2: Schema validation, normalization and -handling
 */

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

    // Check options
    if (schema.options) {
        var opts = schema.options;
        for (var key in opts) {
            var val = opts[key];
            switch(key) {
            case 'compression':
                if (!Array.isArray(val)
                        || !val.length
                        || val.some(function(algo) {
                            var cassAlgo = dbu.validCompressionAlgorithms[algo.algorithm];
                            return cassAlgo === undefined || cassAlgo === false;
                        })) {
                    throw new Error('Invalid compression settings: '
                            + JSON.stringify(val));
                }
                break;
            case 'durability':
                if (val !== 'low' && val !== 'standard') {
                    throw new Error ('Invalid durability level: ' + opts[key]);
                }
                break;
            default:
                throw new Error('Unknown option: ' + key);
            }
        }
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

    // include the orignal schema's conversion table
    s.conversions = {};
    if (dataSchema.conversions) {
        for (var attr in s.attributes) {
            if (dataSchema.conversions[attr]) {
                s.conversions[attr] = dataSchema.conversions[attr];
            }
        }
    }

    s.attributes._del = 'timeuuid';

    return s;
};

function encodeBlob (blob) {
    if (blob instanceof Buffer) {
        return blob;
    } else {
        return new Buffer(blob);
    }
}


var schemaTypeToCQLTypeMap = {
    'blob': 'blob',
    'set<blob>': 'set<blob>',
    'decimal': 'decimal',
    'set<decimal>': 'set<decimal>',
    'double': 'double',
    'set<double>': 'set<double>',
    'float': 'float',
    'set<float>': 'set<float>',
    'boolean': 'boolean',
    'set<boolean>': 'set<boolean>',
    'int': 'int',
    'set<int>': 'set<int>',
    'varint': 'varint',
    'set<varint>': 'set<varint>',
    'string': 'text',
    'set<string>': 'set<text>',
    'timeuuid': 'timeuuid',
    'set<timeuuid>': 'set<timeuuid>',
    'uuid': 'uuid',
    'set<uuid>': 'set<uuid>',
    'timestamp': 'timestamp',
    'set<timestamp>': 'set<timestamp>',
    'json': 'text',
    'set<json>': 'set<text>'
};

// Map a schema type to the corresponding CQL type
dbu.schemaTypeToCQLType = function(schemaType) {
    var cqlType = schemaTypeToCQLTypeMap[schemaType];
    if (!cqlType) {
        throw new Error('Invalid schema type ' + cqlType);
    }
    return cqlType;
};


/**
 * Generates read/write conversion functions for set-typed attributes
 *
 * @param {Object} convObj the conversion object to use for individual values (from dbu.conversions)
 * @returns {Object} an object with 'read' and 'write' attributes
 */
function generateSetConvertor (convObj) {
    if (!convObj) {
        return {
            write: function(arr) {
                // Default to-null conversion for empty sets
                if (!Array.isArray(arr) || arr.length === 0) {
                    return null;
                } else {
                    return arr;
                }
            },
            // XXX: Should we convert null to the empty array here?
            read: null
        };
    }
    var res = {
        write: null,
        read: null
    };
    if (convObj.write) {
        res.write = function (valArray) {
            if (!Array.isArray(valArray) || valArray.length === 0) {
                // Empty set is equivalent to null in Cassandra
                return null;
            } else {
                return valArray.map(convObj.write);
            }
        };
    }
    if (convObj.read) {
        res.read = function (valArray) {
            return valArray.map(convObj.read);
        };
    }
    return res;
}

// Conversion factories. We create a function for each type so that it can be
// compiled monomorphically.
function toString() {
    return function(val) {
        return val.toString();
    };
}
function toNumber() {
    return function(val) {
        return val.toNumber();
    };
}

dbu.conversions = {
    json: { write: JSON.stringify, read: JSON.parse },
    decimal: { read: toString() },
    timestamp: {
        read: function (date) {
            return date.toISOString();
        }
    },
    blob: { write: encodeBlob },
    varint: { read: toNumber() },
    timeuuid: { read: toString() },
    uuid: { read: toString() }
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

    // Extract attributes that need conversion in the read or write path
    psi.conversions = {};
    for (var att in psi.attributes) {
        var type = psi.attributes[att];
        var set_type = /^set<(\w+)>$/.exec(type);
        if (set_type) {
            // this is a set-typed attribute
            type = set_type[1];
            // generate the convertors only if the underlying type has them defined
            psi.conversions[att] = generateSetConvertor(dbu.conversions[type]);
        } else if (dbu.conversions[type]) {
            // this is regular type and conversion methods are defined for it
            psi.conversions[att] = dbu.conversions[type];
        }
    }

    // Add a non-index _del flag to track deletions
    // This is normally null, but will be set on an otherwise empty row to
    // mark the row as deleted.
    psi.attributes._del = 'timeuuid';

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


/**
 * Converts a result row from Cassandra to JS values
 *
 * @param {Row} row the result row to convert; modified in place
 * @param {Schema} schema the schema to use for conversion
 * @returns {Row} the row with converted attribute values
 */
dbu.convertRow = function convertRow (row, schema) {
    Object.keys(row).forEach(function(att) {
        if (row[att] !== null && schema.conversions[att] && schema.conversions[att].read) {
            row[att] = schema.conversions[att].read(row[att]);
        }
    });
    return row;
};

dbu.convertRows = function convertRows (rows, schema) {
    rows.forEach(function(row) {
        dbu.convertRow(row, schema);
    });
    return rows;
};

/*
 * # Section 3: CQL query generation
 */

dbu.buildCondition = function buildCondition (pred, schema) {
    function convert(key, val) {
        var convObj = schema.conversions[key];
        if (convObj && convObj.write) {
            return convObj.write(val);
        } else {
            return val;
        }
    }

    var params = [];
    var conjunctions = [];
    Object.keys(pred).forEach(function(predKey) {
        var cql = '';
        var predObj = pred[predKey];
        cql += dbu.cassID(predKey);
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey)
                    + ' is undefined');
        } else if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            cql += ' = ?';
            params.push(convert(predKey, predObj));
        } else {
            var predKeys = Object.keys(predObj);
            if (predKeys.length === 1) {
                var predOp = predKeys[0];
                var predArg = predObj[predOp];
                // TODO: Combine the repetitive cases here
                switch (predOp.toLowerCase()) {
                case 'eq':
                    cql += ' = ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'lt':
                    cql += ' < ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'gt':
                    cql += ' > ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'le':
                    cql += ' <= ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'ge':
                    cql += ' >= ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'neq':
                case 'ne':
                    cql += ' != ?';
                    params.push(convert(predKey, predArg));
                    break;
                case 'between':
                        cql += ' >= ?' + ' AND ';
                        params.push(convert(predKey, predArg[0]));
                        cql += dbu.cassID(predKey) + ' <= ?';
                        params.push(convert(predKey, predArg[1]));
                        break;
                default: throw new Error ('Operator ' + predOp + ' not supported!');
                }
            } else {
                throw new Error ('Invalid predicate ' + JSON.stringify(pred));
            }
        }
        conjunctions.push(cql);
    });
    return {
        query: conjunctions.join(' AND '),
        params: params,
    };
};

dbu.buildPutQuery = function(req, keyspace, table, schema) {

    //table = schema.table;

    if (!schema) {
        throw new Error('Table not found!');
    }

    // Convert the attributes
    var attributes = req.attributes;
    var conversions = schema.conversions || {};

    // XXX: should we require non-null secondary index entries too?
    var indexKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (attributes[key] === undefined) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                    + JSON.stringify(req) + "; schema: " + JSON.stringify(schema, null, 2));
        } else {
            indexKVMap[key] = attributes[key];
        }
    });

    var nonIndexKeys = [];
    var params = [];
    var placeholders = [];
    var haveNonIndexNonNullValue = false;
    Object.keys(attributes).forEach(function(key) {
        var val = attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (!schema.iKeyMap[key]) {
                nonIndexKeys.push(key);
                // Convert the parameter value
                var conversionObj = conversions[key];
                if (conversionObj && conversionObj.write) {
                    val = conversionObj.write(val);
                }
                if (val !== null) {
                    haveNonIndexNonNullValue = true;
                }
                params.push(val);
            }
            placeholders.push('?');
        }
    });

    var using = '';
    var usingParams = [];
    var usingTypeHints = [];
    var usingParamsKeys = [];
    if (req.timestamp && !req.if) {
        using = ' USING TIMESTAMP ? ';
        usingParams.push(cass.types.Long.fromNumber(Math.round(req.timestamp * 1000)));
        usingParamsKeys.push(null);
    }

    // switch between insert & update / upsert
    // - insert for 'if not exists', or when no non-primary-key attributes are
    //   specified, or they are all null (as Cassandra does not distinguish the two)
    // - update when any non-primary key attributes are supplied
    //   - Need to verify that all primary key members are supplied as well,
    //     else error.

    var cql = '', condResult;

    if (req.if && req.if.constructor === String) {
        req.if = req.if.trim().split(/\s+/).join(' ').toLowerCase();
    }

    var condRes = dbu.buildCondition(indexKVMap, schema);

    var cond = '';
    if (!haveNonIndexNonNullValue || req.if === 'not exists') {
        if (req.if === 'not exists') {
            cond = ' if not exists ';
        }
        var proj = schema.iKeys.concat(nonIndexKeys).map(dbu.cassID).join(',');
        cql = 'insert into ' + dbu.cassID(keyspace) + '.' + dbu.cassID(table)
                + ' (' + proj + ') values (';
        cql += placeholders.join(',') + ')' + cond + using;
        params = condRes.params.concat(params, usingParams);
    } else if (nonIndexKeys.length) {
        var condParams = [];
        var condTypeHints = [];
        var condParamKeys = [];
        if (req.if) {
            cond = ' if ';
            condResult = dbu.buildCondition(req.if, schema);
            cond += condResult.query;
            condParams = condResult.params;
            condParamKeys = condResult.keys;
        }

        var updateProj = nonIndexKeys.map(dbu.cassID).join(' = ?,') + ' = ? ';
        cql += 'update ' + dbu.cassID(keyspace) + '.' + dbu.cassID(table)
               + using + ' set ' + updateProj + ' where ';
        cql += condRes.query + cond;
        params = usingParams.concat(params, condRes.params, condParams);

    } else {
        throw new Error("Can't Update or Insert");
    }

    return {
        query: cql,
        params: params,
    };
};

dbu.buildGetQuery = function(keyspace, req, consistency, table, schema) {
    var proj = '*';

    if (req.index) {
        if (!schema.secondaryIndexes[req.index]) {
            // console.dir(cachedSchema);
            throw new Error("Index not found: " + req.index);
        }
        schema = schema.secondaryIndexes[req.index];
        table = dbu.idxTable(req.index);
    }

    if (req.proj) {
        if (Array.isArray(req.proj)) {
            proj = req.proj.map(dbu.cassID).join(',');
        } else if (req.proj.constructor === String) {
            proj = dbu.cassID(req.proj);
        }
    } else if (req.order) {
        // Work around 'order by' bug in cassandra when using *
        // Trying to change the natural sort order only works with a
        // projection in 2.0.9
        if (schema) {
            proj = Object.keys(schema.attributes).map(dbu.cassID).join(',');
        }
    }

    if (req.limit && req.limit.constructor !== Number) {
        req.limit = undefined;
    }


    if (req.distinct) {
        proj = 'distinct ' + proj;
    }

    var cql = 'select ' + proj + ' from '
        + dbu.cassID(keyspace) + '.' + dbu.cassID(table);

    // Build up the condition
    var params = [];
    var attributes = req.attributes;
    if (attributes) {
        Object.keys(attributes).forEach(function(key) {
            // req should not have non key attributes
            if (!schema.iKeyMap[key]) {
                throw new Error("All request attributes need to be key attributes. Bad attribute: "
                        + key);
            }
        });
        cql += ' where ';
        var condResult = dbu.buildCondition(attributes, schema);
        cql += condResult.query;
        params = condResult.params;
    }

    if (req.order) {
        var reversed;
        // Establish whether we need to read in forward or reverse order,
        // which is what Cassandra supports. Also validate the order for
        // consistency.
        for (var att in req.order) {
            var dir = req.order[att];
            if (dir !== 'asc' && dir !== 'desc') {
                throw new Error("Invalid sort order " + dir + " on key " + att);
            }
            var idxElem = schema.iKeyMap[att];
            if (!idxElem || idxElem.type !== 'range') {
                throw new Error("Cannot order on attribute " + att
                    + "; needs to be a range index, but is " + idxElem);
            }
            var shouldBeReversed = dir !== idxElem.order;
            if (reversed === undefined) {
                reversed = shouldBeReversed;
            } else if (reversed !== shouldBeReversed) {
                throw new Error("Inconsistent sort order; Cassandra only supports "
                        + "reversing the default sort order.");
            }
        }

        // Finally, build up the order query
        var toDir = {
            asc: reversed ? 'desc' : 'asc',
            desc: reversed ? 'asc' : 'desc'
        };
        var orderTerms = [];
        schema.index.forEach(function(elem) {
            if (elem.type === 'range') {
                var dir = toDir[elem.order];
                orderTerms.push(dbu.cassID(elem.attribute) + ' ' + dir);
            }
        });

        if (orderTerms.length) {
            cql += ' order by ' + orderTerms.join(',');
        }
    }

    if (req.limit) {
        cql += ' limit ' + req.limit;
    }

    return {query: cql, params: params};
};


dbu.validCompressionAlgorithms = {
    lz4: 'LZ4Compressor',
    deflate: 'DeflateCompressor',
    lzma: false,
    snappy: 'SnappyCompressor'
};

dbu.validCompressionBlockSizes = {
    64: 1,
    128: 1,
    256: 1,
    512: 1,
    1024: 1
};

dbu.getTableCompressionCQL = function(compressions) {
    for (var i = 0; i < compressions.length; i++) {
        var option = compressions[i];
        if (option
                && dbu.validCompressionAlgorithms[option.algorithm]
                && dbu.validCompressionAlgorithms[option.algorithm].constructor === String
                && dbu.validCompressionBlockSizes[option.block_size]) {
            return " and compression = { 'sstable_compression' : '"
                + dbu.validCompressionAlgorithms[option.algorithm]
                + "', 'chunk_length_kb' : " + option.block_size + " }";
        }
    }
    return '';
};



module.exports = dbu;
