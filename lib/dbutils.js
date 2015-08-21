"use strict";

require('core-js/shim');

var crypto = require('crypto');
var extend = require('extend');
var cass = require('cassandra-driver');
var util = require('util');
var P = require('bluebird');
var stringify = require('json-stable-stringify');
var validator = require('restbase-mod-table-spec').validator;

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

dbu.cassTTL = function cassTTL(name) {
    return '_ttl_' + name;
};

dbu.idxColumnFamily = function idxColumnFamily (name, bucket) {
    var idx = 'idx_' + name;
    if (bucket) {
        return idx + '_' + bucket;
    } else {
        return idx + '_ever';
    }
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
 * Create a schema hash string.
 *
 * @param  {object} schema; a schema object
 * @return {string} the schema serialized to string
 */
dbu.makeSchemaHash = function makeSchemaHash(schema) {
    var clone = Object.assign({}, schema);
    // eliminate _backend_version from hash comparisons (see: DB#createTable)
    delete clone._backend_version;
    return stringify(clone);
};

/**
 * Given a row object, adds a _ttl attribute for the maximum of all
 * contained column TTLs, or undefined if no TTLs are present.
 *
 * @param {object} row; an object representing a result row
 */
dbu.assignMaxTTL = function assignMaxTTL(row) {
    var max;
    Object.keys(row).forEach(function(key) {
        if (/^_ttl_.+/.test(key)) {
            if (max === undefined) {
                max = row[key];
            }
            else if (row[key] > max) {
                max = row[key];
            }
        }
    });
    row._ttl = max;
};

function _nextPage(client, query, params, pageState, options) {
    return P.try(function() {
        return client.execute_p(query, params, {
            prepare: true,
            fetchSize: options.fetchSize || 5,
            pageState: pageState,
        });
    })
    .catch(function(err) {
        if (!options.retries) {
            throw err;
        }
        options.retries--;
        return _nextPage(client, query, params, pageState, options);
    });
}

/**
 * Async-safe Cassandra query execution
 *
 * Client#eachRow in the Cassandra driver relies upon a synchronous callback
 * to provide back-pressure during paging; This function can safely execute
 * async callback handlers.
 *
 * @param   {object} cassandra-driver Client instance
 * @param   {string} CQL query string
 * @param    {array} CQL query params
 * @param   {object} options map
 * @param {function} function to invoke for each row result
 */
dbu.eachRow = function eachRow(client, query, params, options, handler) {
    options.log = options.log || function() {};
    function processPage(pageState) {
        return _nextPage(client, query, params, pageState, options)
        .then(function(res) {
            return P.try(function() {
                return P.each(res.rows, function(row) {
                    // Decorate the row result with the _ttl attribute.
                    if (options.withTTLs) {
                        dbu.assignMaxTTL(row);
                    }
                    handler(row);
                });
            }).then(function() {
                if (!res || res.pageState === null) {
                    return P.resolve();
                } else {
                    // Break the promise chain, so that we don't hold onto a
                    // previous page's memory.
                    process.nextTick(function() {
                        return P.try(function() {
                            return processPage(res.pageState);
                        }).catch(function(e) {
                            // there's something going on, just log it
                            // since we have broken the promise chain
                            options.log('error/cassandra/backgroundUpdates', e);
                        });
                    });
                }
            });
        });
    }

    return processPage(null);
};

/*
 * # Section 2: Schema validation, normalization and -handling
 */

dbu.DEFAULT_BACKEND_VERSION = 0;
dbu.CURRENT_BACKEND_VERSION = 1;

/**
 * Wrapper for validator#validateAndNormalizeSchema (shipped in
 * restbase-m-t-spec). Ensures the presence of the private,
 * implementation-specific _backend_version schema attribute.
 */
dbu.validateAndNormalizeSchema = function validateAndNormalizeSchema(schema) {
    if (!schema._backend_version) {
        schema._backend_version = dbu.CURRENT_BACKEND_VERSION;
    }
    return validator.validateAndNormalizeSchema(schema);
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
        iKeyMap: {},
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
dbu.makeSchemaInfo = function makeSchemaInfo(schema, isMetaCF) {
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

    if (!isMetaCF) {
        // Prefix a _domain attribute to each hash key, so that we can share CFs
        // between groups of domains
        psi.attributes._domain = 'string';
        psi.index.unshift({ attribute: '_domain', type: 'hash' });
        if (psi.secondaryIndexes) {
            Object.keys(psi.secondaryIndexes).forEach(function(idxName) {
                var idx = psi.secondaryIndexes[idxName];
                idx.unshift({ attribute: '_domain', type: 'hash' });
            });
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
    psi.staticKeyMap = {};
    psi.index.forEach(function(elem) {
        if (elem.type === 'static') {
            psi.staticKeyMap[elem.attribute] = elem;
        } else {
            psi.iKeyMap[elem.attribute] = elem;
        }
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

    if (!psi.revisionRetentionPolicy) {
        psi.revisionRetentionPolicy = { type: 'all' };
    }

    if (!psi._backend_version) {
        psi._backend_version = dbu.DEFAULT_BACKEND_VERSION;
    }

    // define a 'hash' string representation for the schema for quick schema
    // comparisons.
    psi.hash = dbu.makeSchemaHash(psi);

    return psi;
};


/**
 * Converts an array of result rows from Cassandra to JS values
 *
 * @param {array} rows the result rows to convert; not modified
 * @param {object} schema the schema info to use for conversion
 * @returns {array} a converted array of result rows
 */
dbu.convertRows = function convertRows (rows, schema) {
    var conversions = schema.conversions;
    var newRows = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        var newRow = {};
        Object.keys(row).forEach(function(att) {
            // Skip over internal attributes
            if (!/^_/.test(att)) {
                if (row[att] !== null && conversions[att]
                        && conversions[att].read) {
                    newRow[att] = schema.conversions[att].read(row[att]);
                } else {
                    newRow[att] = row[att];
                }
            }
        });
        newRows[i] = newRow;
    }
    return newRows;
};

/**
 * Deep-clones and converts an internal request's query attributes to native
 * Cassandra representations
 *
 * @param {InternalRequest} internalReq the request whose attributes to convert
 * @param {Object} extendFields any other fields to use when extending the request object; optional
 * @return {InternalRequest} the clone of the request passed in, with converted values
 */
dbu.makeRawRequest = function(internalReq, extendFields) {
    var conversions = (internalReq.schema || {}).conversions;
    extendFields = extendFields || {};
    extendFields.query = extend(true, {}, internalReq.query);
    var clonedReq = internalReq.extend(extendFields);
    var attrs = clonedReq.query.attributes;
    if(!conversions || !attrs) {
        return clonedReq;
    }
    Object.keys(attrs).forEach(function(key) {
        var conv = conversions[key];
        if(conv && conv.write) {
            attrs[key] = conv.write(attrs[key]);
        }
    });
    return clonedReq;
};

/*
 * # Section 3: CQL query generation
 */

/**
 * CQL building for conditional requests in general.
 * @param {object} predicates, the 'attributes' object in queries.
 * @param {object} schema, the schema info for the logical table.
 * @param {Boolean} noConvert if true, no attribute value conversion will take place
 * @return {object} queryInfo object with cql and params attributes
 */
dbu.buildCondition = function buildCondition (predicates, schema, noConvert) {
    function convert(key, val) {
        var convObj = schema.conversions[key];
        if (!noConvert && convObj && convObj.write) {
            return convObj.write(val);
        } else {
            return val;
        }
    }

    // make sure we have got a predicate object
    if(!predicates || predicates.constructor !== Object) {
        throw new Error('The condition predicate has not been supplied or is not an Object.');
    }

    var params = [];
    var conjunctions = [];
    Object.keys(predicates).forEach(function(predKey) {
        var predObj = predicates[predKey];
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey)
                    + ' is undefined');
        } else if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            conjunctions.push(dbu.cassID(predKey) + ' = ?');
            params.push(convert(predKey, predObj));
        } else {
            Object.keys(predObj).forEach(function(predOp) {
                var predArg = predObj[predOp];
                var cql = dbu.cassID(predKey);
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
                    case 'between':
                        cql += ' >= ?' + ' AND ';
                        params.push(convert(predKey, predArg[0]));
                        cql += dbu.cassID(predKey) + ' <= ?';
                        params.push(convert(predKey, predArg[1]));
                        break;
                    default: throw new Error ('Operator ' + predOp + ' not supported!');
                }
                conjunctions.push(cql);
            });
        }
    });
    return {
        cql: conjunctions.join(' AND '),
        params: params,
    };
};


/**
 * CQL building for PUT queries
 * @param {InternalRequest} req
 * @param {Boolean} noConvert if true, no attribute value conversion will take place
 * @return {object} queryInfo object with cql and params attributes
 */
dbu.buildPutQuery = function(req, noConvert) {

    //table = schema.table;

    if (!req.schema) {
        throw new Error('Table not found!');
    }
    var schema = req.schema;
    var query = req.query;

    // Convert the attributes
    var attributes = query.attributes || {};
    if (req.columnfamily !== 'meta') {
        attributes._domain = req.domain;
    }
    var conversions = schema.conversions || {};

    // XXX: should we require non-null secondary index entries too?
    var indexKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (attributes[key] === undefined) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                    + JSON.stringify(query) + "; schema: " + JSON.stringify(schema, null, 2));
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
                if (!noConvert && conversionObj && conversionObj.write) {
                    val = conversionObj.write(val);
                }
                if (val !== null && schema.staticKeyMap && !schema.staticKeyMap[key]) {
                    haveNonIndexNonNullValue = true;
                }
                params.push(val);
            }
            placeholders.push('?');
        }
    });


    var using = '';
    var usingBits = [];
    var usingParams = [];
    if (query.timestamp && !query.if) {
        usingBits.push('TIMESTAMP ?');
        usingParams.push(cass.types.Long.fromNumber(Math.round(query.timestamp * 1000)));
    }
    if (req.ttl) {
        usingBits.push('TTL ?');
        usingParams.push(cass.types.Long.fromNumber(req.ttl));
    }

    if (usingBits.length) {
        using = ' USING ' + usingBits.join(' AND ');
    }

    // switch between insert & update / upsert
    // - insert for 'if not exists', or when no non-primary-key attributes are
    //   specified, or they are all null (as Cassandra does not distinguish the two)
    // - update when any non-primary key attributes are supplied
    //   - Need to verify that all primary key members are supplied as well,
    //     else error.

    var cql = '', condResult;

    if (query.if && query.if.constructor === String) {
        query.if = query.if.trim().split(/\s+/).join(' ').toLowerCase();
        if (query.if !== 'not exists') {
            throw new Error("Only 'not exists' conditionals are supported.");
        }
    }

    var condRes = dbu.buildCondition(indexKVMap, schema, noConvert);

    var cond = '';
    if (!haveNonIndexNonNullValue || query.if === 'not exists') {
        if (query.if === 'not exists') {
            cond = ' if not exists ';
        }
        var proj = schema.iKeys.concat(nonIndexKeys).map(dbu.cassID).join(',');
        cql = 'insert into ' + dbu.cassID(req.keyspace) + '.' + dbu.cassID(req.columnfamily)
                + ' (' + proj + ') values (';
        cql += placeholders.join(',') + ')' + cond + using;
        params = condRes.params.concat(params, usingParams);
    } else if (nonIndexKeys.length) {
        var condParams = [];
        var condTypeHints = [];
        var condParamKeys = [];
        if (query.if) {
            cond = ' if ';
            condResult = dbu.buildCondition(query.if, schema, noConvert);
            cond += condResult.cql;
            condParams = condResult.params;
            condParamKeys = condResult.keys;
        }

        var updateProj = nonIndexKeys.map(dbu.cassID).join(' = ?,') + ' = ? ';
        cql += 'update ' + dbu.cassID(req.keyspace) + '.' + dbu.cassID(req.columnfamily)
               + using + ' set ' + updateProj + ' where ';
        cql += condRes.cql + cond;
        params = usingParams.concat(params, condRes.params, condParams);

    } else {
        throw new Error("Can't Update or Insert");
    }

    return {
        cql: cql,
        params: params,
    };
};


/**
 * CQL building for GET queries
 * @param {InternalRequest} req
 * @param  {object} options map
 * @return {object} queryInfo object with cql and params attributes
 */
dbu.buildGetQuery = function(req, options) {
    options = options || {};
    var query = req.query;
    if (!query) {
        throw new Error('Query missing!');
    }
    var schema = req.schema;

    if (query.index) {
        if (!schema.secondaryIndexes[query.index]) {
            // console.dir(cachedSchema);
            throw new Error("Index not found: " + query.index);
        }
        schema = schema.secondaryIndexes[query.index];
        req.columnfamily = dbu.idxColumnFamily(query.index);
    }

    var projCQL = Object.keys(schema.attributes).map(dbu.cassID).join(',');
    var projAttrs = schema.attributes;

    if (query.proj) {
        if (Array.isArray(query.proj)) {
            projCQL = query.proj.map(dbu.cassID).join(',');
            projAttrs = query.proj;
        } else if (query.proj.constructor === String) {
            projCQL = dbu.cassID(query.proj);
            projAttrs = [query.proj];
        }
    }

    // Add TTL attributes for all non-index attributes
    if (options.withTTLs) {
        // Candidates for TTL are non-index, non-collection, attributes
        var ttlCandidates = Object.keys(projAttrs).filter(
            function(v) {
                return !schema.iKeyMap[v] && !/^(set|map|list)<.*>$/.test(schema.attributes[v]);
            }
        );
        var projTTLs = ttlCandidates.map(
            function(v) {
                return 'TTL(' + dbu.cassID(v) + ') as ' + dbu.cassID(dbu.cassTTL(v));
            }
        );
        projCQL += ',' + projTTLs.join(',');
    }

    if (query.distinct) {
        projCQL = 'distinct ' + projCQL;
    }

    var cql = 'select ' + projCQL + ' from '
        + dbu.cassID(req.keyspace) + '.' + dbu.cassID(req.columnfamily);

    // Build up the condition
    var params = [];
    var attributes = query.attributes || {};
    if (req.columnfamily !== 'meta') {
        attributes._domain = req.domain;
    }
    Object.keys(attributes).forEach(function(key) {
        // query should not have non key attributes
        if (!schema.iKeyMap[key]) {
            throw new Error("All request attributes need to be key attributes. Bad attribute: "
                    + key);
        }
    });
    cql += ' where ';
    var condResult = dbu.buildCondition(attributes, schema);
    cql += condResult.cql;
    params = condResult.params;

    if (query.order) {
        var reversed;
        // Establish whether we need to read in forward or reverse order,
        // which is what Cassandra supports. Also validate the order for
        // consistency.
        for (var att in query.order) {
            var dir = query.order[att];
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

    // Generally, req.query.limit is used to limit per-page results, which is
    // managed through the driver's pageState.  When it's necessary to use a
    // CQL LIMIT, it should be included in options.
    if (options.limit) {
        var limit = parseInt(options.limit);
        cql += limit ? ' limit ' + limit : '';
    }

    return {cql: cql, params: params};
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
