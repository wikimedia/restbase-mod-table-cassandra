"use strict";

require('core-js/shim');

const crypto = require('crypto');
const extend = require('extend');
const cass = require('cassandra-driver');
const P = require('bluebird');
const stableStringify = require('json-stable-stringify');
const validator = require('restbase-mod-table-spec').validator;
const Long = require('cassandra-driver').types.Long;

/*
 * Various static database utility methods
 *
 * Three main sections:
 * 1) low-level helpers
 * 2) schema handling
 * 3) CQL query building
 */

const dbu = {};

/*
 * # Section 1: low-level helpers
 */


/*
 * Error instance wrapping HTTP error responses
 *
 * Has the same properties as the original response.
 */
class HTTPError extends Error {
    constructor(response) {
        super();
        Error.captureStackTrace(this, HTTPError);
        this.name = this.constructor.name;
        this.message = JSON.stringify(response);
        Object.assign(this, response);
    }
}

dbu.HTTPError = HTTPError;

dbu.cassID = function cassID(name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return `"${name}"`;
    } else {
        return `"${name.replace(/"/g, '""')}"`;
    }
};

dbu.cassTTL = function cassTTL(name) {
    return `_ttl_${name}`;
};

// Hash a key into a valid Cassandra key name
dbu.hashKey = function hashKey(key) {
    return new crypto.Hash('sha1')
        .update(key)
        .digest()
        .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
        .replace(/[+/]/g, '_')
        // Remove base64 padding, has no entropy
        .replace(/=+$/, '');
};

dbu.getValidPrefix = function getValidPrefix(key) {
    const prefixMatch = /^[a-zA-Z0-9_]+/.exec(key);
    if (prefixMatch) {
        return prefixMatch[0];
    } else {
        return '';
    }
};

dbu.makeValidKey = function makeValidKey(key, length) {
    const origKey = key;
    key = key.replace(/_/g, '__')
                .replace(/\./g, '_');
    if (!/^[a-zA-Z0-9_]+$/.test(key)) {
        // Create a new 28 char prefix
        const validPrefix = dbu.getValidPrefix(key).substr(0, length * 2 / 3);
        return validPrefix + dbu.hashKey(origKey).substr(0, length - validPrefix.length);
    } else if (key.length > length) {
        return key.substr(0, length * 2 / 3) + dbu.hashKey(origKey).substr(0, length / 3);
    } else {
        return key;
    }
};


/**
 * Given a row object, adds a _ttl attribute for the maximum of all
 * contained column TTLs, or undefined if no TTLs are present.
 * @param {Object} row an object representing a result row
 */
dbu.assignMaxTTL = function assignMaxTTL(row) {
    let max;
    Object.keys(row).forEach((key) => {
        if (/^_ttl_.+/.test(key)) {
            if (max === undefined) {
                max = row[key];
            } else if (row[key] > max) {
                max = row[key];
            }
        }
    });
    row._ttl = max;
};

function _nextPage(client, query, params, pageState, options) {
    return P.try(() => client.execute(query, params, {
        prepare: true,
        fetchSize: options.fetchSize || 5,
        pageState,
    }))
    .catch((err) => {
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
 * @param {Object} client cassandra-driver Client instance
 * @param {string} query CQL query string
 * @param {Array}  params CQL query params
 * @param {Object} options options map
 * @param {Function} handler to invoke for each row result
 */
dbu.eachRow = function eachRow(client, query, params, options, handler) {
    options.log = options.log || (() => {});
    function processPage(pageState) {
        return _nextPage(client, query, params, pageState, options)
        .then(res => P.try(() => P.each(res.rows, (row) => {
            // Decorate the row result with the _ttl attribute.
            if (options.withTTL) {
                dbu.assignMaxTTL(row);
            }
            handler(row);
        })).then(() => {
            if (!res || res.pageState === null) {
                return P.resolve();
            } else {
                // Break the promise chain, so that we don't hold onto a
                // previous page's memory.
                process.nextTick(() => P.try(() => processPage(res.pageState)).catch((e) => {
                    // there's something going on, just log it
                    // since we have broken the promise chain
                    options.log('error/cassandra/backgroundUpdates', e);
                }));
            }
        }));
    }

    return processPage(null);
};

/*
 * # Section 2: Schema validation, normalization and -handling
 */

dbu.DEFAULT_BACKEND_VERSION = 0;
dbu.CURRENT_BACKEND_VERSION = 2;

dbu.DEFAULT_CONFIG_VERSION = 0;    // Implicit module config version.

/**
 * Wrapper for validator#validateAndNormalizeSchema (shipped in
 * restbase-m-t-spec). Ensures the presence of the private,
 * implementation-specific version attributes.
 */
dbu.validateAndNormalizeSchema = function validateAndNormalizeSchema(schema, configVer) {
    if (!schema._backend_version) {
        schema._backend_version = dbu.CURRENT_BACKEND_VERSION;
    }
    if (configVer) {
        schema._config_version = configVer;
    }
    return validator.validateAndNormalizeSchema(schema);
};

// Extract the index keys from a table schema
dbu.indexKeys = function indexKeys(index) {
    const res = [];
    index.forEach((elem) => {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};


function encodeBlob(blob) {
    if (blob instanceof Buffer) {
        return blob;
    } else {
        return new Buffer(blob);
    }
}


const schemaTypeToCQLTypeMap = {
    blob: 'blob',
    'set<blob>': 'set<blob>',
    decimal: 'decimal',
    'set<decimal>': 'set<decimal>',
    double: 'double',
    'set<double>': 'set<double>',
    float: 'float',
    'set<float>': 'set<float>',
    boolean: 'boolean',
    'set<boolean>': 'set<boolean>',
    int: 'int',
    'set<int>': 'set<int>',
    varint: 'varint',
    'set<varint>': 'set<varint>',
    string: 'text',
    'set<string>': 'set<text>',
    timeuuid: 'timeuuid',
    'set<timeuuid>': 'set<timeuuid>',
    uuid: 'uuid',
    'set<uuid>': 'set<uuid>',
    timestamp: 'timestamp',
    'set<timestamp>': 'set<timestamp>',
    json: 'text',
    'set<json>': 'set<text>',
    long: 'bigint',
    'set<long>': 'set<bigint>'
};

// Map a schema type to the corresponding CQL type
dbu.schemaTypeToCQLType = (schemaType) => {
    const cqlType = schemaTypeToCQLTypeMap[schemaType];
    if (!cqlType) {
        throw new Error(`Invalid schema type ${cqlType}`);
    }
    return cqlType;
};


/**
 * Generates read/write conversion functions for set-typed attributes
 * @param {Object} convObj the conversion object to use for individual values (from dbu.conversions)
 * @return {Object} an object with 'read' and 'write' attributes
 */
function generateSetConvertor(convObj) {
    if (!convObj) {
        return {
            write(arr) {
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
    const res = {
        write: null,
        read: null
    };
    if (convObj.write) {
        res.write = (valArray) => {
            if (!Array.isArray(valArray) || valArray.length === 0) {
                // Empty set is equivalent to null in Cassandra
                return null;
            } else {
                return valArray.map(convObj.write);
            }
        };
    }
    if (convObj.read) {
        res.read = valArray => valArray.map(convObj.read);
    }
    return res;
}

// Conversion factories. We create a function for each type so that it can be
// compiled monomorphically.
function toString() {
    return val => val.toString();
}
function toNumber() {
    return val => val.toNumber();
}

dbu.conversions = {
    json: { write: JSON.stringify, read: JSON.parse },
    decimal: { read: toString() },
    timestamp: {
        read(date) {
            return date.toISOString();
        }
    },
    blob: { write: encodeBlob },
    varint: { read: toNumber() },
    timeuuid: { read: toString() },
    uuid: { read: toString() },
    long: {
        read: toString(),
        write(val) { return Long.fromString(val); }
    }
};

/*
 * Derive additional schema info from the public schema
 */
dbu.makeSchemaInfo = function makeSchemaInfo(schema, isMetaCF) {
    // Private schema information
    // Start with a deep clone of the schema
    const psi = extend(true, {}, schema);
    // Then add some private properties
    psi.versioned = false;

    // Extract attributes that need conversion in the read or write path
    psi.conversions = {};
    Object.keys(psi.attributes).forEach((att) => {
        let type = psi.attributes[att];
        const setType = /^set<(\w+)>$/.exec(type);
        if (setType) {
            // this is a set-typed attribute
            type = setType[1];
            // generate the convertors only if the underlying type has them defined
            psi.conversions[att] = generateSetConvertor(dbu.conversions[type]);
        } else if (dbu.conversions[type]) {
            // this is regular type and conversion methods are defined for it
            psi.conversions[att] = dbu.conversions[type];
        }
    });

    if (!isMetaCF) {
        // Prefix a _domain attribute to each hash key, so that we can share CFs
        // between groups of domains
        psi.attributes._domain = 'string';
        psi.index.unshift({ attribute: '_domain', type: 'hash' });
    }

    // Create summary data on the primary data index
    psi.iKeys = dbu.indexKeys(psi.index);
    psi.iKeyMap = {};
    psi.staticKeyMap = {};
    psi.index.forEach((elem) => {
        if (elem.type === 'static') {
            psi.staticKeyMap[elem.attribute] = elem;
        } else {
            psi.iKeyMap[elem.attribute] = elem;
        }
    });

    if (!psi._backend_version) {
        psi._backend_version = dbu.DEFAULT_BACKEND_VERSION;
    }

    if (!psi._config_version) {
        psi._config_version = dbu.DEFAULT_CONFIG_VERSION;
    }

    // define a 'hash' string representation for the schema for quick schema
    // comparisons.
    psi.hash = stableStringify(psi);

    return psi;
};


/**
 * Converts an array of result rows from Cassandra to JS values
 * @param {Array} rows the result rows to convert; not modified
 * @param {Object} schema the schema info to use for conversion
 * @return {Array} a converted array of result rows
 */
dbu.convertRows = function convertRows(rows, schema) {
    const conversions = schema.conversions;
    const newRows = new Array(rows.length);
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const newRow = {};
        Object.keys(row).forEach((att) => {
            // Skip over internal attributes
            if (!/^_/.test(att)) {
                if (row[att] !== null && conversions[att]
                        && conversions[att].read) {
                    newRow[att] = schema.conversions[att].read(row[att]);
                } else {
                    newRow[att] = row[att];
                }
            } else if (att === '_ttl') {
                newRow._ttl = row._ttl;
            }
        });
        newRows[i] = newRow;
    }
    return newRows;
};

/**
 * Deep-clones and converts an internal request's query attributes to native
 * Cassandra representations
 * @param {InternalRequest} internalReq the request whose attributes to convert
 * @param {Object} extendFields any other fields to use when extending the request object; optional
 * @return {InternalRequest} the clone of the request passed in, with converted values
 */
dbu.makeRawRequest = (internalReq, extendFields) => {
    const conversions = (internalReq.schema || {}).conversions;
    extendFields = extendFields || {};
    extendFields.query = extend(true, {}, internalReq.query);
    const clonedReq = internalReq.extend(extendFields);
    const attrs = clonedReq.query.attributes;
    if (!conversions || !attrs) {
        return clonedReq;
    }
    Object.keys(attrs).forEach((key) => {
        const conv = conversions[key];
        if (conv && conv.write) {
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
 * @param {Object} predicates the 'attributes' object in queries.
 * @param {Object} schema the schema info for the logical table.
 * @param {boolean} [noConvert] if true, no attribute value conversion will take place
 * @return {Object} queryInfo object with cql and params attributes
 */
dbu.buildCondition = function buildCondition(predicates, schema, noConvert) {
    function convert(key, val) {
        const convObj = schema.conversions[key];
        if (!noConvert && convObj && convObj.write) {
            return convObj.write(val);
        } else {
            return val;
        }
    }

    // make sure we have got a predicate object
    if (!predicates || predicates.constructor !== Object) {
        throw new Error('The condition predicate has not been supplied or is not an Object.');
    }

    const params = [];
    const conjunctions = [];
    Object.keys(predicates).forEach((predKey) => {
        const predObj = predicates[predKey];
        if (predObj === undefined) {
            throw new Error(`Query error: attribute ${JSON.stringify(predKey)} is undefined`);
        } else if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            conjunctions.push(`${dbu.cassID(predKey)} = ?`);
            params.push(convert(predKey, predObj));
        } else {
            Object.keys(predObj).forEach((predOp) => {
                const predArg = predObj[predOp];
                let cql = dbu.cassID(predKey);
                /* eslint-disable indent */
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
                        cql += ' >= ? AND ';
                        params.push(convert(predKey, predArg[0]));
                        cql += `${dbu.cassID(predKey)} <= ?`;
                        params.push(convert(predKey, predArg[1]));
                        break;
                    default:
                        throw new Error(`Operator ${predOp} not supported!`);
                }
                /* eslint-enable indent */
                conjunctions.push(cql);
            });
        }
    });
    return {
        cql: conjunctions.join(' AND '),
        params,
    };
};


/**
 * CQL building for PUT queries
 * @param {InternalRequest} req
 * @param {boolean} noConvert if true, no attribute value conversion will take place
 * @return {Object} queryInfo object with cql and params attributes
 */
dbu.buildPutQuery = (req, noConvert) => {
    if (!req.schema) {
        throw new Error('Table not found!');
    }
    const schema = req.schema;
    const query = req.query;

    // Convert the attributes
    const attributes = query.attributes || {};
    if (req.columnfamily !== 'meta') {
        attributes._domain = req.domain;
    }
    const conversions = schema.conversions || {};

    // XXX: should we require non-null secondary index entries too?
    const indexKVMap = {};
    schema.iKeys.forEach((key) => {
        if (attributes[key] === undefined) {
            throw new Error(`Index attribute ${JSON.stringify(key)} missing `
                + `in ${JSON.stringify(query)}; schema: ${JSON.stringify(schema, null, 2)}`);
        } else {
            indexKVMap[key] = attributes[key];
        }
    });

    const nonIndexKeys = [];
    let params = [];
    const placeholders = [];
    let haveNonIndexNonNullValue = false;
    Object.keys(attributes).forEach((key) => {
        let val = attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (!schema.iKeyMap[key]) {
                nonIndexKeys.push(key);
                // Convert the parameter value
                const conversionObj = conversions[key];
                if (!noConvert && conversionObj && conversionObj.write) {
                    val = conversionObj.write(val);
                }
                if (val !== null && schema.staticKeyMap && !schema.staticKeyMap[key]) {
                    haveNonIndexNonNullValue = true;
                }
                params.push(val);
            }
            placeholders.push('?');
        } else if (!/^_ttl.*/.test(key) && !schema.attributes[key]) {
            // Allow TTL fields not in the schema
            throw new Error(`Unknown attribute ${key}`);
        }
    });


    let using = '';
    const usingBits = [];
    const usingParams = [];
    if (query.timestamp && !query.if) {
        usingBits.push('TIMESTAMP ?');
        usingParams.push(cass.types.Long.fromNumber(Math.round(query.timestamp * 1000)));
    }
    if (req.ttl) {
        usingBits.push('TTL ?');
        usingParams.push(cass.types.Long.fromNumber(req.ttl));
    }

    if (usingBits.length) {
        using = ` USING ${usingBits.join(' AND ')}`;
    }

    // switch between insert & update / upsert
    // - insert for 'if not exists', or when no non-primary-key attributes are
    //   specified, or they are all null (as Cassandra does not distinguish the two)
    // - update when any non-primary key attributes are supplied
    //   - Need to verify that all primary key members are supplied as well,
    //     else error.

    let cql = '';
    let condResult;

    if (query.if && query.if.constructor === String) {
        query.if = query.if.trim().split(/\s+/).join(' ').toLowerCase();
        if (query.if !== 'not exists') {
            throw new Error("Only 'not exists' conditionals are supported.");
        }
    }

    const condRes = dbu.buildCondition(indexKVMap, schema, noConvert);

    let cond = '';
    if (!haveNonIndexNonNullValue || query.if === 'not exists') {
        if (query.if === 'not exists') {
            cond = ' if not exists ';
        }
        const proj = schema.iKeys.concat(nonIndexKeys).map(dbu.cassID).join(',');
        cql = `insert into ${dbu.cassID(req.keyspace)}.${dbu.cassID(req.columnfamily)}`
            + ` (${proj}) values (`;
        cql += `${placeholders.join(',')})${cond}${using}`;
        params = condRes.params.concat(params, usingParams);
    } else if (nonIndexKeys.length) {
        let condParams = [];
        if (query.if) {
            cond = ' if ';
            condResult = dbu.buildCondition(query.if, schema, noConvert);
            cond += condResult.cql;
            condParams = condResult.params;
        }

        const updateProj = `${nonIndexKeys.map(dbu.cassID).join(' = ?,')} = ? `;
        cql += `update ${dbu.cassID(req.keyspace)}.${dbu.cassID(req.columnfamily)}`
            + `${using} set ${updateProj} where `;
        cql += condRes.cql + cond;
        params = usingParams.concat(params, condRes.params, condParams);

    } else {
        throw new Error("Can't Update or Insert");
    }

    return {
        cql,
        params,
    };
};


/**
 * CQL building for GET queries
 * @param {InternalRequest} req
 * @param  {Object} options map
 * @return {Object} queryInfo object with cql and params attributes
 */
dbu.buildGetQuery = (req, options) => {
    options = options || {};
    const schema = req.schema;
    const query = req.query;
    if (!query) {
        throw new Error('Query missing!');
    }

    if (query.index) {
        throw new Error('No support for secondary indices!');
    }

    let projCQL = Object.keys(schema.attributes).map(dbu.cassID).join(',');
    let projAttrs = Object.keys(schema.attributes);

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
    if (options.withTTL) {
        // Candidates for TTL are non-index, non-collection, attributes
        const ttlCandidates = projAttrs.filter(
            v => !schema.iKeyMap[v] && !/^(set|map|list)<.*>$/.test(schema.attributes[v])
        );
        const projTTLs = ttlCandidates.map(
            v => `TTL(${dbu.cassID(v)}) as ${dbu.cassID(dbu.cassTTL(v))}`
        );
        projCQL += `,${projTTLs.join(',')}`;
    }

    if (query.distinct) {
        projCQL = `distinct ${projCQL}`;
    }

    let cql = `select ${projCQL} from ${dbu.cassID(req.keyspace)}.${dbu.cassID(req.columnfamily)}`;

    // Build up the condition
    let params = [];
    const attributes = query.attributes || {};
    if (req.columnfamily !== 'meta') {
        attributes._domain = req.domain;
    }
    Object.keys(attributes).forEach((key) => {
        // query should not have non key attributes
        if (!schema.iKeyMap[key]) {
            throw new Error(`All request attributes need to be key attributes. `
                + `Bad attribute: ${key}`);
        }
    });
    cql += ' where ';
    const condResult = dbu.buildCondition(attributes, schema);
    cql += condResult.cql;
    params = condResult.params;

    if (query.order) {
        let reversed;
        // Establish whether we need to read in forward or reverse order,
        // which is what Cassandra supports. Also validate the order for
        // consistency.
        Object.keys(query.order).forEach((att) => {
            const dir = query.order[att];
            if (dir !== 'asc' && dir !== 'desc') {
                throw new Error(`Invalid sort order ${dir} on key ${att}`);
            }
            const idxElem = schema.iKeyMap[att];
            if (!idxElem || idxElem.type !== 'range') {
                throw new Error(`Cannot order on attribute ${att}; `
                    + `needs to be a range index, but is ${idxElem}`);
            }
            const shouldBeReversed = dir !== idxElem.order;
            if (reversed === undefined) {
                reversed = shouldBeReversed;
            } else if (reversed !== shouldBeReversed) {
                throw new Error("Inconsistent sort order; Cassandra only supports "
                        + "reversing the default sort order.");
            }
        });

        // Finally, build up the order query
        const toDir = {
            asc: reversed ? 'desc' : 'asc',
            desc: reversed ? 'asc' : 'desc'
        };
        const orderTerms = [];
        schema.index.forEach((elem) => {
            if (elem.type === 'range') {
                const dir = toDir[elem.order];
                orderTerms.push(`${dbu.cassID(elem.attribute)} ${dir}`);
            }
        });

        if (orderTerms.length) {
            cql += ` order by ${orderTerms.join(',')}`;
        }
    }

    /**
     * Limit handling
     *
     * Most queries benefit from having a pageState returned if there are more
     * results, which is why we are interpreting `query.limit` as `fetchSize`
     * in `db._getRaw()`, and **IGNORE query.limit HERE**.
     *
     * That said, for the cases where an actual limit is needed we do support
     * it here by passing it in *options*, rather than the query.
     */
    if (options.limit) {
        const limit = parseInt(options.limit, 10);
        cql += limit ? ` limit ${limit}` : '';
    }

    return { cql, params };
};

/**
 * CQL building for DELETE queries
 * @param {InternalRequest} req
 * @return {Object} queryInfo object with cql and params attributes
 */
dbu.buildDeleteQuery = (req) => {
    if (req.columnFamily === 'meta') {
        throw new Error("Deleting from 'meta' is not supported!");
    }
    const schema = req.schema;
    const query = req.query;
    const attributes = query.attributes || {};
    attributes._domain = req.domain;
    const keyspace = dbu.cassID(req.keyspace);
    const columnfamily = dbu.cassID(req.columnfamily);
    const condition = dbu.buildCondition(attributes, schema);
    const cql = `DELETE FROM ${keyspace}.${columnfamily} WHERE ${condition.cql}`;
    return { cql, params: condition.params };
};

dbu.getOptionCQL = (options) => {
    if (options.default_time_to_live) {
        return `default_time_to_live = ${options.default_time_to_live}`;
    }
    return '';
};

module.exports = dbu;
