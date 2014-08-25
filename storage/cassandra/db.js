"use strict";

var crypto = require('crypto');
var cass = require('node-cassandra-cql');
var defaultConsistency = cass.types.consistencies.one;

function cassID (name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
}

function buildCondition (pred) {
    var params = [];
    var conjunctions = [];
    for (var predKey in pred) {
        var cql = '';
        var predObj = pred[predKey];
        cql += cassID(predKey);
        if (predObj.constructor === String) {
            // Default to equality
            cql += ' = ?';
            params.push(predObj);
        } else if (predObj.constructor === Object) {
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
        cql: conjunctions.join(' AND '),
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
 * chars as far as possible, but fall back to a sha1 if not possible. Also
 * respect Cassandra's limit of 48 or fewer alphanum chars & first char being
 * an alpha char.
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


function DB (client) {
    // cassandra client
    this.client = client;

    // cache keyspace -> schema
    this.schemaCache = {};
}

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
            return JSON.parse(res.items[0].value);
        } else {
            return null;
        }
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

var getCache = {};

DB.prototype._get = function (keyspace, req, consistency, table) {
    if (!table) {
        table = 'data';
    }
    var proj = '*';
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
        var cachedSchema = this.schemaCache[keyspace];
        if (cachedSchema) {
            proj = Object.keys(cachedSchema.attributes).map(cassID).join(',');
        }
    }
    if (req.index) {
        table = 'i_' + req.index;
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
        cql += condResult.cql;
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
            rangeColumn = 'tid';
        }
        var dir = req.order.toLowerCase();
        if (rangeColumn && dir in {'asc':1, 'desc':1}) {
            cql += ' order by ' + cassID(rangeColumn) + ' ' + dir;
        }
    }

    if (req.limit && req.limit.constructor === Number) {
        cql += ' limit ' + req.limit;
    }

    //console.log(cql, params);
    return this.client.executeAsPrepared_p(cql, params, consistency)
    .then(function(result) {
        //console.log(result);
        var rows = result.rows;
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

DB.prototype.put = function (reverseDomain, req) {
    var keyspace = keyspaceName(reverseDomain, req.table);


    // consistency
    var consistency = defaultConsistency;
    if (req.consistency && req.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[req.consistency];
    }
    return this._put(keyspace, req, consistency);
};


DB.prototype._put = function(keyspace, req, consistency, table) {
    // Get the type info for the table & verify types & ops per index
    // var schema = this.getSchema(keyspace, req.table);
    if (!table) {
        table = 'data';
    }

    var keys = [];
    var params = [];
    var placeholders = [];
    for (var key in req.attributes) {
        var val = req.attributes[key];
        if (val !== undefined) {
            if (val.constructor === Object) {
                val = JSON.stringify(val);
            }
            keys.push(key);
            params.push(val);
            placeholders.push('?');
        }
    }
    var proj = keys.map(cassID).join(',');
    // XXX: switch between insert & update / upsert?
    // - insert for 'if not exists', or when no non-primary-key attributes are
    //   specified
    // - update when any non-primary key attributes are supplied
    //  - Need to verify that all primary key members are supplied as well,
    //    else error.
    var cql = 'insert into ' + cassID(keyspace) + '.' + cassID(table)
            + ' (' + proj + ') values (';
    cql += placeholders.join(',') + ')';

    // Build up the condition
    if (req.if) {
        cql += ' if ';
        var condResult = buildCondition(req.if);
        cql += condResult.cql;
        params = params.concat(condResult.params);
    }

    // TODO: update indexes

    //console.log('cql', cql, 'params', JSON.stringify(params));
    return this.client.executeAsPrepared_p(cql, params, consistency)
    .then(function(result) {
        var rows = result.rows;
        return {
            // XXX: check if condition failed!
            status: 401
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
        cql += condResult.cql;
        params = condResult.params;
    }

    // TODO: delete from indexes too!

    return this.client.executeAsPrepared_p(cql, params, consistency);
};

DB.prototype._createKeyspace = function (keyspace, consistency) {
    var cql = 'create keyspace ' + cassID(keyspace)
        + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}";
    return this.client.execute_p(cql, [], consistency || defaultConsistency);
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

    // Info table schema
    var infoSchema = {
        name: 'meta',
        attributes: {
            key: 'string',
            value: 'json'
        },
        index: {
            hash: 'key'
        }
    };

    return this._createKeyspace(keyspace, consistency)
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

    if (!req.index || !req.index.hash) {
        //console.log(req);
        throw new Error("Missing index or hash key in table schema");
    }

    cql += 'primary key (';
    var rangeIndex = '';
    if (req.index.range) {
        if (Array.isArray(req.index.range)) {
            rangeIndex = req.index.range.map(cassID).join(',');
        } else {
            rangeIndex = cassID(req.index.range);
        }
        rangeIndex = ', ' + rangeIndex;
    }
    cql += cassID(req.index.hash) + rangeIndex;
    cql += '))';

    if (req.order && req.order.toLowerCase() in {'asc':1, 'desc':1} && req.index.range) {
        var firstRange = Array.isArray(req.index.range) ? req.index.range[0] : req.index.range;
        cql += ' with clustering order by ( ' + cassID(firstRange) + ' ' + req.order.toLowerCase() + ')';
    }

    // XXX: Handle secondary indexes
    var tasks = [];
    if (req.secondaryIndexes) {
        for (var indexName in req.secondaryIndexes) {
            var index = req.secondaryIndexes[indexName];

            // Make sure we have an array for the range part of the index
            if (index.range) {
                if (!Array.isArray(index.range)) {
                    index.range = [index.range];
                }
            } else {
                index.range = [];
            }

            // Build up attributes
            var attributes = {};
            // copy over type info
            attributes[index.hash] = req.attributes[index.hash];

            // Make sure the main index keys are included in the new index
            // First, the hash key.
            if (!attributes[req.index.hash] && index.range.indexOf(req.index.hash) === -1) {
                // Add in the original hash key as an additional range key
                index.range.push(req.index.hash);
            }
            // Now the range key(s).
            var origRange = req.index.range;
            if (origRange) {
                if (!Array.isArray(origRange)) {
                    origRange = [origRange];
                }
            } else {
                origRange = [];
            }
            origRange.forEach(function(att) {
                if (!attributes[att] && index.range.indexOf(att) === -1) {
                    // Add in the original hash key(s) as additional range
                    // key(s)
                    index.range.push(att);
                }
            });

            // Now make sure that all range keys are also included in the
            // attributes.
            index.range.forEach(function(att) {
                if (!attributes[att]) {
                    attributes[att] = req.attributes[att];
                }
            });

            // Finally, deal with projections
            if (index.proj && Array.isArray(index.proj)) {
                index.proj.forEach(function(attr) {
                    if (!attributes[attr]) {
                        attributes[attr] = req.attributes[attr];
                    }
                });
            }

            var indexSchema = {
                name: indexName,
                attributes: attributes,
                index: index,
                consistency: defaultConsistency
            };

            tasks.push(this._createTable(keyspace, indexSchema, 'i_' + indexName));
        }
        tasks.push(this.client.execute_p(cql, [], consistency));
        return Promise.all(tasks);
    } else {
        return this.client.execute_p(cql, [], consistency);
    }
};

DB.prototype.dropTable = function (reverseDomain, table) {
    var keyspace = keyspaceName(reverseDomain, table);
    return this.client.execute_p('drop keyspace ' + cassID(keyspace), [], defaultConsistency);
};


module.exports = DB;
