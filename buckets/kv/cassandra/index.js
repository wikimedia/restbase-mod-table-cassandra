"use strict";
/**
 * Simple Cassandra-based revisioned storage implementation
 *
 * Implements (currently a subset of) the functionality documented in
 * https://www.mediawiki.org/wiki/User:GWicke/Notes/Storage#Strawman_backend_API
 *
 * Interface is intended to be general, so that other backends can be dropped
 * in. We don't want inheritance though, just implementing the interface is
 * enough.
 */

var util = require('util');
var cass = require('node-cassandra-cql');
var consistencies = cass.types.consistencies;
var uuid = require('node-uuid');
var util = require('util');
var crypto = require('crypto');
var fs = require('fs');

function CassandraRevisionStore (client) {
    var self = this;

    // convert consistencies from string to the numeric constants
    this.consistencies = {
        read: consistencies.one,
        write: consistencies.one
    };

    self.client = client;
}

var CRSP = CassandraRevisionStore.prototype;

function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
}

CRSP.getBucketInfo = function (env, req) {
    var keyspaceName = this.client.keyspaceName(req.params.prefix, req.params.bucket);
    var cql = util.format("select value from %s.info where key = 'meta' limit 1", keyspaceName);
    return this.client.executeAsPrepared_p(cql, [], this.consistencies.read)
    .then(function(result) {
        return JSON.parse(result[0].rows[0].value);
    });
};

var tableCQLTemplate = fs.readFileSync(__dirname + '/tables.cql').toString();
var updateInfoCQLTemplate = "INSERT INTO %s.info (key, value) values ('meta',?)";
function infoTableCQL (keyspace) {
    return util.format('CREATE TABLE IF NOT EXISTS %s.info ('
                + 'key text,'
                + 'value text,'
                + 'PRIMARY KEY(key)'
                + ');', keyspace);
}

function cassID (name) {
    return '"' + name.replace(/"/g, '""') + '"';
}

/*
 * {
    "AttributeDefinitions": [
        {
            "AttributeName": "ForumName",
            "AttributeType": "S"
        },
        {
            "AttributeName": "Subject",
            "AttributeType": "S"
        },
        {
            "AttributeName": "LastPostDateTime",
            "AttributeType": "S"
        }
    ],
    }*/
CRSP.buildTableCQL = function (keyspaceName, tableName, schema) {
    var defs = schema.attributes;
    if (!defs) {
        throw new Error('No AttributeDefinitions for table!');
    }
    var cql = 'create table if not exists ' + cassID(keyspaceName + '.' + tableName) + ' (';
    defs.forEach(function(def) {
        cql += cassID(def.name) + ' ';
        switch (def.type) {
        case 'blob': cql += 'blob'; break;
        case 'set<blob>': cql += 'set<blob>'; break;
        case 'number': cql += 'decimal'; break;
        case 'set<number>': cql += 'set<decimal>'; break;
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
        default: throw new Error('Invalid type ' + def.type
                     + ' for attribute ' + def.name);
        }
        cql += ', ';
    });

    var keySchema = schema.keys;
    var hashKey, rangeKey;
    keySchema.forEach(function(ks) {
        if (ks.type.toLowerCase() === 'hash') {
            hashKey = ks.attributeName;
        } else if (ks.type.toLowerCase() === 'range') {
            rangeKey = ks.attributeName;
        }
    });

    if (!hashKey) {
        throw new Error("Missing hash key in table schema");
    }

    cql += 'primary key (';
    cql += cassID(hashKey) + (rangeKey ? ', ' + cassID(rangeKey) : '');
    cql += ');';
    return cql;
};


CRSP.revBucketSchema = {
    attributes: {
        uri: 'string',
        tid: 'timeuuid',
        body: 'blob',
        'content-type': 'string',
        'content-length': 'varint',
        'content-sha256': 'string',
        // redirect
        'content-location': 'string',
        // 'deleted', 'nomove' etc?
        restrictions: 'set<string>'
    },
    index: {
        hash: 'uri',
        range: 'tid'
    }
};

// Create a new bucket
CRSP.createBucket = function(env, req) {
    var self = this;
    var options = req.body.options;
    var params = req.params;
    var keyspaceName = this.client.keyspaceName(params.prefix, params.bucket);
    // TODO: support non-standard buckets?
    var tableCQL = this.buildTableCQL(keyspaceName, 'data', this.revBucketSchema);
    var updateInfoCQL = util.format(updateInfoCQLTemplate, keyspaceName);
    var bucketInfo = {
        type: 'kv_rev',
        version: 1,
        options: options,
        domain: params.domain,
        prefix: params.prefix,
        name: params.bucket
    };
    return this.client.createKeyspace_p(keyspaceName)
    .then(function() {
        return self.client.executeAsPrepared_p(tableCQL,
            [], self.consistencies.write);
    })
    .then(function() {
        return self.client.executeAsPrepared_p(infoTableCQL(keyspaceName),
            [], self.consistencies.write);
    })
    .then(function() {
        return self.client.executeAsPrepared_p(updateInfoCQL,
                [JSON.stringify(bucketInfo)],
                self.consistencies.write);
    });
};

// Create a new bucket
CRSP.listBucket = function(env, req) {
    var self = this;
    var params = req.params;
    var keyspaceName = this.client.keyspaceName(params.prefix, params.bucket);
    var listCQL = util.format('select distinct key from %s.kv_rev', keyspaceName);
    return this.client.executeAsPrepared_p(listCQL, [], this.consistencies.read)
    .then(function(result) {
        return result[0].rows;
    });
};

// Get the latest revision of an object
CRSP.getLatest = function(env, req) {
    var keyspaceName = this.client.keyspaceName(req.params.prefix, req.params.bucket);
    var cql = util.format('select tid, headers, value from %s.revisions where key = ? limit 1;',
            keyspaceName);
    return this.client.executeAsPrepared_p(cql, [req.params.key], this.consistencies.read)
    .then(function(result) {
        console.log(result);
        return result.rows[0];
    });
};

// Add a new revision of an object
CRSP.putLatest = function(env, req) {
    var keyspaceName = this.client.keyspaceName(req.params.prefix, req.params.bucket);
    var cql = util.format('insert into %s.kv_rev (key, tid, headers, value) values (?,?,?,?)',
            keyspaceName);
    var tid = uuid.v1();
    if (req.headers['last-modified']) {
        try {
            // XXX: require elevated rights for passing in the revision time
            tid = tidFromDate(new Date(req.headers['last-modified']));
        } catch (e) { }
    }
    var params = [
        req.params.key, tid,
        {value: req.headers, hint: 'map'},
        new Buffer(JSON.stringify(req.body))
    ];
    return this.client.executeAsPrepared_p(cql, params, this.consistencies.read)
    .then(function(result) {
        return {tid: tid};
    });
};

/**
 * Add a new revision with several properties
 */
CRSP.addRevision = function (revision) {
    var tid;
    if(revision.timestamp) {
        // Create a new, deterministic timestamp
        // XXX: pass in a date directly
        tid = tidFromDate(new Date(revision.timestamp));
    } else {
        tid = uuid.v1();
    }
    // Build up the CQL
    // Simple revison table insertion only for now
    var cql = 'BEGIN BATCH ',
        args = [],
        props = Object.keys(revision.props);
    // Insert the _rev metadata
    cql += 'insert into revisions (name, prop, tid, revtid, value) ' +
            'values(?, ?, ?, ?, ?);\n';
    args = args.concat([
            revision.page.title,
            '_rev',
            tid,
            tid,
            new Buffer(JSON.stringify({rev:revision.id}))]);

    // Insert the revid -> timeuuid index
    cql += 'insert into idx_revisions_by_revid (revid, name, tid) ' +
            'values(?, ?, ?);\n';
    args = args.concat([
            revision.id,
            revision.page.title,
            tid]);

    // Now insert each revision property
    props.forEach(function(prop) {
        cql += 'insert into revisions (name, prop, tid, revtid, value) ' +
            'values(?, ?, ?, ?, ?);\n';
        args = args.concat([
            revision.page.title,
            prop,
            tid,
            tid,
            revision.props[prop].value]);
    });

    // And finish it off
    cql += 'APPLY BATCH;';

    return this.client.executeAsPrepared_p(cql, args, this.consistencies.write)
    .then(function() {
        return {tid: tid};
    });
};

/**
 * Get the latest version of a given property of a page
 *
 * Takes advantage of the latest-first clustering order.
 */
CRSP.getRevision = function (name, rev, prop) {
    var resolve, reject;
    var pr = new Promise(function(res, rej) {
        resolve = res;
        reject = rej;
    });
    var queryCB = function (err, results) {
            if (err) {
                reject(err);
            } else if (!results || !results.rows || results.rows.length === 0) {
                resolve([]);
            } else {
                resolve(results.rows);
            }
        },
        consistencies = this.consistencies,
        client = this.client,
        cql = '',
        args = [], tid;

    if (rev === 'latest') {
        // Build the CQL
        cql = 'select value from revisions where name = ? and prop = ? limit 1;';
        args = [name, prop];
        client.executeAsPrepared(cql, args, consistencies.read, queryCB);
    } else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(rev)) {
        // By UUID
        cql = 'select value from revisions where name = ? and prop = ? and tid = ? limit 1;';
        args = [name, prop, rev];
        return client.executeAsPrepared(cql, args, consistencies.read, queryCB);
    } else {
        switch(rev.constructor) {
            case Number:
                // By MediaWiki oldid

                // First look up the timeuuid from the revid
                cql = 'select tid from idx_revisions_by_revid where revid = ? limit 1;';
                args = [rev];
                client.executeAsPrepared(cql, args, consistencies.read, function (err, results) {
                            if (err) {
                                reject(err);
                            }
                            if (!results.rows.length) {
                                resolve('Revision not found'); // XXX: proper error
                            } else {
                                // Now retrieve the revision using the tid
                                tid = results.rows[0][0];
                                cql = 'select value from revisions where ' +
                                    'name = ? and prop = ? and tid = ? limit 1;';
                                args = [name, prop, tid];
                                client.executeAsPrepared(cql, args, consistencies.read, queryCB);
                            }
                        });
                break;
            case Date:
                // By date
                tid = tidFromDate(rev);
                cql = 'select value from revisions where name = ? and prop = ? and tid <= ? limit 1;';
                args = [name, prop, tid];
                client.executeAsPrepared(cql, args, consistencies.read, queryCB);
                break;
        }
    }
    return pr;
};

module.exports = CassandraRevisionStore;
