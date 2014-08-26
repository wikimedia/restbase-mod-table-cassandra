"use strict";

/**
 * Revisioned blob handler
 */

var RouteSwitch = require('routeswitch');
var util = require('util');
var uuid = require('node-uuid');

var backend;
var config;

function reverseDomain(domain) {
    if (!domain) {
        throw new Error("Domain required!");
    }
    return domain.toLowerCase().split('.').reverse().join('.');
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

function KVBucket (log) {
    this.log = log || function(){};

    this.router = new RouteSwitch([
        //{
        //    pattern: '/{key}/{rev}',
        //    methods: {
        //        GET: this.getRevision.bind(this),
        //        PUT: this.putRevision.bind(this)
        //    }
        //},
        //{
        //    pattern: '/{key}/',
        //    methods: {
        //        GET: this.listRevisions.bind(this)
        //    }
        //},
        {
            pattern: '',
            methods: {
                GET: this.getBucketInfo.bind(this),
                PUT: this.createBucket.bind(this)
            }
        },
        {
            pattern: '/',
            methods: {
                GET: this.listBucket.bind(this),
            }
        },
        {
            pattern: '/{key}',
            methods: {
                GET: this.getLatest.bind(this),
                PUT: this.putLatest.bind(this)
            }
        },
        {
            pattern: '/{key}/',
            methods: {
                GET: this.listRevisions.bind(this),
            }
        },
        {
            pattern: '/{key}/{revision}',
            methods: {
                GET: this.getRevision.bind(this),
            }
        }
    ]);
}
KVBucket.prototype.getBucketInfo = function(req, store) {
    var self = this;
    return store.getSchema(
        reverseDomain(req.params.domain),
        req.params.bucket
    )
    .then(function(res) {
        return {
            status: 200,
            body: res
        };
    })
    .catch(function(err) {
        self.log(err.stack);
        return {
            status: 500,
            body: {
                message: 'Internal error',
                stack: err.stack
            }
        };
    });
};

KVBucket.prototype.makeSchema = function (opts) {
    if (opts.type === 'kv_rev') {
        opts.schemaVersion = 1;
        return {
            bucket: opts,
            attributes: {
                key: opts.keyType || 'string',
                tid: 'timeuuid',
                latestTid: 'timeuuid',
                value: opts.valueType || 'blob',
                'content-type': 'string',
                'content-length': 'varint',
                'content-sha256': 'string',
                // redirect
                'content-location': 'string',
                // 'deleted', 'nomove' etc?
                tags: 'set<string>',
            },
            index: {
                hash: 'key',
                range: 'tid',
                static: 'latestTid'
            },
            order: 'desc'
        };
    } else {
        throw new Error('Bucket type ' + opts.type + ' not yet implemented');
    }
};

var bucketTypes = {
    kv_rev: true,
    kv: true,
    kv_ordered: true,
    kv_ordered_rev: true
};
KVBucket.prototype.createBucket = function(req, store) {
    if (!req.body
            || req.body.constructor !== Object
            || !(req.body.type in bucketTypes) )
    {
        // XXX: validate with JSON schema
        var exampleBody = {
            type: 'kv',
            revisioned: true,
            keyType: 'string',
            valueType: 'blob'
        };

        return Promise.resolve({
            status: 400,
            body: {
                message: "Expected JSON body describing the bucket.",
                example: exampleBody
            }
        });
    }
    var opts = req.body;
    if (!opts.keyType) { opts.keyType = 'string'; }
    if (!opts.valueType) { opts.valueType = 'blob'; }
    var schema = this.makeSchema(opts);
    schema.table = req.params.bucket;
    return store.createTable(reverseDomain(req.params.domain), schema);
};


KVBucket.prototype.getListQuery = function (type, bucket) {
    // TODO: support other bucket types
    if (type !== 'kv_rev') {
        throw new Error('Only kv_rev supported to far');
    }
    return {
        table: bucket,
        distinct: true,
        proj: 'key',
        limit: 10000
    };
};



KVBucket.prototype.listBucket = function(req, store) {
    // XXX: check params!
    var params = req.params;
    if (!params.domain || !params.bucket) {
        return Promise.resolve({
            status: 400,
            body: { message: "Domain / bucket missing" }
        });
    }

    var listQuery = this.getListQuery('kv_rev', params.bucket);
    return store.get(reverseDomain(params.domain), listQuery)
    .then(function(result) {
        var listing = result.items.map(function(row) {
            return row.key;
        });
        return {
            status: 200,
            headers: {
                'content-type': 'application/json'
            },
            body: listing
        };
    })
    .catch(function(error) {
        console.error(error);
        return {
            status: 404,
            body: {
                message: "Not found."
            }
        };
    });
};

// Format a revision response. Shared between different ways to retrieve a
// revision (latest & with explicit revision).
KVBucket.prototype.returnRevision = function(dbResult) {
    if (dbResult.items.length) {
        var row = dbResult.items[0];
        var headers = {
            etag: row.tid,
            'content-type': row['content-type']
        };
        return {
            status: 200,
            headers: headers,
            body: row.value
        };
    } else {
        return {
            status: 404,
            body: {
                message: "Not found."
            }
        };
    }
};

KVBucket.prototype.getLatest = function(req, store) {
    // XXX: check params!
    var query = {
        table: req.params.bucket,
        attributes: {
            key: req.params.key
        },
        limit: 1
    };

    return store.get(reverseDomain(req.params.domain), query)
    .then(this.returnRevision.bind(this))
    .catch(function(error) {
        console.error(error);
        return {
            status: 404,
            body: {
                message: "Not found."
            }
        };
    });
};

KVBucket.prototype.putLatest = function(req, store) {
    var self = this;

    var tid = uuid.v1();
    if (req.headers['last-modified']) {
        try {
            // XXX: require elevated rights for passing in the revision time
            tid = tidFromDate(new Date(req.headers['last-modified']));
        } catch (e) { }
    }

    var query = {
        table: req.params.bucket,
        attributes: {
            key: req.params.key,
            tid: tid,
            value: req.body,
            'content-type': req.headers['content-type']
        }
    };

    return store.put(reverseDomain(req.params.domain), query)
    .then(function(result) {
        return {
            status: 201,
            headers: {
                etag: tid
            },
            body: {
                message: "Created.",
                tid: tid
            }
        };
    })
    .catch(function(err) {
        self.log(err.stack);
        return {
            status: 500,
            body: {
                message: "Unknown error\n" + err.stack
            }
        };
    });
};

KVBucket.prototype.listRevisions = function(req, store) {
    var query = {
        table: req.params.bucket,
        attributes: {
            key: req.params.key
        },
        proj: ['tid']
    };
    var domain = reverseDomain(req.params.domain);
    return store.get(domain, query)
    .then(function(res) {
        return {
            status: 200,
            headers: {
                'content-type': 'application/json'
            },
            body: res.items.map(function(row) {
                        return row.tid;
                  })
        };
    });
};

KVBucket.prototype.getRevision = function(req, store) {
    // TODO: support other formats! See cassandra backend getRevision impl.
    var query = {
        table: req.params.bucket,
        attributes: {
            key: req.params.key,
            tid: req.params.revision
        }
    };
    var domain = reverseDomain(req.params.domain);
    return store.get(domain, query)
    .then(this.returnRevision.bind(this))
    .catch(function(error) {
        console.error(error);
        return {
            status: 404,
            body: {
                message: "Not found."
            }
        };
    });
};



KVBucket.prototype.handlePOST = function (req, store) {
    var self = this;
    var match = this.router.match(req.uri);

    var title = match.params.title;
    if (title !== undefined) {
        // Atomically create a new revision with several properties
        if (req.body._rev && req.body._timestamp) {
            var props = {};
            props[match.params.prop] = {
                value: new Buffer(req.body[match.params.prop])
            };
            //console.log(props);
            var revision = {
                page: {
                    title: title
                },
                id: Number(req.body._rev),
                timestamp: req.body._timestamp,
                props: props
            };
            return store.addRevision(revision)
            .then(function (result) {
                return {
                    status: 200,
                    body: {'message': 'Added revision ' + result.tid, id: result.tid}
                };
            })
            .catch(function(err) {
                // XXX: figure out whether this was a user or system
                // error
                //console.error('Internal error', err.toString(), err.stack);
                self.log(err.stack);
                return {
                    status: 500,
                    body: 'Internal error\n' + err.stack
                };
            });
        } else {
            // We don't support _rev or _timestamp-less revisions yet
            return Promise.resolve({
                status: 400,
                body: '_rev or _timestamp are missing!'
            });
        }
    } else {
        console.log(title, req.params, match.params);
        return Promise.resolve({
            status: 404,
            body: 'Not found'
        });
    }
};


KVBucket.prototype.handleGET = function (req, store) {
    var self = this;
    var match = this.router.match(req.uri);

    var page = match.params.title;
    if (page && match.params.rev) {
        var revString = match.params.rev,
            // sanitized / parsed rev
            rev = null,
            // 'wikitext', 'html' etc
            prop = match.params.prop; //queryComponents[2] || null;

        if (revString === 'latest') {
            // latest revision
            rev = revString;
        } else if (/^\d+$/.test(revString)) {
            // oldid
            rev = Number(revString);
        } else if (/^\d{4}-\d{2}-\d{2}/.test(revString)) {
            // timestamp
            rev = new Date(revString);
            if (isNaN(rev.valueOf())) {
                // invalid date
                return Promise.resolve({
                    status: 400,
                    body: 'Invalid date'
                });
            }
        } else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(revString)) {
            // uuid
            rev = revString;
        }

        if (page && prop && rev) {
            //console.log(page, prop, rev);
            return store.getRevision(page, rev, prop)
            .then(function (results) {
                if (!results.length) {
                    return {
                        status: 404,
                        body: 'Not found'
                    };
                }
                return {
                    status: 200,
                    headers: {'content-type': 'text/plain'},
                    body: results[0].value
                };
            })
            .catch(function(err) {
                    //console.error('ERROR', err.toString(), err.stack);
                    self.log(err.stack);
                    return {
                        status: 500,
                        body: 'Internal error\n' + err.stack
                    };
            });
        }
    }

    return Promise.resolve({
        status: 404,
        body: 'Not found'
    });
};

KVBucket.prototype.handleALL = function (req, store) {
    var match = this.router.match(req.uri);
    if (match) {
        var handler = match.route.methods[req.method] || match.route.methods.ALL;
        if (handler) {
            var newReq = util._extend({}, req);
            var params = match.params;
            util._extend(params, req.params);
            newReq.params = params;
            return handler(newReq, store);
        }
    }
    // Fall through: Error case
    return Promise.resolve({
        status: 404,
        body: {
            message: 'Not found!',
            hint: 'In kv_rev bucket',
            uri: req.uri
        }
    });
};



module.exports = function(log) {
    var revBucket = new KVBucket(log);
    return revBucket.handleALL.bind(revBucket);
};
