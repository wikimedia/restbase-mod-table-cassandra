"use strict";

/**
 * Revisioned blob handler
 */

var RevisionBackend = require('./cassandra');
var RouteSwitch = require('routeswitch');
var util = require('util');

var backend;
var config;

function KVRevBucket (backend, log) {
    // XXX: create store based on backend.type
    // console.log('backend.type', backend.type);
    this.store = new RevisionBackend(backend.client);
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
        //{
        //    pattern: '/',
        //    methods: {
        //        GET: this.listBucket.bind(this),
        //    }
        //},
        {
            pattern: '/{key}',
            methods: {
                GET: this.getLatest.bind(this),
                PUT: this.putLatest.bind(this)
            }
        }
    ]);
}
KVRevBucket.prototype.getBucketInfo = function(env, req) {
    var self = this;
    return this.store.getBucketInfo (env, req)
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
        }
    });
};

KVRevBucket.prototype.createBucket = function(env, req) {
    if (!req.body
            || req.body.constructor !== Object
            || req.body.type !== 'kv_rev')
    {
        // XXX: validate with JSON schema
        var exampleBody = {
            type: 'kv_rev',
            options: {
                keyType: 'text',
                valueType: 'blob'
            }
        };

        return Promise.resolve({
            status: 400,
            body: {
                message: "Expected JSON body describing the bucket.",
                example: exampleBody
            }
        });
    }
    if (!req.body.options) { req.body.options = {}; }
    var opts = req.body.options;
    if (!opts.keyType) { opts.keyType = 'text'; }
    if (!opts.valueType) { opts.valueType = 'blob'; }
    return this.store.createBucket(env, req);
};

KVRevBucket.prototype.getLatest = function(env, req) {
    // XXX: check params!
    return this.store.getLatest(env, req)
    .then(function(result) {
        var headers = result.headers;
        headers.etag = result.tid.toString();
        return {
            status: 200,
            headers: headers,
            body: result.value
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

KVRevBucket.prototype.putLatest = function(env, req) {
    var self = this;

    return this.store.putLatest(env, req)
    .then(function(result) {
        return {
            status: 201,
            headers: {
                etag: result.tid
            },
            body: {
                message: "Created.",
                tid: result.tid
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



KVRevBucket.prototype.handlePOST = function (env, req) {
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
            return this.store.addRevision(revision)
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


KVRevBucket.prototype.handleGET = function (env, req) {
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
            return this.store.getRevision(page, rev, prop)
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

KVRevBucket.prototype.handleALL = function (env, req) {
    var match = this.router.match(req.uri);
    if (match) {
        var handler = match.route.methods[req.method] || match.route.methods.ALL;
        if (handler) {
            var newReq = util._extend({}, req);
            var params = match.params;
            util._extend(params, req.params);
            newReq.params = params;
            return handler(env, newReq);
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



module.exports = function(options, log) {
    var revBucket = new KVRevBucket(options.backend, log);
    return revBucket.handleALL.bind(revBucket)
};
