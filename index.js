"use strict";
/*
 * Rashomon HTTP backend handler.
 */

// global includes
var prfun = require('prfun');
var fs = require('fs');
var util = require('util');

// TODO: retrieve dynamically from storage!
var fakeRegistry = {
    "en.wikipedia.org": {
        prefix: 'enwiki',
        buckets: {
            "foo": {
                "type": "kv_rev",
                // XXX: is this actually needed?
                "backend": "cassandra",
                "backendID": "store/default",
                "acl": {
                    read: [
                        // A publicly readable bucket
                        [
                            // Any path in bucket (default: anything - so can be omitted)
                            {
                                type: "pathRegExp",
                                re: '*'
                            },
                            {
                                type: "role",
                                anyOf: [ '*', 'user', 'admin' ]
                            }
                        ]
                    ],
                    write: [
                        // Require both the user group & the service signature for writes
                        [
                            {
                                type: "role",
                                anyOf: [ 'user', 'admin' ]
                            },
                            {
                                type: "serviceSignature",
                                // Can require several service signatures here, for example to
                                // ensure that a request was sanitized by all of them.
                                allOf: [ 'b7821dbca23b6f36db2bdcc3ba10075521999e6b' ]
                            }
                        ]
                    ]
                }
            }
        }
    }
};

//            {
//                path: '/v1/{domain}/{bucket}/{+path}',
//                methods: {
//                    GET: {
//                        handler: this.handleAll.bind(this),
//                        doc: { /* swagger docs */
//                            "summary": "Retrieves the property of a specific revision through Rashomon",
//                            "notes": "See <link> for information on properties and content types.",
//                            "type": "html",
//                            "produces": ["text/html;spec=mediawiki.org/specs/html/1.0"],
//                            "responseMessages": [
//                                {
//                                    "code": 404,
//                                    "message": "No HTML for page & revision found"
//                                }
//                            ]
//                        }
//                    },
//                    PUT: {
//                        handler: this.handleAll.bind(this),
//                        doc: { /* swagger docs */
//                            "summary": "Adds a new version of an object",
//                            "notes": "See <link> for information on properties and content types.",
//                            "type": "html",
//                            "produces": ["text/html;spec=mediawiki.org/specs/html/1.0"],
//                            "responseMessages": [
//                                {
//                                    "code": 404,
//                                    "message": "No HTML for page & revision found"
//                                }
//                            ]
//                        }
//                    },
//                    POST: {
//                        handler: this.handleAll.bind(this),
//                        doc: { /* swagger docs */
//                            "summary": "Saves a new revision to Rashomon",
//                            "notes": "Some notes.",
//                            "type": "html",
//                            "produces": ["text/html;spec=mediawiki.org/specs/html/1.0"],
//                            "responseMessages": [
//                                {
//                                    "code": 404,
//                                    "message": "No HTML for page & revision found"
//                                }
//                            ]
//                        }
//                    }
//                }
//            },

function Rashomon (options) {
    this.config = options.config;
    this.log = options.log;
    this.setup = this.setup.bind(this);
    this.handlers = {};
    this.backends = {};
    this.handler = {
        routes: [
            {
                // domain creation
                path: '/v1/{domain}',
                methods: {
                    PUT: {
                        handler: this.putDomain.bind(this)
                    }
                }
            },
            {
                path: '/v1/{domain}/{bucket}/',
                methods: {
                    GET: {
                        handler: this.listBucket.bind(this)
                    }
                }
            },
            {
                // bucket creation
                path: '/v1/{domain}/{bucket}',
                methods: {
                    PUT: {
                        handler: this.putBucket.bind(this)
                    }
                }
            },
            {
                path: '/v1/{domain}/{bucket}/{key}',
                methods: {
                    PUT: {
                        handler: this.handleAll.bind(this)
                    },
                    GET: {
                        handler: this.handleAll.bind(this)
                    }
                }
            },
            //{
            //    path: '/v1/{domain}/{bucket}/{key}/{rev}',
            //    methods: {
            //        PUT: {
            //            handler: this.putItem.bind(this)
            //        }
            //    }
            //}
        ]
    };
}


/*
 * Setup / startup
 *
 * @return {Promise<registry>}
 */
Rashomon.prototype.setup = function setup () {
    var self = this;
    // Set up all backends, including the default storage backend
    var backendNames = Object.keys(this.config.backends);
    var backendPromises = backendNames.map(function(key) {
            var backendConf = self.config.backends[key];
            try {
                var moduleName = __dirname + '/backends/' + backendConf.type;
                console.log(moduleName);
                var backend = require(moduleName);
                return backend(backendConf);
            } catch (e) {
                self.log('error/setup/backend/' + key, e, e.stack);
                Promise.resolve(null);
            }
    });

    return Promise.all(backendPromises)
    .then(function(backends) {
        for (var i = 0; i < backends.length; i++) {
            if (backends[i]) {
                self.backends[backendNames[i]] = backends[i];
            }
        }
    })

    // Load bucket handlers
    .then(function() {
        return Promise.promisify(fs.readdir)(__dirname + '/buckets');
    })

    .then(function(handlerNames) {
        var handlers = [];
        handlerNames.forEach(function (fileName) {
            try {
                // Instantiate one for each configured backend?
                var handlerFn = require(__dirname + '/buckets/' + fileName);
                self.handlers[fileName] = {};
                Object.keys(self.backends).forEach(function(backendID) {
                    var handler = handlerFn({
                        config: self.config.handlers[fileName],
                        backend: self.backends[backendID]
                    }, self.log);
                    if (handler) {
                        // ex:
                        // self.handlers["revisioned-blob"]["store/default"]
                        self.handlers[fileName][backendID] = handler;
                        self.log('notice/setup/bucket', fileName);
                    }
                });
            } catch (e) {
                self.log('warning/setup/handlers', e, e.stack);
            }
        });
    })

    .then(function() {
        return self.createSystemBucket();
    })

    .then(function(res) {
        console.log(res);
        // Fake it for now:
        self.registry = fakeRegistry;
    })

    // Finally return the handler
    .then(function() {
        return self.handler;
    })
    .catch(function(e) {
        self.log('error/rashomon/setup', e, e.stack);
    });
};

Rashomon.prototype.createSystemBucket = function() {
    // XXX: Retrieve the global config using the default revisioned blob
    // bucket & backend
    var sysprefix = this.config.backends['store/default'].sysprefix;

    var rootHandler = this.handlers.kv_rev['store/default'];

    var rootRequest = {
        method: 'GET',
        uri: '',
        params: {
            prefix: sysprefix,
            domain: sysprefix,
            bucket: 'system'
        }
    };

    return rootHandler({}, rootRequest)
    .then(function(res) {
        if (res.status === 200) {
            return res;
        } else {

            var rootReq = {
                method: 'PUT',
                uri: '',
                params: {
                    prefix: sysprefix,
                    domain: sysprefix,
                    bucket: 'system'
                },
                body: {
                    type: 'kv_rev',
                    options: {
                        keyType: 'text',
                        valueType: 'blob'
                    }
                }
            };

            // XXX: change the registry to not use verbs here
            return rootHandler({}, rootReq)
            .then(function() {
                return rootHandler({}, rootRequest);
            });
        }
    });
};

/**
 * Universal bucket handler
 *
 * Looks up account & bucket, authenticates the request and calls the bucket
 * handler for the method if found.
 */
Rashomon.prototype.handleAll = function (env, req) {
    var domain = this.registry[req.params.domain];
    if (domain) {
        var bucket = domain.buckets[req.params.bucket];
        if (bucket) {
            // XXX: authenticate against bucket ACLs
            //console.log(bucket);
            var bucketTypeHandlers = this.handlers[bucket.type];
            var handler = bucketTypeHandlers && bucketTypeHandlers[bucket.backendID];
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.uri = req.uri.replace(/^(?:\/[^\/]+){3}/, '');
                req.params.prefix = domain.prefix;
                console.log(req);
                // XXX: shift params?
                //req.params = req.params.slice(2);
                return handler(env, req);
            } else {
                // Options request
                return Promise.resolve({
                    headers: {
                        Allow: Object.keys(bucket.handlers).join(' ')
                    },
                    status: 405,
                    body: {
                        "code":"MethodNotAllowedError",
                        "message": req.method + " is not allowed"
                    }
                });
            }
        } else {
            return Promise.resolve({
                status: 404,
                body: {
                    "code": "NotFoundError",
                    "message": "Bucket " + req.params.domain + '/'
                                + req.params.bucket + " not found for " + req.uri
                }
            });
        }
    } else {
        return Promise.resolve({
            status: 404,
            body: {
                "code":"NotFoundError",
                "message": "Domain " + req.params.domain + " not found for "
                            + req.uri
            }
        });
    }
};

Rashomon.prototype.putDomain = function (env, req) {
    if (/^\/v1\/[a-zA-Z]+(?:\.[a-zA-Z\.]+)*$/.test(req.uri)) {
        // Insert the domain
        // Verify the domain metadata
        var exampleBody = {
            prefix: 'enwiki',
            buckets: [
            {
                name: 'pages', // path
                type: 'kv_rev', // used for handler selection
                keyspace: 'storoid1_enwiki' // auto-generated from name
            }
            ]
        };

        var sysprefix = this.config.backends['store/default'].sysprefix;
        var domainReq = {
            method: 'PUT',
            uri: '/' + req.params.domain,
            headers: req.headers,
            params: {
                prefix: sysprefix,
                domain: sysprefix,
                bucket: 'system'
            },
            body: exampleBody // req.body
        };
        var rootHandler = this.handlers.kv_rev['store/default'];
        return rootHandler({}, domainReq);

    } else {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Invalid domain requested'
            }
        });
    }
};

Rashomon.prototype.putBucket = function (env, req) {
    // check if the domain exists
    if (!this.registry[req.params.domain]) {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Domain does not exist'
            }
        });
    }
    // XXX: fake the body
    req.body = {
        type: 'kv_rev',
        options: {
            keyType: 'text',
            valueType: 'blob'
        }
    };
    // Check whether we have a backend for the requested type
    if (req.body && req.body.constructor === Object
            && req.body.type
            && this.handlers[req.body.type])
    {
        // XXX: Use a generic default here rather than store/default?
        var handler = this.handlers[req.body.type]['store/default'];
        var domOptions = this.registry[req.params.domain];

        //XXX: Fake the registry entry for now
        domOptions = {
            prefix: 'enwiki',
            buckets: {}
        };
        req.params.prefix = domOptions.prefix;
        req.uri = '';
        return handler(env, req);
    } else {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Invalid domain requested'
            }
        });
    }
};

Rashomon.prototype.listBucket = function (env, req) {
    // check if the domain exists
    if (!this.registry[req.params.domain]) {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Domain does not exist'
            }
        });
    }
    var domainInfo = this.registry[req.params.domain];
    var bucketInfo = domainInfo.buckets[req.params.bucket];
    if (!bucketInfo) {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Domain does not exist'
            }
        });
    }

    var handler = this.handlers[bucketInfo.type]['store/default'];
    if (handler) {
        console.log(bucketInfo);
        req.uri = '/';
        req.params.prefix = domainInfo.prefix;
        return handler(env, req);
    } else {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'No bucket handler found'
            }
        });
    }
};


/**
 * Factory
 * @param options
 * @return {Promise<registration>} with registration being the registration
 * object
 */
function makeRashomon (options) {
    // XXX: move to global config
    options.config = {
        backends: {
            "store/default": {
                "type": "cassandra",
                "hosts": ["localhost"],
                "id": "<uuid>",
                "keyspace": "system",
                "username": "test",
                "password": "test",
                "poolSize": 1,
                // The Storoid root bucket prefix
                "sysprefix": "storoid1"
            }
            // "queue/default": {}
        },
        handlers: {}
        // bucket type -> handler config
    };

    var rashomon = new Rashomon(options);
    return rashomon.setup();
}

module.exports = makeRashomon;

