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
    "storoid.system" : {
        buckets: {
            system: {
                type: 'kv',
                backend: 'default'
            }
        }
    },
    "en.wikipedia.org": {
        prefix: 'enwiki',
        buckets: {
            "foo": {
                "type": "kv_rev",
                // XXX: is this actually needed?
                "backend": "cassandra",
                "backendID": "default",
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
    this.buckets = {};
    this.stores = {};
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
    // Set up storage backends
    var storageNames = Object.keys(this.config.storage);
    var storagePromises = storageNames.map(function(key) {
            var storageConf = self.config.storage[key];
            try {
                var moduleName = __dirname + '/storage/' + storageConf.type;
                console.log(moduleName);
                var backend = require(moduleName);
                return backend(storageConf);
            } catch (e) {
                self.log('error/setup/backend/' + key, e, e.stack);
                Promise.resolve(null);
            }
    });

    return Promise.all(storagePromises)
    .then(function(stores) {
        for (var i = 0; i < stores.length; i++) {
            if (stores[i]) {
                self.stores[storageNames[i]] = stores[i];
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
                self.buckets[fileName] = handlerFn(self.log);
            } catch (e) {
                self.log('warning/setup/handlers', e, e.stack);
            }
        });
        self.registry = fakeRegistry;
        return self.createSystemBucket();
    })

    .then(function(res) {
        console.log(res);
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
    var self = this;
    // XXX: Retrieve the global config using the default revisioned blob
    // bucket & backend
    var sysdomain = this.config.storage.default.sysdomain;

    var bucketHandler = this.buckets.kv;

    var rootRequest = {
        method: 'GET',
        uri: '',
        params: {
            domain: sysdomain,
            bucket: 'system'
        }
    };

    return this.handleAll({}, rootRequest)
    .then(function(res) {
        console.log(res);
        if (res.status === 200) {
            return res;
        } else {

            var rootReq = {
                method: 'PUT',
                uri: '',
                params: {
                    domain: sysdomain,
                    bucket: 'system'
                },
                body: {
                    type: 'kv_rev',
                    keyType: 'text',
                    valueType: 'blob'
                }
            };

            // XXX: change the registry to not use verbs here
            return self.putBucket({}, rootReq)
            .then(function() {
                return self.handleAll({}, rootRequest);
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
            var handler = this.buckets.kv;
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.uri = req.uri.replace(/^(?:\/[^\/]+){3}/, '');
                console.log(req);
                // XXX: shift params?
                //req.params = req.params.slice(2);
                // TODO: look up store from registry!
                return handler(req, this.stores.default);
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
                                + req.params.bucket + " not found for "
                                + JSON.stringify(req.uri)
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
            buckets: {
                pages: {
                    type: 'kv_rev', // used for handler & backend selection
                    backend: 'default', // which backend to use
                    keyType: 'string',
                    valueType: 'blob'
                }
            }
        };

        var sysprefix = this.config.backends['store/default'].sysprefix;
        var domainReq = {
            method: 'PUT',
            uri: '/' + req.params.domain,
            headers: req.headers,
            params: {
                domain: sysprefix,
                bucket: 'system'
            },
            body: exampleBody // req.body
        };
        var rootHandler = this.handlers.kv;
        return rootHandler(domainReq, this.stores.default);

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
        keyType: 'string',
        valueType: 'blob'
    };
    // Check whether we have a backend for the requested type
    if (req.body && req.body.constructor === Object
            && req.body.type
            && this.buckets.kv)
    {
        req.uri = '';
        return this.buckets.kv(req, this.stores.default);
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
    //var domainInfo = this.registry[req.params.domain];
    //var bucketInfo = domainInfo.buckets[req.params.bucket];
    //if (!bucketInfo) {
    //    return Promise.resolve({
    //        status: 400,
    //        body: {
    //            message: 'Domain does not exist'
    //        }
    //    });
    //}

    var handler = this.buckets.kv;
    if (handler) {
        req.uri = '/';
        return handler(req, this.stores.default);
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
        storage: {
            "default": {
                "type": "cassandra",
                "hosts": ["localhost"],
                "id": "<uuid>",
                "keyspace": "system",
                "username": "test",
                "password": "test",
                "poolSize": 1,
                // The Storoid root bucket prefix
                "sysdomain": "storoid.system"
            }
        },
        handlers: {}
        // bucket type -> handler config
    };

    var rashomon = new Rashomon(options);
    return rashomon.setup();
}

module.exports = makeRashomon;

