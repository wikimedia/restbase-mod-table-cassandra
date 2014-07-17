"use strict";
/*
 * Rashomon HTTP backend handler.
 */

// global includes
var prfun = require('prfun');
var fs = require('fs');

// TODO: retrieve dynamically from storage!
var fakeRegistry = {
    "en.wikipedia.org": {
        buckets: {
            "pages": {
                "type": "revisioned-blob",
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

function Rashomon (options) {
    this.config = options.config;
    this.log = options.log;
    this.setup = this.setup.bind(this);
    this.handlers = {};
    this.backends = {};
    this.handler = {
        routes: [
            {
                path: '/v1/{domain}/{bucket}/{title}/rev/{rev}/{prop}',
                methods: {
                    get: {
                        handler: this.handleAll.bind(this),
                        doc: { /* swagger docs */
                            "summary": "Retrieves the property of a specific revision through Rashomon",
                            "notes": "See <link> for information on properties and content types.",
                            "type": "html",
                            "produces": ["text/html;spec=mediawiki.org/specs/html/1.0"],
                            "responseMessages": [
                                {
                                    "code": 404,
                                    "message": "No HTML for page & revision found"
                                }
                            ]
                        }
                    },
                    post: {
                        handler: this.handleAll.bind(this),
                        doc: { /* swagger docs */
                            "summary": "Saves a new revision to Rashomon",
                            "notes": "Some notes.",
                            "type": "html",
                            "produces": ["text/html;spec=mediawiki.org/specs/html/1.0"],
                            "responseMessages": [
                                {
                                    "code": 404,
                                    "message": "No HTML for page & revision found"
                                }
                            ]
                        }
                    }
                }
            }
        ]
    };
}


Rashomon.prototype.loadMetaData = function (env) {
    var sysHandler = this.handlers['revisioned-blob']['store/default'];
    // list domains
    sysHandler(env, {
        uri: '/v1/system/',
        method: 'GET'
    })
    // get domain metadata for each
    .then(function(domains) {
        //
    });
    // list buckets
    // get bucket metadata for each
};


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
                self.log('warning/setup/backend/' + key, e);
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
                    });
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
        // XXX: Retrieve the global config using the default revisioned blob
        // bucket & backend
        // this.handlers['revisioned-blob'](req, this.backends['store/default'])
        // Fake it for now:
        self.registry = fakeRegistry;
    })

    // Finally return the handler
    .then(function() {
        return self.handler;
    })
    .catch(function(e) {
        this.log('error/rashomon/setup', e, e.stack);
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
            var handlerObj = bucketTypeHandlers && bucketTypeHandlers[bucket.backendID];
            var handler = handlerObj.verbs[req.method];
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.uri = req.uri.replace(/^(?:\/[^\/]+){3}/, '');
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
                    "message": "Bucket " + req.params[1] + " not found"
                }
            });
        }
    } else {
        return Promise.resolve({
            status: 404,
            body: {
                "code":"NotFoundError",
                "message": "Account " + req.params[0] + " not found"
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
                "keyspace": "testdb",
                "username": "test",
                "password": "test",
                "poolSize": 70
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

