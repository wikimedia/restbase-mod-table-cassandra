"use strict";
/*
 * Rashomon HTTP backend handler.
 */

// global includes
var async = require('async');
var prfun = require('prfun');
var fs = require('fs');
var CassandraRevisionStore = require('./CassandraRevisionStore');


// XXX: load accounts from Cassandra
// Accounts.load()
// .then(newAccounts) {
//     accounts = newAccounts;
// }


function Rashomon (options) {
    this.config = options.config;
    this.log = options.log;
    this.setup = this.setup.bind(this);
    this.handler = {};
    this.backends = {};
    this.handler = {
        routes: [
            {
                path: '/v1/{domain}/{bucket}/{title}/rev/{rev}/html',
                methods: {
                    all: {
                        handler: this.handleAll.bind(this),
                        doc: { /* swagger docs */
                            "summary": "Retrieves the html of a specific revision",
                            "notes": "Returns HTML+RDFa.",
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

/*
 * Setup / startup
 *
 * @return {Promise<registry>}
 */
Rashomon.prototype.setup = function setup (resolve, reject) {
    var self = this;
    var setupCB = function(err, res) {
        if (err) { reject(err); }
        else { resolve(err); }
    };
    // Set up all backends, including the default storage backend
    var backendNames = Object.keys(this.config.backends);
    var backendPromises = backendNames.map(function(name) {
            try {
                var backend = require('./backends/' + name);
                return backend(self.config.backends[name]);
            } catch (e) {
                self.log('warning/setup/backend', e);
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
        return prfun.promisify(fs.readfile)('./buckets');
    })

    .then(function(handlerNames) {
        var handlers = [];
        handlerNames.forEach(function (fileName) {
            if (/.js$/.test(fileName)) {
                var name = fileName.replace(/.js$/, '');
                try {
                    // Instantiate one for each configured backend?
                    var handlerFn = require('./buckets/' + fileName);
                    self.handlers[name] = {};
                    self.backends.forEach(function(backendID) {
                        var handler = handlerFn({
                            config: self.config.handlers[name],
                            backend: self.backends[backendID]
                        });
                        if (handler) {
                            // ex:
                            // self.handlers["revisioned-blob"]["store/default"]
                            self.handlers[name][backendID] = handler;
                        }
                    });
                } catch (e) {
                    self.log('warning/setup/handlers', e);
                }
            }
        });
    })

    .then(function() {
        // XXX: Retrieve the global config using the default revisioned blob
        // bucket & backend
        // this.handlers['revisioned-blob'](req, this.backends['store/default'])
        // Fake it for now:
        self.registry = {
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
    });
};

/**
 * Universal bucket handler
 *
 * Looks up account & bucket, authenticates the request and calls the bucket
 * handler for the method if found.
 */
Rashomon.prototype.handleAll = function (env, req) {
    // XXX: validate params[0] & 1
    var domain = this.registry[req.params[0]];
    if (domain) {
        var bucket = domain.buckets[req.params[1]];
        if (bucket) {
            // XXX: authenticate against bucket ACLs
            var handlerObj = this.handlers[bucket.type][bucket.backendID];
            var handler = handlerObj[req.method];
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.path = req.params[2];
                req.url = req.params[2];
                req.params = req.params.slice(2);
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
    // XXX: move
    options.config = {
        backends: {
            "store/default": {
                default: {
                    "type": "cassandra",
                    "hosts": ["localhost"],
                    "id": "<uuid>",
                    "keyspace": "testdb",
                    "username": "test",
                    "password": "test",
                    "poolSize": 1
                }
            }
            // "queue/default": {}
        },
        // bucket type -> handlers & their configs
    };

    var rashomon = new Rashomon(options);
    return new Promise(rashomon.setup);
}

module.exports = makeRashomon;

