"use strict";
/*
 * Rashomon HTTP backend handler.
 */

// global includes
var prfun = require('prfun');
var fs = require('fs');
var util = require('util');

function reverseDomain (domain) {
    return domain.toLowerCase().split('.').reverse().join('.');
}

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
                    },
                    GET: {
                        handler: this.getBucket.bind(this)
                    }
                }
            },
            {
                path: '/v1/{domain}/{bucket}/{+rest}',
                methods: {
                    PUT: {
                        handler: this.handleAll.bind(this)
                    },
                    GET: {
                        handler: this.handleAll.bind(this)
                    }
                }
            }
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
        // self.registry = fakeRegistry;
        return self.loadRegistry();
    })

    .then(function(res) {
        console.log('registry', res);
        self.registry = res;
    })

    // Finally return the handler
    .then(function() {
        return self.handler;
    })
    .catch(function(e) {
        self.log('error/rashomon/setup', e, e.stack);
    });
};

var domainRegistrySchema = {
    table: 'domains',
    attributes: {
        domain: 'string',
        acls: 'json', // default acls for entire domain
        quota: 'varint'
    },
    index: {
        hash: 'domain'
    }
};

var tableRegistrySchema = {
    table: 'tables',
    attributes: {
        domain: 'string',
        table: 'string',
        type: 'string',     // 'table' or 'kv'
        store: 'string',    // 'default' or uuid
        acls: 'json'
    },
    index: {
        hash: 'domain',
        range: 'table'
    }
};

Rashomon.prototype.loadRegistry = function() {
    var self = this;
    var store = self.stores.default;
    // XXX: Retrieve the global config using the default revisioned blob
    // bucket & backend
    var sysDomain = this.config.sysdomain;

    // check if the domains table exists
    return store.getSchema(sysDomain, 'domains')
    .catch(function(err) {
        console.log(err.stack);
        return store.createTable(sysDomain, domainRegistrySchema);
    })
    // check if the 'table' registry exists
    .then(function() {
        return store.getSchema(sysDomain, 'tables')
        .catch(function(err) {
            return store.createTable(sysDomain, tableRegistrySchema);
        });
    })
    // Load the registry
    .then(function() {
        var registry = {};
        var domainQuery = {
            table: 'domains'
        };
        return store.get(sysDomain, { table: 'domains' })
        .then(function(res) {
            //console.log('domains', res);
            res.items.forEach(function(domainObj) {
                domainObj.tables = {};
                registry[domainObj.domain] = domainObj;
            });
            return store.get(sysDomain, { table: 'tables' });
        })
        .then(function(res) {
            //console.log('tables', res);
            res.items.forEach(function(tableObj) {
                var domain = registry[tableObj.domain];
                if (!domain) {
                    throw new Error('Domain ' + tableObj.domain
                        + ' has tables, but no domain entry!');
                }
                domain.tables[tableObj.table] = tableObj;
            });

            return registry;
        });
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
        var table = domain.tables[req.params.bucket];
        if (table) {
            // XXX: authenticate against table ACLs
            //console.log(table);
            var handler = this.buckets[table.type];
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.uri = req.uri.replace(/^(?:\/[^\/]+){3}/, '');
                //console.log(req);
                // TODO: look up store from registry!
                return handler(req, this.stores[table.store] || this.stores.default);
            } else {
                // Options request
                return Promise.resolve({
                    headers: {
                        // FIXME: table.handlers does not exist
                        Allow: Object.keys(table.handlers).join(' ')
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
    var self = this;
    if (/^\/v1\/[a-zA-Z]+(?:\.[a-zA-Z\.]+)*$/.test(req.uri)) {
        // Insert the domain
        // Verify the domain metadata
        var exampleBody = {
            acls: {},
            quota: 0
        };

        var sysdomain = this.config.sysdomain;
        var domain = req.params.domain.toLowerCase();
        var query = {
            table: 'domains',
            attributes: {
                domain: domain,
                acls: req.body.acls,
                quota: req.body.quota
            }
        };
        return this.stores.default.put(sysdomain, query)
        .then(function() {
            return self.loadRegistry()
            .then(function(registry) {
                self.registry = registry;
            });
        });
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
    var self = this;
    // check if the domain exists
    var domain = (req.params.domain || '').toLowerCase();
    if (!this.registry[domain]) {
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
        return this.buckets.kv(req, this.stores.default)
        .then(function(res) {
            // Insert the table into the registry
            var query = {
                table: 'tables',
                attributes: {
                    domain: domain,
                    table: req.params.bucket,
                    type: 'kv',
                    store: 'default'
                }
            };
            return self.stores.default.put(self.config.sysdomain, query)
            .then(function() {
                self.loadRegistry()
                .then(function(registry) {
                    self.registry = registry;
                });
                return res;
            });
        });
    } else {
        return Promise.resolve({
            status: 400,
            body: {
                message: 'Invalid domain requested'
            }
        });
    }
};

Rashomon.prototype.getBucket = function (env, req) {
    var domain = req.params.domain.toLowerCase();
    var bucket = req.params.bucket;
    var query = {
        table: 'tables',
        attributes: {
            domain: domain,
            table: bucket
        }
    };
    return this.stores.default.get(this.config.sysdomain, query)
    .then(function(res) {
        return {
            status: 200,
            body: res.items[0]
        };
    });
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
        sysdomain: "system.storoid", // reverse DNS notation
        storage: {
            "default": {
                "type": "cassandra",
                "hosts": ["localhost"],
                "id": "<uuid>",
                "keyspace": "system",
                "username": "test",
                "password": "test",
                "poolSize": 70,
                // The Storoid root bucket prefix
            }
        },
        handlers: {}
        // bucket type -> handler config
    };

    var rashomon = new Rashomon(options);
    return rashomon.setup();
}

module.exports = makeRashomon;

