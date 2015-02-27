"use strict";
/*
 * Cassandra-backed table storage service
 */

// global includes
var fs = require('fs');
var yaml = require('js-yaml');
var util = require('util');

// TODO: move to separate package!
var spec = yaml.safeLoad(fs.readFileSync(__dirname + '/table.yaml'));

function reverseDomain (domain) {
    return domain.toLowerCase().split('.').reverse().join('.');
}

function RBCassandra (options) {
    this.options = options;
    this.conf = options.conf;
    this.log = options.log;
    this.setup = this.setup.bind(this);
    this.store = null;
    this.handler = {
        spec: spec,
        operations: {
            createTable: this.createTable.bind(this),
            dropTable: this.dropTable.bind(this),
            get: this.get.bind(this),
            put: this.put.bind(this)
        }
    };
}

RBCassandra.prototype.createTable = function (rb, req) {
    var store = this.store;
    // XXX: decide on the interface
    req.body.table = req.params.table;
    var domain = reverseDomain(req.params.domain);

    // check if the domains table exists
    return store.createTable(domain, req.body)
    .then(function() {
        return {
            status: 201, // created
            body: {
                type: 'table_created',
                title: 'Table was created.',
                domain: req.params.domain,
                table: req.params.table
            }
        };
    })
    .catch(function(e) {
        if (e.status >= 400) {
            return {
                status: e.status,
                body: e.body
            };
        }
        return {
            status: 500,
            body: {
                type: 'table_creation_error',
                title: 'Internal error while creating a table within the cassandra storage backend',
                stack: e.stack,
                schema: req.body
            }
        };
    });
};

// Query a table
RBCassandra.prototype.get = function (rb, req) {
    var rp = req.params;
    if (!rp.rest && !req.body) {
        // Return the entire table
        // XXX: Only list the hash keys?
        req.body = {
            table: rp.table,
            limit: 10
        };
    }
    var domain = reverseDomain(req.params.domain);
    return this.store.get(domain, req.body)
    .then(function(res) {
        return {
            status: res.items.length ? 200 : 404,
            body: res
        };
    })
    .catch(function(e) {
        return {
            status: 500,
            body: {
                type: 'query_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack
            }
        };
    });
};

// Update a table
RBCassandra.prototype.put = function (rb, req) {
    var domain = reverseDomain(req.params.domain);
    // XXX: Use the path to determine the primary key?
    return this.store.put(domain, req.body)
    .then(function(res) {
        return {
            status: 201 // created
        };
    })
    .catch(function(e) {
        return {
            status: 500,
            body: {
                type: 'update_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack,
                req: req
            }
        };
    });
};

RBCassandra.prototype.dropTable = function (rb, req) {
    var domain = reverseDomain(req.params.domain);
    return this.store.dropTable(domain, req.params.table)
    .then(function(res) {
        return {
            status: 204 // done
        };
    })
    .catch(function(e) {
        return {
            status: 500,
            body: {
                type: 'delete_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack
            }
        };
    });
};

/*
 * Setup / startup
 *
 * @return {Promise<registry>}
 */
RBCassandra.prototype.setup = function setup () {
    var self = this;
    // Set up storage backend
    var backend = require('./lib/index');
    return backend(self.options)
    .then(function(store) {
        self.store = store;
        // console.log('RB setup complete', self.handler);
        return self.handler;
    });
};


/**
 * Factory
 * @param options
 * @return {Promise<registration>} with registration being the registration
 * object
 */
function makeRBCassandra (options) {
    var rb = new RBCassandra(options);
    return rb.setup();
}

module.exports = makeRBCassandra;

