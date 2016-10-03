"use strict";

/*
 * Cassandra-backed table storage service
 */

// global includes
const spec = require('restbase-mod-table-spec').spec;

class RBCassandra {
    constructor(options) {
        this.options = options;
        this.conf = options.conf;
        this.log = options.log;
        this.setup = this.setup.bind(this);
        this.store = null;
        this.handler = {
            spec,
            operations: {
                createTable: this.createTable.bind(this),
                dropTable: this.dropTable.bind(this),
                getTableSchema: this.getTableSchema.bind(this),
                get: this.get.bind(this),
                put: this.put.bind(this)
            }
        };
    }

    createTable(rb, req) {
        const store = this.store;
        // XXX: decide on the interface
        req.body.table = req.params.table;
        const domain = req.params.domain;

        // check if the domains table exists
        return store.createTable(domain, req.body)
        .then(() => ({
            // created
            status: 201,

            body: {
                type: 'table_created',
                title: 'Table was created.',
                domain: req.params.domain,
                table: req.params.table
            }
        }))
        .catch((e) => {
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
                    title: 'Internal error while creating a table' +
                        ' within the cassandra storage backend',
                    stack: e.stack,
                    err: e,
                    req
                }
            };
        });
    }

    // Query a table
    get(rb, req) {
        const rp = req.params;
        if (!rp.rest && !req.body) {
            // Return the entire table
            // XXX: Only list the hash keys?
            req.body = {
                table: rp.table,
                limit: 10
            };
        }
        const domain = req.params.domain;
        return this.store.get(domain, req.body)
        .then((res) => ({
            status: res.items.length ? 200 : 404,
            body: res
        }))
        .catch((e) => ({
            status: 500,

            body: {
                type: 'query_error',
                title: 'Error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: {
                    uri: req.uri,
                    headers: req.headers,
                    body: req.body && JSON.stringify(req.body).slice(0,200)
                }
            }
        }));
    }

    // Update a table
    put(rb, req) {
        const domain = req.params.domain;
        // XXX: Use the path to determine the primary key?
        return this.store.put(domain, req.body)
        .thenReturn({
            // created
            status: 201
        })
        .catch((e) => ({
            status: 500,

            body: {
                type: 'update_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: {
                    uri: req.uri,
                    headers: req.headers,
                    body: req.body && JSON.stringify(req.body).slice(0,200)
                }
            }
        }));
    }

    dropTable(rb, req) {
        const domain = req.params.domain;
        return this.store.dropTable(domain, req.params.table)
        .thenReturn({
            // done
            status: 204
        })
        .catch((e) => ({
            status: 500,

            body: {
                type: 'delete_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: {
                    uri: req.uri,
                    headers: req.headers,
                    body: req.body && JSON.stringify(req.body).slice(0,200)
                }
            }
        }));
    }

    getTableSchema(rb, req) {
        const domain = req.params.domain;
        return this.store.getTableSchema(domain, req.params.table)
        .then((res) => ({
            status: 200,
            headers: { etag: res.tid.toString() },
            body: res.schema
        }))
        .catch((e) => ({
            status: 500,

            body: {
                type: 'schema_query_error',
                title: 'Internal error querying table schema in Cassandra storage backend',
                stack: e.stack,
                err: e,
                req: {
                    uri: req.uri,
                    headers: req.headers,
                    body: req.body && JSON.stringify(req.body).slice(0,200)
                }
            }
        }));
    }

    /*
     * Setup / startup
     *
     * @return {Promise<registry>}
     */
    setup() {
        // Set up storage backend
        const backend = require('./lib/index');
        return backend(this.options)
        .then((store) => {
            this.store = store;
            return this.handler;
        });
    }
}


/**
 * Factory
 * @param options
 * @return {Promise<registration>} with registration being the registration
 * object
 */
function makeRBCassandra(options) {
    const rb = new RBCassandra(options);
    return rb.setup();
}

module.exports = makeRBCassandra;

