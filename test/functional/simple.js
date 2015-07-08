"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = require('../utils/test_router.js');
var deepEqual = require('../utils/test_utils.js').deepEqual;
var utils = require('../utils/test_utils.js');

describe('Simple tables', function() {

    before(function () { return router.setup(); });

    context('Create', function() {
        it('creates a simple test table', function() {
            this.timeout(15000);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: {
                    table: 'simple-table',
                    options: {
                        durability: 'low',
                        compression: [
                            {
                                algorithm: 'deflate',
                                block_size: 256
                            }
                        ]
                    },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        body: 'blob',
                        'content-type': 'string',
                        'content-length': 'varint',
                        'content-sha256': 'string',
                        // redirect
                        'content-location': 'string',
                        // 'deleted', 'nomove' etc?
                        restrictions: 'set<string>',
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'latestTid', type: 'static' },
                        { attribute: 'tid', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('throws an error on unsupported schema update request', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: {
                    table: 'simple-table',
                    options: {
                        durability: 'low',
                        compression: [
                            {
                                algorithm: 'deflate',
                                block_size: 256
                            }
                        ]
                    },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        body: 'blob',
                        'content-type': 'string',
                        'content-length': 'varint',
                        'content-sha256': 'string',
                        // redirect
                        'content-location': 'string',
                        // 'deleted', 'nomove' etc?
                        //
                        // NO RESTRICTIONS HERE
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'latestTid', type: 'static' },
                        { attribute: 'tid', type: 'range', order: 'desc' }
                    ]
                }
            }).then(function(response){
                deepEqual(response.status, 400);
            });
        });
    });

    context('Put', function() {
        it('inserts a row', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    consistency: 'localQuorum',
                    attributes: {
                        key: 'testing',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('updates a row', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: "testing",
                        tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        body: new Buffer("<p>Service Oriented Architecture</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('inserts using if-not-exists with non index attributes', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    if : "not exists",
                    attributes: {
                        key: "testing if not exists",
                        tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                        body: new Buffer("<p>if not exists with non key attr</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('inserts with if-condition and non index attributes', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "another test",
                        tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')),
                        body: new Buffer("<p>test<p>")
                    },
                    if: { body: { "eq": new Buffer("<p>Service Oriented Architecture</p>") } }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('inserts static columns', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'test',
                        tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        latestTid: utils.testTidFromDate(new Date('2014-01-01 00:00:00')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'test2',
                            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                            body: new Buffer("<p>test<p>"),
                            latestTid: utils.testTidFromDate(new Date('2014-01-01 00:00:00')),
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'test',
                            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                            latestTid: utils.testTidFromDate(new Date('2014-01-02 00:00:00'))
                        }
                    }
                });
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
    });

    context('Get', function() {
        it('retrieves a row', function() {
            return router.request({
                uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: 'testing',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items, [ { key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                } ]);
            });
        });
        it('retrieves using between condition', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    //from: 'foo', // key to start the query from (paging)
                    limit: 3,
                    attributes: {
                        tid: { "BETWEEN": [ utils.testTidFromDate(new Date('2013-07-08 18:43:58-0700')),
                            utils.testTidFromDate(new Date('2013-08-08 18:45:58-0700'))] },
                        key: "testing"
                    }
                }
            }).then(function(response) {
                response= response.body;
                deepEqual(response.items, [{ key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                }]);
            });
        });
        it('retrieves static columns', function() {
            return router.request({
                uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    proj: ["key", "tid", "latestTid", "body"],
                    attributes: {
                        key: 'test2',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'test2');
                deepEqual(response.body.items[0].body, new Buffer("<p>test<p>"));
                deepEqual(response.body.items[0].latestTid,
                    utils.testTidFromDate(new Date('2014-01-01 00:00:00')));
                return router.request({
                    uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: 'test',
                            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'test');
                deepEqual(response.body.items[0].tid, '28730300-0095-11e3-9234-0123456789ab');
                deepEqual(response.body.items[0].latestTid,
                    utils.testTidFromDate(new Date('2014-01-01 00:00:00')));
            });
        });
        it('retrieves using order by', function() {
            return router.request({
                uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    order: {tid: "desc"},
                    attributes: {
                        key: "test"
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 2);
                deepEqual(response.body.items[0].latestTid,
                    utils.testTidFromDate(new Date('2014-01-01 00:00:00')));
                deepEqual(response.body.items[1].latestTid,
                    utils.testTidFromDate(new Date('2014-01-01 00:00:00')));
                delete response.body.items[0].latestTid;
                delete response.body.items[1].latestTid;
                deepEqual(response.body.items, [{
                    "key": "test",
                    "tid": "52dcc300-015e-11e3-9234-0123456789ab",
                    "body": null,
                    "content-type": null,
                    "content-length": null,
                    "content-sha256": null,
                    "content-location": null,
                    "restrictions": null
                },{
                    key: 'test',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    body: null,
                    'content-type': null,
                    'content-length': null,
                    'content-sha256': null,
                    'content-location': null,
                    restrictions: null,
                }]);
            });
        });
    });

    context('Drop', function() {
        this.timeout(15000);
        it('drops table', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/simple-table",
                method: "delete",
                body: {}
            });
        });
    });
});
