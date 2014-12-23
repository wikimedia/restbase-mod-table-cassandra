"use strict";

global.Promise = require('bluebird');

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var cass = require('cassandra-driver');
var RouteSwitch = require('routeswitch');
var uuid = require('node-uuid');
var makeClient = require('../lib/index');
var dbu = require('../lib/dbutils.js');
//TODO: change this name 
var router = require('../test/test_router.js');


function deepEqual (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + JSON.stringify(expected, null, 2));
        console.log('Result:\n' + JSON.stringify(result, null, 2));
        throw e;
    }
}

var DB = require('../lib/db.js');

describe('DB backend', function() {
    before(function() {
        return makeClient({
            log: function(level, msg) {
                if (!/^info|verbose|debug/.test(level)) {
                    console.log(level, msg);
                }
            },
            conf: {
                hosts: ['localhost']
            }
        })
        .then(function(db) {
            DB = db;
            return router.makeRouter();
        });
    });
    describe('createTable', function() {
        this.timeout(15000);
        it('varint table', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/varintTable',
                method: 'put',
                body: {
                    // keep extra redundant info for primary bucket table reconstruction
                    domain: 'restbase.cassandra.test.local',
                    table: 'varintTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        rev: 'varint',
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'rev', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('simple table', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleTable',
                method: 'put',
                body: {
                    // keep extra redundant info for primary bucket table reconstruction
                    domain: 'restbase.cassandra.test.local',
                    table: 'simpleTable',
                    options: { durability: 'low' },
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
        it('table with more than one range keys', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/multiRangeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'multiRangeTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        restrictions: 'set<string>',
                    },
                    index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'latestTid', type: 'static' },
                    { attribute: 'tid', type: 'range', order: 'desc' },
                        { attribute: 'uri', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('table with secondary index', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'simpleSecondaryIndexTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        restrictions: 'set<string>',
                    },
                    index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'tid', type: 'range', order: 'desc' },
                    ],
                    secondaryIndexes: {
                        by_uri : [
                            { attribute: 'uri', type: 'hash' },
                            { attribute: 'body', type: 'proj' }
                        ]
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('table with secondary index and no tid in range', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/unversionedSecondaryIndexTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'unversionedSecondaryIndexTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        //tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        restrictions: 'set<string>',
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'uri', type: 'range', order: 'desc' },
                    ],
                    secondaryIndexes: {
                        by_uri : [
                            { attribute: 'uri', type: 'hash' },
                            { attribute: 'key', type: 'range', order: 'desc' },
                            { attribute: 'body', type: 'proj' }
                        ]
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
    });

    describe('put', function() {
        it('simple put insert', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleTable/',
                method: 'put',
                body: {
                    table: 'simpleTable',
                    attributes: {
                        key: 'testing',
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('simple put insert query on table with more than one range keys', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/multiRangeTable/',
                method: 'put',
                body: {
                    table: "multiRangeTable",
                    attributes: {
                        key: 'testing',
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        uri: "test"
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('simple put update', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleTable/',
                method: 'put',
                body: {
                    table: 'simpleTable',
                    attributes: {
                        key: "testing",
                        tid: dbu.tidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        body: new Buffer("<p>Service Oriented Architecture</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('put with if not exists and non index attributes', function() {
            return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleTable/',
                    method: 'put',
                    body: {
                        table: "simpleTable",
                        if : "not exists",
                        attributes: {
                            key: "testing if not exists",
                            tid: dbu.tidFromDate(new Date('2013-08-10 18:43:58-0700')),
                            body: new Buffer("<p>if not exists with non key attr</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('put with if and non index attributes', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleTable/',
                method: 'put',
                body: {
                    table: "simpleTable",
                    attributes: {
                        key: "another test",
                        tid: dbu.tidFromDate(new Date('2013-08-11 18:43:58-0700')),
                        body: new Buffer("<p>test<p>")
                    },
                    if: { body: { "eq": new Buffer("<p>Service Oriented Architecture</p>") } }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('index update', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test",
                        tid: uuid.v1(),
                        uri: "uri1",
                        body: 'body1'
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: uuid.v1(),
                            uri: "uri2",
                            body: 'body2'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: uuid.v1(),
                            uri: "uri3",
                            body: 'body3'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: uuid.v1(),
                            uri: "uri1",
                            body: 'test_body1'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: uuid.v1(),
                            uri: "uri2",
                            body: 'test_body2'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: uuid.v1(),
                            uri: "uri3",
                            body: 'test_body3'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    url: '/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: uuid.v1(),
                            uri: "uri3",
                            // Also test projection updates
                            body: 'test_body3_modified'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('unversioned index', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/unversionedSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "unversionedSecondaryIndexTable",
                    attributes: {
                        key: "another test",
                        uri: "a uri"
                    },
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('unversioned index update', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/unversionedSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "unversionedSecondaryIndexTable",
                    attributes: {
                        key: "another test",
                        uri: "a uri",
                        body: "abcd"
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
    });

    describe('get', function() {
        it('varint predicates', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/varintTable/',
                method: 'put',
                body: {
                    table: 'varintTable',
                    attributes: {
                        key: 'testing',
                        rev: 1
                    }
                }
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            })
            .then(function () {
                return router.request({
                    url: '/v1/restbase.cassandra.test.local/varintTable/',
                    method: 'put',
                    body: {
                        table: 'varintTable',
                        attributes: {
                            key: 'testing',
                            rev: 5
                        }
                    }
                });
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            })
            .then(function () {
                return router.request({
                    url: '/v1/restbase.cassandra.test.local/varintTable/',
                    method: 'get',
                    body: {
                        table: 'varintTable',
                        limit: 3,
                        attributes: {
                            key: 'testing',
                            rev: 1
                        }
                    }
                });
            })
            .then(function(result) {
                deepEqual(result.body.items.length, 1);
            })
            .then(function () {
                return router.request({
                    url: '/v1/restbase.cassandra.test.local/varintTable/',
                    method: 'get',
                    body: {
                        table: 'varintTable',
                        limit: 3,
                        attributes: {
                            key: 'testing',
                            rev: { gt: 1 }
                        }
                    }
                });
            })
            .then(function(result) {
                deepEqual(result.body.items.length, 1);
            })
            .then(function () {
                return router.request({
                    url: '/v1/restbase.cassandra.test.local/varintTable/',
                    method: 'get',
                    body: {
                        table: 'varintTable',
                        limit: 3,
                        attributes: {
                            key: 'testing',
                            rev: { ge: 1 }
                        }
                    }
                });
            })
            .then(function(result) {
                deepEqual(result.body.items.length, 2);
            });
        });
        it('simple between', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/simpleTable/',
                method: 'get',
                body: {
                    table: "simpleTable",
                    //from: 'foo', // key to start the query from (paging)
                    limit: 3,
                    attributes: {
                        tid: { "BETWEEN": [ dbu.tidFromDate(new Date('2013-07-08 18:43:58-0700')),
                        dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700'))] },
                        key: "testing"
                    }
                }
            }).then(function(response) {
                response= response.body;
                deepEqual(response.items, [{ key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    _del: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                }]);
            });
        });
        it('simple get', function() {
            return router.request({
                url:'/v1/restbase.cassandra.test.local/simpleTable/',
                method: 'get',
                body: {
                    table: "simpleTable",
                    attributes: {
                        key: 'testing',
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items, [ { key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    _del: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                } ]);
            });
        });
        it("index query for values that doesn't match any more", function() {
            return router.request({
                url: "/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/",
                method: "get",
                body: {
                    table: "simpleSecondaryIndexTable",
                    index: "by_uri",
                    attributes: {
                        uri: "uri1"
                    }
                }
            })
            .then(function(response){
                deepEqual(response.status, 404);
                deepEqual(response.body.items.length, 0);
                return router.request({
                    url: "/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/",
                    method: "get",
                    body: {
                        table: "simpleSecondaryIndexTable",
                        index: "by_uri",
                        attributes: {
                            uri: "uri2"
                        }
                    }
                });
            })
            .then(function(response){
                deepEqual(response.body.items.length, 0);
            });
        });

        it("index query for current value", function() {
            return router.request({
                url: "/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable/",
                method: "get",
                body: {
                    table: "simpleSecondaryIndexTable",
                    index: "by_uri",
                    attributes: {
                        uri: "uri3"
                    },
                    proj: ['key', 'uri', 'body']
                }
            })
            .then(function(response){
                deepEqual(response.body.items, [{
                    key: "test2",
                    uri: "uri3",
                    body: new Buffer("test_body3_modified")
                },{
                    key: "test",
                    uri: "uri3",
                    body: new Buffer("body3")
                }]);
            });
        });
    });
    //TODO: implement this using http handler when alternate rest-url for delete item are supported 
    describe('delete', function() {
        it('simple delete query', function() {
            return DB.delete('local.test.cassandra.restbase', {
                table: "simpleTable",
                attributes: {
                    tid: dbu.tidFromDate(new Date('2013-08-09 18:43:58-0700')),
                    key: "testing"
                }
            });
        });
    });

    describe('types', function() {
        this.timeout(5000);
        it('create table', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/typeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeTable',
                    options: { durability: 'low' },
                    attributes: {
                        string: 'string',
                        blob: 'blob',
                        set: 'set<string>',
                        'int': 'int',
                        varint: 'varint',
                        decimal: 'decimal',
                        //'float': 'float',
                        'double': 'double',
                        'boolean': 'boolean',
                        timeuuid: 'timeuuid',
                        uuid: 'uuid',
                        timestamp: 'timestamp',
                        json: 'json'
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ]
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('put', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': -1,
                        varint: -4503599627370496,
                        decimal: '1.2',
                        //'float': 1.2,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        }
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('put 2', function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': 1,
                        varint: 1,
                        decimal: '1.4',
                        //'float': 1.2,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        }
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it("get", function() {
            return router.request({
                url: '/v1/restbase.cassandra.test.local/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'double','boolean','timeuuid','uuid',
                            'timestamp','json']
                }
            })
            .then(function(response){
                deepEqual(response.body.items, [{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': 1,
                    varint: 1,
                    decimal: '1.4',
                    //'float': 1.2,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    }
                },{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': -1,
                    varint: -4503599627370496,
                    decimal: '1.2',
                    //'float': 1.2,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    }
                }]);
            });
        });
        it('drop table', function() {
            this.timeout(15000);
            return router.request({
                url: "/v1/restbase.cassandra.test.local/typeTable",
                method: "delete",
                body: {}
            });
        });
    });

    describe('dropTable', function() {
        this.timeout(15000);
        it('drop a simple table', function() {
            return router.request({
                url: "/v1/restbase.cassandra.test.local/varintTable",
                method: "delete",
                body: {}
            }).then(function() {
                return router.request({
                    url: "/v1/restbase.cassandra.test.local/simpleTable",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    url: "/v1/restbase.cassandra.test.local/multiRangeTable",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    url: "/v1/restbase.cassandra.test.local/simpleSecondaryIndexTable",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    url: "/v1/restbase.cassandra.test.local/unversionedSecondaryIndexTable",
                    method: "delete",
                    body: {}
                });
            });
        });
    });
});
