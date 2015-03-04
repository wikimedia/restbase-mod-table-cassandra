"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var cass = require('cassandra-driver');
var Uuid = cass.types.Uuid;
var TimeUuid = cass.types.TimeUuid;
var Integer = cass.types.Integer;
var BigDecimal = cass.types.BigDecimal;
var makeClient = require('../lib/index');
var dbu = require('../lib/dbutils.js');
//TODO: change this name
var router = require('../test/test_router.js');
var fs = require('fs');
var yaml = require('js-yaml');


function deepEqual (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + JSON.stringify(expected, null, 2));
        console.log('Result:\n' + JSON.stringify(result, null, 2));
        throw e;
    }
}

function roundDecimal(item) {
    return Math.round( item * 100) / 100;
}

describe('DB backend', function() {
    var db;
    before(function() {
        return makeClient({
            log: function(level, info) {
                if (!/^info|verbose|debug|trace/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/test_router.conf.yaml'))
        })
        .then(function(newDb) {
            db = newDb;
            return router.makeRouter();
        });
    });
    describe('createTable', function() {
        this.timeout(15000);
        it('varint table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable',
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
        it('throws error on unsupported schema update request', function() {
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
        it('table with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
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
                uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable',
                method: 'put',
                body: {
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
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable',
                method: 'put',
                body: {
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
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    consistency: 'localQuorum',
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('simple put insert query on table with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'put',
                body: {
                    table: "multiRangeTable",
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
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
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: "testing",
                        tid: dbu.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
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
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: "simple-table",
                        if : "not exists",
                        attributes: {
                            key: "testing if not exists",
                            tid: dbu.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
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
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "another test",
                        tid: dbu.testTidFromDate(new Date('2013-08-11 18:43:58-0700')),
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
                uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test",
                        tid: TimeUuid.now(),
                        uri: "uri1",
                        body: 'body1'
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: TimeUuid.now(),
                            uri: "uri2",
                            body: 'body2'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: TimeUuid.now(),
                            uri: "uri3",
                            body: 'body3'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now(),
                            uri: "uri1",
                            body: 'test_body1'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now(),
                            uri: "uri2",
                            body: 'test_body2'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now(),
                            uri: "uri3",
                            body: 'test_body3'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now(),
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
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
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
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
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
        it('try a put on a non existing table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unknownTable/',
                method: 'put',
                body: {
                    table: 'unknownTable',
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 500);
            });
        });
    });

    describe('get', function() {
        it('varint predicates', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'put',
                body: {
                    table: 'varintTable',
                    consistency: 'localQuorum',
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
                    uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
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
                    uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
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
                    uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
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
                    uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
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
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    //from: 'foo', // key to start the query from (paging)
                    limit: 3,
                    attributes: {
                        tid: { "BETWEEN": [ dbu.testTidFromDate(new Date('2013-07-08 18:43:58-0700')),
                        dbu.testTidFromDate(new Date('2013-08-08 18:45:58-0700'))] },
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
        it('simple get', function() {
            return router.request({
                uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
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
        //it('simple get with paging', function() {
        //    return router.request({
        //        uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
        //        method: 'get',
        //        body: {
        //            table: "simple-table",
        //            pageSize: 1,
        //            attributes: {
        //                key: 'testing',
        //            }
        //        }
        //    })
        //    .then(function(response) {
        //        deepEqual(response.body.items.length, 1);
        //        return router.request({
        //            uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
        //            method: 'get',
        //            body: {
        //                table: "simple-table",
        //                pageSize: 1,
        //                next: response.body.next,
        //                attributes: {
        //                    key: 'testing',
        //                }
        //            }
        //        });
        //    })
        //    .then(function(response) {
        //        deepEqual(response.body.items[0], { key: 'testing',
        //            tid: '28730300-0095-11e3-9234-0123456789ab',
        //            latestTid: null,
        //            body: null,
        //            'content-length': null,
        //            'content-location': null,
        //            'content-sha256': null,
        //            'content-type': null,
        //            restrictions: null
        //        });
        //    });
        //});
        it("index query for values that doesn't match any more", function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
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
                    uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
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
                uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
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
        it('try a get on a non existing table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unknownTable/',
                method: 'get',
                body: {
                    table: 'unknownTable',
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 500);
            });
        });
    });
    //TODO: implement this using http handler when alternate rest-url for delete item are supported
    describe('delete', function() {
        it('simple delete query', function() {
            return db.delete('restbase.cassandra.test.local', {
                table: "simple-table",
                attributes: {
                    tid: dbu.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                    key: "testing"
                }
            });
        });
    });

    describe('types', function() {
        this.timeout(5000);
        it('create table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable',
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
                        'float': 'float',
                        'double': 'double',
                        'boolean': 'boolean',
                        timeuuid: 'timeuuid',
                        uuid: 'uuid',
                        timestamp: 'timestamp',
                        json: 'json',
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ],
                    secondaryIndexes: {
                        test: [
                            { attribute: 'int', type: 'hash' },
                            { attribute: 'string', type: 'range' },
                            { attribute: 'boolean', type: 'range' }
                        ]
                    }
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('create sets table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeSetsTable',
                    options: { durability: 'low' },
                    attributes: {
                        string: 'string',
                        set: 'set<string>',
                        blob: 'set<blob>',
                        'int': 'set<int>',
                        varint: 'set<varint>',
                        decimal: 'set<decimal>',
                        'float': 'set<float>',
                        'double': 'set<double>',
                        'boolean': 'set<boolean>',
                        timeuuid: 'set<timeuuid>',
                        uuid: 'set<uuid>',
                        timestamp: 'set<timestamp>',
                        json: 'set<json>',
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
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
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
                        'float': -1.1,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('put 2', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
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
                        'float': -3.434,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('put typeSetsTable, nulls and equivalents', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'put',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'nulls',
                        set: [],
                        blob: [],
                        'int': [],
                        varint: null
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('get typeSetsTable, nulls and equivalents', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'get',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'nulls'
                    }
                }
            })
            .then(function(res) {
                deepEqual(res.body.items[0].string, 'nulls');
                deepEqual(res.body.items[0].blob, null);
            });
        });
        it('put sets', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'put',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'string',
                        blob: [new Buffer('blob')],
                        set: ['bar','baz','foo'],
                        varint: [-4503599627370496,12233232],
                        decimal: ['1.2','1.6'],
                        'float': [1.3, 1.1],
                        'double': [1.2, 1.567],
                        'boolean': [true, false],
                        timeuuid: ['c931ec94-6c31-11e4-b6d0-0f67e29867e0'],
                        uuid: ['d6938370-c996-4def-96fb-6af7ba9b6f72'],
                        timestamp: ['2014-11-14T19:10:40.912Z', '2014-12-14T19:10:40.912Z'],
                        'int': [123456, 2567, 598765],
                        json: [
                            {one: 1, two: 'two'},
                            {foo: 'bar'},
                            {test: [{a: 'b'}, 3]}
                        ]
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it("get typeTable", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string'
                    },
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'float', 'double','boolean','timeuuid','uuid',
                            'timestamp','json']
                }
            })
            .then(function(response){
                response.body.items[0].float = roundDecimal(response.body.items[0].float);
                response.body.items[1].float = roundDecimal(response.body.items[1].float);
                deepEqual(response.body.items, [{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': 1,
                    varint: 1,
                    decimal: '1.4',
                    'float': -3.43,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    },
                },{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': -1,
                    varint: -4503599627370496,
                    decimal: '1.2',
                    'float': -1.1,
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
        it("get typeTable index", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    attributes: {
                        int: '1'
                    },
                    index: 'test',
                    proj: ['int', 'boolean']
                }
            })
            .then(function(response){
                response.body.items[0].int = 1;
                response.body.items[0].boolean = true;
            });
        });
        it("get sets", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'get',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'string'
                    },
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'double','boolean','timeuuid','uuid', 'float',
                            'timestamp','json']
                }
            })
            .then(function(response){
                // note: Cassandra orders sets, so the expected rows are
                // slightly different than the original, supplied ones
                response.body.items[0].float = [roundDecimal(response.body.items[0].float[0]),
                                                roundDecimal(response.body.items[0].float[1])];
                deepEqual(response.body.items[0], {
                    string: 'string',
                    blob: [new Buffer('blob')],
                    set: ['bar','baz','foo'],
                    'int': [2567, 123456, 598765],
                    varint: [
                        -4503599627370496,
                        12233232
                    ],
                    decimal: [
                        '1.2',
                        '1.6'
                    ],
                    'double': [1.2, 1.567],
                    'boolean': [false, true],
                    timeuuid: ['c931ec94-6c31-11e4-b6d0-0f67e29867e0'],
                    uuid: ['d6938370-c996-4def-96fb-6af7ba9b6f72'],
                    'float': [1.1, 1.3],
                    timestamp: ['2014-11-14T19:10:40.912Z', '2014-12-14T19:10:40.912Z'],
                    json: [
                        {foo: 'bar'},
                        {one: 1, two: 'two'},
                        {test: [{a: 'b'}, 3]}
                    ]
                });
            });
        });
        it('drop tables', function() {
            this.timeout(15000);
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/typeTable",
                method: "delete",
                body: {}
            }).then(function() {
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/typeSetsTable",
                    method: "delete",
                    body: {}
                });
            });
        });
    });

    describe('dropTable', function() {
        this.timeout(15000);
        it('drop some simple table', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/varintTable",
                method: "delete",
                body: {}
            }).then(function() {
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simple-table",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/multiRangeTable",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable",
                    method: "delete",
                    body: {}
                });
            }).then(function() {
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable",
                    method: "delete",
                    body: {}
                });
            });
        });
    });
});
