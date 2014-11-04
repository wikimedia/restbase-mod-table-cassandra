"use strict";

if (!global.Promise) {
    global.Promise = require('bluebird');
}
if (!Promise.promisify) {
    Promise.promisify = require('bluebird').promisify;
}

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var cass = require('cassandra-driver');
var uuid = require('node-uuid');
var makeClient = require('../lib/index');

function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
}

function deepEqual (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + expected);
        console.log('Result:\n' + result);
        throw e;
    }
}

// FIXME: Use the REST interface!
var DB = require('../lib/db.js');

describe('DB backend', function() {
    before(function() {
        return makeClient({
            contactPoints: ['localhost'],
            keyspace: 'system'
        })
        .then(function(db) { DB = db; });
    });

    describe('createTable', function() {
        this.timeout(15000);
        it('simple table', function() {
            return DB.createTable('org.wikipedia.en', {
                // keep extra redundant info for primary bucket table reconstruction
                domain: 'en.wikipedia.org',
                table: 'simpleTable',
                options: { storageClass: 'SimpleStrategy', durabilityLevel: 1 },
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
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
        it('table with more than one range keys', function() {
            return DB.createTable('org.wikipedia.en', {
                domain: 'en.wikipedia.org',
                table: 'multiRangeTable',
                options: { storageClass: 'SimpleStrategy', durabilityLevel: 1 },
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
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
        it('table with secondary index', function() {
            return DB.createTable('org.wikipedia.en', {
                domain: 'en.wikipedia.org',
                table: 'simpleSecondaryIndexTable',
                options: { storageClass: 'SimpleStrategy', durabilityLevel: 1 },
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
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
        it('table with secondary index and no tid in range', function() {
            return DB.createTable('org.wikipedia.en', {
                domain: 'en.wikipedia.org',
                table: 'unversionedSecondaryIndexTable',
                options: { storageClass: 'SimpleStrategy', durabilityLevel: 1 },
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
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });

    describe('put', function() {
        it('simple put insert', function() {
            return DB.put('org.wikipedia.en', {
                table: 'simpleTable',
                attributes: {
                    key: 'testing',
                    tid: tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                },
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
        it('simple put insert query on table with more than one range keys', function() {
            return DB.put('org.wikipedia.en', {
                table: "multiRangeTable",
                attributes: {
                    key: 'testing',
                    tid: tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    uri: "test"
                },
            })
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
        it('simple put update', function() {
            return DB.put('org.wikipedia.en', {
                table: 'simpleTable',
                attributes: {
                    key: "testing",
                    tid: tidFromDate(new Date('2013-08-09 18:43:58-0700')),
                    body: "<p>Service Oriented Architecture</p>"
                }
            })
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
        it('put with if not exists and non index attributes', function() {
            return DB.put('org.wikipedia.en', {
                table: "simpleTable",
                if : "not exists",
                attributes: {
                    key: "testing if not exists",
                    tid: tidFromDate(new Date('2013-08-10 18:43:58-0700')),
                    body: "<p>if not exists with non key attr</p>"
                }
            })
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
        it('put with if and non index attributes', function() {
            return DB.put('org.wikipedia.en', {
                table: "simpleTable",
                attributes: {
                    key: "another test",
                    tid: tidFromDate(new Date('2013-08-11 18:43:58-0700')),
                    body: "<p>test<p>"
                },
                if: { body: { "eq": "<p>Service Oriented Architecture</p>" } }
            })
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
        it('index update', function() {
            return DB.put('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                attributes: {
                    key: "test",
                    tid: uuid.v1(),
                    uri: "uri1",
                    body: 'body1'
                },
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                attributes: {
                    key: "test",
                    tid: uuid.v1(),
                    uri: "uri2",
                    body: 'body2'
                },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test",
                        tid: uuid.v1(),
                        uri: "uri3",
                        body: 'body3'
                    },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test2",
                        tid: uuid.v1(),
                        uri: "uri1",
                        body: 'test_body1'
                    },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                attributes: {
                    key: "test2",
                    tid: uuid.v1(),
                    uri: "uri2",
                    body: 'test_body2'
                },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test2",
                        tid: uuid.v1(),
                        uri: "uri3",
                        body: 'test_body3'
                    },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});

                return DB.put('org.wikipedia.en', {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test2",
                        tid: uuid.v1(),
                        uri: "uri3",
                        // Also test projection updates
                        body: 'test_body3_modified'
                    },
                });
            })
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
        it('unversioned index', function() {
            return DB.put('org.wikipedia.en', {
                table: "unversionedSecondaryIndexTable",
                attributes: {
                    key: "another test",
                    uri: "a uri"
                },
            })
            .then(function(result){
                deepEqual(result, {status:201});
            });
        });
        it('unversioned index update', function() {
            return DB.put('org.wikipedia.en', {
                table: "unversionedSecondaryIndexTable",
                attributes: {
                    key: "another test",
                    uri: "a uri",
                    body: "abcd"
                }
            })
            .then(function(result){
                deepEqual(result, {status:201});
            });
        });
    });

    describe('get', function() {
        it('simple between', function() {
            return DB.get('org.wikipedia.en', {
                table: "simpleTable",
                //from: 'foo', // key to start the query from (paging)
                limit: 3,
                attributes: {
                    tid: { "BETWEEN": [ tidFromDate(new Date('2013-07-08 18:43:58-0700')),
                        tidFromDate(new Date('2013-08-08 18:43:58-0700'))] },
                    key: "testing"
                }
            })
            .then(function(result) {
                deepEqual(result.count, 1);
                deepEqual(result.items, [{ key: 'testing',
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
            return DB.get('org.wikipedia.en', {
                table: "simpleTable",
                attributes: {
                    key: 'testing',
                    tid: tidFromDate(new Date('2013-08-08 18:43:58-0700'))
                }
            })
            .then(function(result) {
                deepEqual(result.count, 1);
                deepEqual(result.items, [ { key: 'testing',
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
            return DB.get('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                index: "by_uri",
                attributes: {
                    uri: "uri1"
                }
            })
            .then(function(result){
                deepEqual(result.items.length, 0);

                return DB.get('org.wikipedia.en', {
                    table: "simpleSecondaryIndexTable",
                    index: "by_uri",
                    attributes: {
                        uri: "uri2"
                    }
                });
            })
            .then(function(result){
                deepEqual(result.items.length, 0);
            });
        });

        it("index query for current value", function() {
            return DB.get('org.wikipedia.en', {
                table: "simpleSecondaryIndexTable",
                index: "by_uri",
                attributes: {
                    uri: "uri3"
                },
                proj: ['key', 'uri', 'body']
            })
            .then(function(result){
                deepEqual(result.items, [{
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
    describe('delete', function() {
        it('simple delete query', function() {
            return DB.delete('org.wikipedia.en', {
                table: "simpleTable",
                attributes: {
                    tid: tidFromDate(new Date('2013-08-09 18:43:58-0700')),
                    key: "testing"
                }
            });
        });
    });

    describe('dropTable', function() {
        it('drop a simple table', function() {
            this.timeout(15000);
            return Promise.all([
                DB.dropTable('org.wikipedia.en', 'simpleTable'),
                DB.dropTable('org.wikipedia.en', 'multiRangeTable'),
                DB.dropTable('org.wikipedia.en', 'simpleSecondaryIndexTable'),
                DB.dropTable('org.wikipedia.en', 'unversionedSecondaryIndexTable')
            ]);
        });
    });
});
