"use strict";

if (!global.Promise) {
    global.promise = require('bluebird');
}
var assert = require('assert');
var cass = require('cassandra-driver');
var uuid = require('node-uuid');
var makeClient = require('./index.js');

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

var DB = require('./db');

var dynamoQuery = {
    "TableName": "Thread",
    "IndexName": "LastPostIndex",
    "Select": "ALL_ATTRIBUTES",
    "Limit":3,
    "ConsistentRead": true,
    "KeyConditions": {
        "LastPostDateTime": {
            "AttributeValueList": [
                {
                    "S": "20130101"
                },
                {
                    "S": "20130115"
                }
            ],
            "ComparisonOperator": "BETWEEN"
        },
        "ForumName": {
            "AttributeValueList": [
                {
                    "S": "Amazon DynamoDB"
                }
            ],
            "ComparisonOperator": "EQ"
        }
    },
    "ReturnConsumedCapacity": "TOTAL"
};

// Sample revisioned bucket schema
var revisionedKVSchema = {
    // keep extra redundant info for primary bucket table reconstruction
    domain: 'en.wikipedia.org',
    table: 'someTable',
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
};

// simple schema with more than one range keys
var anotherSimpleSchema = {
    domain: 'en.wikipedia.org',
    table: 'someTable1',
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
};

// simple schema with secondary index
var simpleSchemaWithIndex = {
    domain: 'en.wikipedia.org',
    table: 'someTable2',
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
};

// simple schema with secondary index
var SchemaWithIndexWithoutTID = {
    domain: 'en.wikipedia.org',
    table: 'someTable3',
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
};


var simpleKVSchema = {
    // extra redundant info for primary bucket table reconstruction
    domain: 'en.wikipedia.org',
    table: 'someTable',
    options: { storageClass: 'SimpleStrategy', durabilityLevel: 1 },
    attributes: {
        uri: 'string',
        tid: 'timeuuid',
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
        { attribute: 'uri', type: 'hash' }
    ]
};

var rangeKVSchema = {
    table: 'someTable',
    attributes: {
        prefix: 'string', // fixed or trie prefix, managed by bucket handler
        uri: 'string',
        tid: 'timeuuid',
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
        { attribute: 'prefix', type: 'hash' },
        { attribute: 'uri', type: 'range', order: 'desc' }
    ]
};

// simple insert query
var simplePutQuery = {
    table: "someTable",
    attributes: {
        key: 'testing',
        tid: tidFromDate(new Date('2013-08-08 18:43:58-0700')),
    },
};

// simple insert query to test more than one range keys
var anotherSimplePutQuery = {
    table: "someTable1",
    attributes: {
        key: 'testing',
        tid: tidFromDate(new Date('2013-08-08 18:43:58-0700')),
        uri: "test"
    },
};

// simple update query
var updateQuery = {
    table: "someTable",
    attributes: {
        key: "testing",
        tid: tidFromDate(new Date('2013-08-09 18:43:58-0700')),
        body: "<p>Service Oriented Architecture</p>"
    }
};

// simple insert query using if non exist and non index attributes
var putIfNotExistQuery = {
    table: "someTable",
    if : "not exists",
    attributes: {
        key: "testing if not exists",
        tid: tidFromDate(new Date('2013-08-10 18:43:58-0700')),
        body: "<p>if not exists with non key attr</p>"
    }
};

// simple update query using if conditional
var putIfQuery = {
    table: "someTable",
    attributes: {
        key: "another test",
        tid: tidFromDate(new Date('2013-08-11 18:43:58-0700')),
        body: "<p>test<p>"
    },
    if: { body: { "eq": "<p>Service Oriented Architecture</p>" } }
};

// simple query to test secondary index update functionality
var putIndexQuery = {
    table: "someTable2",
    attributes: {
        key: "another test",
        tid: tidFromDate(new Date('2013-08-11 18:43:58-0700')),
        uri: "a uri"
    },
};

var putIndexQuery2 = {
    table: "someTable2",
    attributes: {
        key: "test",
        tid: tidFromDate(new Date()),
        uri: "another/test/without/timeuuid"
    },
};

var putIndexQuery3 = {
    table: "someTable2",
    attributes: {
        key: "test4",
        tid: tidFromDate(new Date()),
        uri: "test/uri"
    },
};

// simple query to test unversioned secondary indexes
var putIndexQuery4 = {
    table: "someTable3",
    attributes: {
        key: "another test",
        uri: "a uri"
    },
};

var putIndexQuery5 = {
    table: "someTable3",
    attributes: {
        key: "another test",
        uri: "a uri",
        body: "abcd"
    }
};

// simple select query
var simpleQuery = {
    table: "someTable",
    attributes: {
        key: 'testing',
        tid: tidFromDate(new Date('2013-08-08 18:43:58-0700'))
    }
};

// simeple select query using between relation
var betweenQuery = {
    table: "someTable",
    //from: 'foo', // key to start the query from (paging)
    limit: 3,
    attributes: {
        tid: { "BETWEEN": [ tidFromDate(new Date('2013-07-08 18:43:58-0700')),
                            tidFromDate(new Date('2013-08-08 18:43:58-0700'))] },
        key: "testing"
    }
};

// simple index query
var simpleIndexQuery = {
    table: "someTable2",
    index: "by_uri",
    attributes: {
        key: "another test",
        tid: { "le" : tidFromDate(new Date('2013-08-11 18:43:58-0700')) },
        uri: "a uri"
    },
    limit: 1
};

// simple delete query
var deleteQuery = {
    table: "someTable",
    attributes: {
        tid: tidFromDate(new Date('2013-08-09 18:43:58-0700')),
        key: "testing"
    }
};

var ourPutQuery = {
    method: 'put',
    table: "someTable",
    limit: 3,
    // alternative: if: 'EXISTS'
    if: {
        LastPostDateTime: { "BETWEEN": ["20130101", "20130115"] },
        ForumName: { "ne": "Amazon DynamoDB" }
    },
    attributes: {
        LastPostDateTime: 'foo',
        ForumName: 'bar'
    },
    // dependent requests
    then: [
        { /* more dependent requests */ }
    ]
};

var ourQueryResult = {
    status: 200,
    then: [
        {
            /* dependent result */
            status: 200
        }
    ],
    next: {
        /* pred matching next key to scan for paging */
    }
};


var DB;

describe('DB backend', function() {
    this.timeout(15000);
    before(function() { return makeClient({ contactPoints: ['localhost'], keyspace: 'system' })
        .then(function(db) { DB = db; }); });
    describe('createTable', function() {
        it('should create a simple table', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', revisionedKVSchema)
                .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('createTable', function() {
        it('should create a simple table with more than one range keys', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', anotherSimpleSchema)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('createTable', function() {
        it('should create a simple table with secondary index', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', simpleSchemaWithIndex)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('createTable', function() {
        it('should create a simple table with secondary index and no tid in range', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', SchemaWithIndexWithoutTID)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a simple put insert query', function() {
            this.timeout(15000);
            return DB.put('org.wikipedia.en', simplePutQuery)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a simple put insert query on table with more than one range keys', function() {
            this.timeout(15000);
            return DB.put('org.wikipedia.en', anotherSimplePutQuery)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a simple put update query', function() {
            return DB.put('org.wikipedia.en', updateQuery)
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a put query with if not exists and non index attributes', function() {
            return DB.put('org.wikipedia.en', putIfNotExistQuery)
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a put query with if and non index attributes', function() {
            return DB.put('org.wikipedia.en', putIfQuery)
            .then(function(result) {
                deepEqual(result, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a put query to test index update functionality ', function() {
            return Promise.all([DB.put('org.wikipedia.en', putIndexQuery).then(function(result) {deepEqual(result, {status:201});}),
                    DB.put('org.wikipedia.en', putIndexQuery3).then(function(result){deepEqual(result, {status:201});})
                ]);
        });
    });
    describe('put', function() {
        it('should perform a put query to test unversioned index functionality ', function() {
            return DB.put('org.wikipedia.en', putIndexQuery4)
            .then(function(result){
                deepEqual(result, {status:201});
            });
        });
    });
    describe('put', function() {
        it('should perform a put query to test unversioned index update functionality ', function() {
            return DB.put('org.wikipedia.en', putIndexQuery5)
            .then(function(result){
                deepEqual(result, {status:201});
            });
        });
    });
    describe('get', function() {
        it('should perform a simple get query', function() {
            return DB.get('org.wikipedia.en', betweenQuery)
            .then(function(result) {
                deepEqual(result.count, 1);
            });
        });
    });
    describe('get', function() {
        it('should perform a update query', function() {
            return DB.get('org.wikipedia.en', simpleQuery)
            .then(function(result) {
                deepEqual(result.count, 1);
            });
        });
    });
    describe('get', function() {
        it('should perform a index query', function() {
            return Promise.all([DB.get('org.wikipedia.en', simpleIndexQuery).then(function(result){deepEqual(result.count, 1);}),
                DB.get('org.wikipedia.en', simpleIndexQuery).then(function(result){deepEqual(result.count, 1);}),
                DB.get('org.wikipedia.en', simpleIndexQuery).then(function(result){deepEqual(result.count, 1);})]);
        });
    });
    describe('delete', function() {
        it('should perform a simple delete query', function() {
            return DB.delete('org.wikipedia.en', deleteQuery);
        });
    });
    describe('dropTable', function() {
        it('should drop a simple table', function() {
            this.timeout(15000);
            return Promise.all([ DB.dropTable('org.wikipedia.en', 'someTable'),
                                 DB.dropTable('org.wikipedia.en', 'someTable1'),
                                 DB.dropTable('org.wikipedia.en', 'someTable2'),
                                 DB.dropTable('org.wikipedia.en', 'someTable3')]);
        });
    });
});
