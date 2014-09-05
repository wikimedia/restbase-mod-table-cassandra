"use strict";

require('prfun');
var assert = require('assert');
var cass = require('node-cassandra-cql');
var uuid = require('node-uuid');

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
    index: {
        hash: 'key',
        range: 'tid',
        static: 'latestTid'
    }
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
    index: {
        hash: 'key',
        range: ['tid', 'uri'],
        static: 'latestTid'
    }
};

var simpleKVSchema = {
    // extra redundant info for primary bucket table reconstruction
    domain: 'en.wikipedia.org',
    table: 'someTable',
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
    index: {
        hash: 'uri'
    }
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
    index: {
        hash: 'prefix',
        range: 'uri'
    }
};

// simple insert query
var simplePutQuery = {
    table: "someTable",
    limit: 3,
    attributes: {
        key: 'testing',
        tid: tidFromDate(new Date('2013-08-08 18:43:58-0700')),
    },
};

// simple insert query to test more than one range keys
var anotherSimplePutQuery = {
    table: "someTable1",
    limit: 3,
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
    //index: "key",
    //from: 'foo', // key to start the query from (paging)
    proj: ["key", "tid"],
    limit: 3,
    attributes: {
        tid: { "BETWEEN": [ tidFromDate(new Date('2013-07-08 18:43:58-0700')),
                            tidFromDate(new Date('2013-08-08 18:43:58-0700'))] },
        key: "testing"
    }
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

function promisifyClient (client, options) {
    var methods = ['connect', 'shutdown', 'executeAsPrepared', 'execute', 'executeBatch'];
    methods.forEach(function(method) {
        //console.log(method, client[method]);
        client[method + '_p'] = Promise.promisify(client[method].bind(client));
    });

    return client;
}

function makeClient () {
    var client =  promisifyClient(new cass.Client({hosts: ['localhost'], keyspace: 'system'}));

    var reconnectCB = function(err) {
        if (err) {
            // keep trying each 500ms
            console.error('Cassandra connection error @ localhost :', err, '\nretrying..');
            setTimeout(client.connect.bind(client, reconnectCB), 500);
        }
    };
    client.on('connection', reconnectCB);
    client.connect();
    return new DB(client);
}

var DB = makeClient();

// light-weight transactions
// secondary index tests

describe('DB backend', function() {
    describe('createTable', function() {
        it('should create a simple table', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', revisionedKVSchema)
            .then(function(item) {
                deepEqual(item, {status:201});
            });
        });
    });anotherSimpleSchema
    describe('createTable', function() {
        it('should create a simple table with more than one range keys', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', anotherSimpleSchema)
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
    describe('delete', function() {
        it('should perform a simple delete query', function() {
            return DB.delete('org.wikipedia.en', deleteQuery);
        });
    });
    describe('dropTable', function() {
        it('should drop a simple table', function() {
            this.timeout(15000);
            return DB.dropTable('org.wikipedia.en', 'someTable');
        });
    });
});
