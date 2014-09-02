"use strict";

require('prfun');
var assert = require('assert');
var cass = require('node-cassandra-cql');

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


var ourQuery = {
    table: "someTable",
    index: "LastPostIndex",
    // from: 'foo', // key to start the query from (paging)
    proj: "all",
    limit: 3,
    attributes: {
        LastPostDateTime: { "BETWEEN": ["20130101", "20130115"] },
        ForumName: "Amazon DynamoDB"
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


var results = [];

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

// A few tests

describe('DB backend', function() {
    describe('createTable', function() {
        it('should create a simple table', function() {
            this.timeout(15000);
            return DB.createTable('org.wikipedia.en', revisionedKVSchema)
            .then(function(item) {
                console.log(item);
            });
        });
    });
    describe('dropTable', function() {
        it('should drop a simple table', function() {
            results = [];
            return DB.dropTable('org.wikipedia.en', 'someTable');
        });
    });
    /*
    describe('get', function() {
        it('should perform a simple get query', function() {
            return DB.get('org.wikipedia.en', ourQuery)
            .then(function(item) {
                console.log(item);
                //var expected = [ { query: 'select * from "org_wikipedia_en_T_Thread"."meta"',
                //        params: [] },
                //  { query: 'select "all" from "org_wikipedia_en_T_Thread"."i_LastPostIndex" where "LastPostDateTime" >= ? AND "LastPostDateTime" <= ? AND "ForumName" = ? limit 3',
                //          params: [ '20130101', '20130115', 'Amazon DynamoDB' ] } ];
                //assert.deepEqual(results, expected, results);
            });
        });
    });
    /*
    describe('put', function() {
        it('should perform a simple put query', function() {
            results = [];
            return testDB.put('org.wikipedia.en', ourPutQuery)
            .then(function() {
                var expected = [ { query: 'insert into "org_wikipedia_en_T_Thread"."data" ("LastPostDateTime","ForumName") values (?,?) if "LastPostDateTime" >= ? AND "LastPostDateTime" <= ? AND "ForumName" != ?',
    params: [ 'foo', 'bar', '20130101', '20130115', 'Amazon DynamoDB' ] } ];
                assert.deepEqual(results, expected, results);
            });
        });
    });*/
});
