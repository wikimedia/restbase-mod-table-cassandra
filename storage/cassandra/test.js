"use strict";

require('prfun');
var assert = require('assert');

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
        hash: 'uri',
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
    table: "Thread",
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
    table: "Thread",
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
var mockClient = {
    // mock client
    execute_p: function(cql, params) {
        var result = {
            query: cql,
            params: params
        };
        results.push(result);
        //console.log(result);
        return Promise.resolve({rows:[]});
    }
};
mockClient.executeAsPrepared_p = mockClient.execute_p;

var testDB = new DB(mockClient);

// A few tests

describe('DB backend', function() {
    describe('createTable', function() {
        it('should create a simple table', function() {
            results = [];
            return testDB.createTable('org.wikipedia.en', revisionedKVSchema)
            .then(function() {
                var expected = [ { query: 'create keyspace "org_wikipedia_en_T_someTable" WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\': 3}',
                        params: [] },
                  { query: 'create table "org_wikipedia_en_T_someTable"."data" ("key" text, "tid" timeuuid, "latestTid" timeuuid static, "body" blob, "content-type" text, "content-length" varint, "content-sha256" text, "content-location" text, "restrictions" set<text>, primary key ("uri","tid")) WITH compaction = { \'class\' : \'LeveledCompactionStrategy\' }',
                          params: [] },
                  { query: 'create table "org_wikipedia_en_T_someTable"."meta" ("key" text, "value" text, primary key ("key")) WITH compaction = { \'class\' : \'LeveledCompactionStrategy\' }',
                          params: [] },
                  { query: 'insert into "org_wikipedia_en_T_someTable"."meta" ("key","value") values (?,?)',
                          params:
                     [ 'schema',
                       '{"domain":"en.wikipedia.org","table":"someTable","attributes":{"key":"string","tid":"timeuuid","latestTid":"timeuuid","body":"blob","content-type":"string","content-length":"varint","content-sha256":"string","content-location":"string","restrictions":"set<string>"},"index":{"hash":"uri","range":"tid","static":"latestTid"}}' ] } ];
                deepEqual(results, expected);
            });
        });
    });
    describe('dropTable', function() {
        it('should drop a simple table', function() {
            results = [];
            return testDB.dropTable('org.wikipedia.en', 'someTable')
            .then(function() {
                var expected = [{
                    query: 'drop keyspace "org_wikipedia_en_T_someTable"',
                    params: []
                }];
                deepEqual(results, expected, results);
            });
        });
    });
    describe('get', function() {
        it('should perform a simple get query', function() {
            results = [];
            return testDB.get('org.wikipedia.en', ourQuery)
            .then(function() {
                var expected = [ { query: 'select * from "org_wikipedia_en_T_Thread"."meta"',
                        params: [] },
                  { query: 'select "all" from "org_wikipedia_en_T_Thread"."i_LastPostIndex" where "LastPostDateTime" >= ? AND "LastPostDateTime" <= ? AND "ForumName" = ? limit 3',
                          params: [ '20130101', '20130115', 'Amazon DynamoDB' ] } ];
                deepEqual(results, expected, results);
            });
        });
    });
    describe('put', function() {
        it('should perform a simple put query', function() {
            results = [];
            return testDB.put('org.wikipedia.en', ourPutQuery)
            .then(function() {
                var expected = [ { query: 'insert into "org_wikipedia_en_T_Thread"."data" ("LastPostDateTime","ForumName") values (?,?) if "LastPostDateTime" >= ? AND "LastPostDateTime" <= ? AND "ForumName" != ?',
    params: [ 'foo', 'bar', '20130101', '20130115', 'Amazon DynamoDB' ] } ];
                deepEqual(results, expected, results);
            });
        });
    });
});
