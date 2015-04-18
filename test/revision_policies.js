"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var cass = require('cassandra-driver');
var dbu = require('../lib/dbutils.js');
var fs = require('fs');
var makeClient = require('../lib/index');
var TimeUuid = cass.types.TimeUuid;
var yaml = require('js-yaml');

var testSchema = {
    table: 'revPolicyLatestTest',
    options: { durability: 'low' },
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        comment: 'string',
        author: 'string'
    },
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ],
    secondaryIndexes: {
        by_rev : [
            { attribute: 'rev', type: 'hash' },
            { attribute: 'tid', type: 'range', order: 'desc' },
            { attribute: 'title', type: 'range', order: 'asc' },
            { attribute: 'comment', type: 'proj' }
        ]
    },
    revisionRetentionPolicy: {
        type: 'latest',
        count: 2,
        grace_ttl: 10
    }
};

describe('MVCC revision policies', function() {
    var db;
    before(function() {
        return makeClient({
            log: function(level, info) {
                if (/^error|fatal/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/test_router.conf.yaml'))
        })
        .then(function(newDb) {
            db = newDb;
        })
        .then(function() {
            return db.createTable("domains_test", testSchema)
            .then(function(response) {
                assert.deepEqual(response.status, 201);
            });
        });
    });

    after(function() {
        return db.dropTable("domains_test", 'revPolicyLatestTest');
    });

    it('sets a TTL on all but the latest N entries', function() {
        this.timeout(12000);
        return db.put('domains_test', {
            table: 'revPolicyLatestTest',
            consistency: 'localQuorum',
            attributes: {
                title: 'Revisioned',
                rev: 1000,
                tid: dbu.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                comment: 'once',
                author: 'jsmith'
            }
        })
        .then(function(response) {
            assert.deepEqual(response.status, 201);
        })
        .then(function() {
            return db.put('domains_test', {
                table: 'revPolicyLatestTest',
                consistency: 'localQuorum',
                attributes: {
                    title: 'Revisioned',
                    rev: 1000,
                    tid: dbu.testTidFromDate(new Date("2015-04-01 12:00:01-0500")),
                    comment: 'twice',
                    author: 'jsmith'
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response, {status:201});

            return db.put('domains_test', {
                table: 'revPolicyLatestTest',
                consistency: 'localQuorum',
                attributes: {
                    title: 'Revisioned',
                    rev: 1000,
                    tid: dbu.testTidFromDate(new Date("2015-04-01 12:00:02-0500")),
                    comment: 'thrice',
                    author: 'jsmith'
                }
            });
        })
        // Delay long enough for the background updates to complete, then
        // for the grace_ttl to expire.
        .delay(11000)
        .then(function(response) {
            assert.deepEqual(response, {status: 201});

            return db.get('domains_test', {
                table: 'revPolicyLatestTest',
                attributes: {
                    title: 'Revisioned',
                    rev: 1000,
                },
            });
        })
        .then(function(response) {
            assert.ok(response);
            assert.ok(response.items);
            assert.deepEqual(response.items.length, 2);
        });
    });

    // XXX: Consider moving to a file of dbutils unit tests?
    it('rejects invalid revision retention policy schema', function() {
        var policies = [
            {
                type: 'bogus'    // Invalid
            },
            {
                type: 'latest',
                count: 1,
                grace_ttl: 5     // Invalid
            },
            {
                type: 'latest',
                count: 0,        // Invalid
                grace_ttl: 86400
            }
        ];

        policies.forEach(function(policy) {
            var schema = { revisionRetentionPolicy: policy };
            assert.throws(
                dbu.validateAndNormalizeRevPolicy.bind(null, schema),
                Error,
                'Validated an invalid schema');
        });
    });

    it('defaults to retention \'all\'', function() {
        var schemaInfo = dbu.makeSchemaInfo(
                dbu.validateAndNormalizeSchema({
                        attributes: {
                            foo: 'int'
                        },
                        index: [
                            { attribute: 'foo', type: 'hash' }
                        ]
                    }));
        assert.deepEqual('all', schemaInfo.revisionRetentionPolicy.type);
    });
});

