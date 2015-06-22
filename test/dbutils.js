"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var dbu = require('../lib/dbutils');

var testTable0a = {
    domain: 'restbase.cassandra.test.local',
    table: 'testTable0',
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
    }
};

// Same as testTable0a, but with a different definition ordering.
var testTable0b = {
    table: 'testTable0',
    options: { durability: 'low' },
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        author: 'string',
        comment: 'string'
    },
    domain: 'restbase.cassandra.test.local',
    secondaryIndexes: {
        by_rev : [
            { attribute: 'rev', type: 'hash' },
            { attribute: 'tid', type: 'range', order: 'desc' },
            { attribute: 'title', type: 'range', order: 'asc' },
            { attribute: 'comment', type: 'proj' }
        ]
    },
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ]
};


describe('DB utilities', function() {
    it('generates deterministic hash', function() {
        assert.deepEqual(
            dbu.makeSchemaHash(testTable0a),
            dbu.makeSchemaHash(testTable0b));
    });
});

describe('Schema validation', function() {
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

    it('defaults revision retention policy to \'all\'', function() {
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
