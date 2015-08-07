"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var dbu = require('../../lib/dbutils');
var validator = require('restbase-mod-table-spec').validator;

var testTable0a = {
    domain: 'restbase.cassandra.test.local',
    table: 'testTable0',
    options: { durability: 'low' },
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        comment: 'string',
        author: 'string',
        tags: 'set<string>'
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
        comment: 'string',
        tags: 'set<string>'
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

    it('builds SELECTs with included TTLs', function() {
        var req = {
            keyspace: 'keyspace',
            columnfamily: 'columnfamily',
            domain: 'en.wikipedia.org',
            schema: dbu.makeSchemaInfo(validator.validateAndNormalizeSchema(testTable0a)),
            query: {},
        };
        var statement = dbu.buildGetQuery(req, { withTTLs: true });
        var match = statement.cql.match(/select (.+) from .+$/i);

        assert(match.length === 2, 'result has no matching projection');

        var projs = match[1].split(',').map(function(i) { return i.trim(); });

        var exp = /TTL\((.+)\) as "_ttl_(.+)"/;

        // There should be 8 non-ttl attributes total.
        assert(projs.filter(function(v) { return !exp.test(v); }).length === 8);

        var matching = [];
        projs.filter(function(v) { return exp.test(v); }).forEach(
            function(v) {
                var v1 = v.match(exp)[1];
                var v2 = v.match(exp)[2];
                assert.deepEqual(v1, dbu.cassID(v2));
                matching.push(v2);
            }
        );

        // matching should include _del, author, and comment only; should only
        // include non-index, and non-collection attributes.
        assert(matching.length === 3, 'incorrect number of TTL\'d attributes');
        assert.deepEqual(matching.sort(), ["_del", "author", "comment"]);
    });

    it('builds SELECTS with an included LIMIT', function() {
        var req = {
            keyspace: 'keyspace',
            columnfamily: 'columnfamily',
            domain: 'en.wikipedia.org',
            schema: dbu.makeSchemaInfo(validator.validateAndNormalizeSchema(testTable0a)),
            query: {},
        };
        var cql = dbu.buildGetQuery(req, { limit: 42 }).cql;
        assert(cql.toLowerCase().includes('limit 42'), 'missing limit clause');
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
                validator.validateAndNormalizeSchema.bind(null, schema),
                Error,
                'Validated an invalid schema');
        });
    });

    it('defaults revision retention policy to \'all\'', function() {
        var schemaInfo = dbu.makeSchemaInfo(
                validator.validateAndNormalizeSchema({
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
