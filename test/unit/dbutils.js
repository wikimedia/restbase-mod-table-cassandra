"use strict";

var assert = require('assert');
var dbu = require('../../lib/dbutils');

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
    ]
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
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ]
};

describe('DB utilities', () => {

    it('builds SELECTs with included TTLs', () => {
        var req = {
            keyspace: 'keyspace',
            columnfamily: 'columnfamily',
            domain: 'en.wikipedia.org',
            schema: dbu.makeSchemaInfo(dbu.validateAndNormalizeSchema(testTable0a)),
            query: {},
        };
        var statement = dbu.buildGetQuery(req, { withTTL: true });
        var match = statement.cql.match(/select (.+) from .+$/i);

        assert(match.length === 2, 'result has no matching projection');

        var projs = match[1].split(',').map((i) => { return i.trim(); });

        var exp = /TTL\((.+)\) as "_ttl_(.+)"/;

        // There should be 7 non-ttl attributes total.
        assert(projs.filter((v) => { return !exp.test(v); }).length === 7);

        var matching = [];
        projs.filter((v) => { return exp.test(v); }).forEach(
            (v) => {
                var v1 = v.match(exp)[1];
                var v2 = v.match(exp)[2];
                assert.deepEqual(v1, dbu.cassID(v2));
                matching.push(v2);
            }
        );

        // matching should include, author, and comment only; should only
        // include non-index, and non-collection attributes.
        assert(matching.length === 2, 'incorrect number of TTL\'d attributes');
        assert.deepEqual(matching.sort(), ["author", "comment"]);
    });

    it('builds SELECTS with an included LIMIT', () => {
        var req = {
            keyspace: 'keyspace',
            columnfamily: 'columnfamily',
            domain: 'en.wikipedia.org',
            schema: dbu.makeSchemaInfo(dbu.validateAndNormalizeSchema(testTable0a)),
            query: {},
        };
        var cql = dbu.buildGetQuery(req, { limit: 42 }).cql;
        assert(cql.toLowerCase().includes('limit 42'), 'missing limit clause');
    });
});
