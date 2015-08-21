"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var dbu = require('../../lib/dbutils');
var fs = require('fs');
var makeClient = require('../../lib/index');
var yaml = require('js-yaml');

var testTable0 = {
    table: 'backendVersioning',
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

describe('Backend migration', function() {
    var db;
    before(function() {
        return makeClient({
            log: function(level, info) {
                if (!/^info|warn|verbose|debug|trace/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/../utils/test_client.conf.yaml'))
        })
        .then(function(newDb) {
            db = newDb;
        })
        .then(function() {
            return db.createTable('restbase.cassandra.test.local', testTable0);
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
        });
    });
    after(function() {
        db.dropTable('restbase.cassandra.test.local', testTable0.table);
    });

    it('persists a backend version', function() {
        return db.getTableSchema('restbase.cassandra.test.local', testTable0.table)
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.schema.table, testTable0.table);
            assert.deepEqual(response.schema._backend_version, dbu.CURRENT_BACKEND_VERSION);
        });
    });
});
