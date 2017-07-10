"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var dbu = require('../../lib/dbutils');
var fs = require('fs');
var makeClient = require('../../lib/index');
var yaml = require('js-yaml');

var testTable0 = {
    table: 'config-versioning',
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
    ]
};

describe('Configuration migration', function() {
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
        return db.dropTable('restbase.cassandra.test.local', testTable0.table);
    });

    it('migrates version', function() {
        return db.getTableSchema('restbase.cassandra.test.local', testTable0.table)
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.schema.table, testTable0.table);
            assert.deepEqual(response.schema._config_version, 1);

            db.conf.version = 2;
            return db.createTable('restbase.cassandra.test.local', testTable0);
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);

            return db.getTableSchema('restbase.cassandra.test.local', testTable0.table);
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.schema.table, testTable0.table);
            assert.deepEqual(response.schema._config_version, 2);
        });
    });

    it('disallows decreasing versions', function() {
        // Migrate version (from 1) to 2.
        db.conf.version = 2;
        return db.createTable('restbase.cassandra.test.local', testTable0)
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);

            // Attempt to downgrade version (from 2) to 1
            db.conf.version = 1;
            return db.createTable('restbase.cassandra.test.local', testTable0)
            .then(function(response) {
                // A successful response means a downgrade happened (this is wrong).
                assert.fail(response, undefined, 'expected HTTPError exception');
            })
            .catch(function(error) {
                // This is what we want, an HTTPError and status 400.
                assert.deepEqual(error.status, 400);
            });
        });
    });
});
