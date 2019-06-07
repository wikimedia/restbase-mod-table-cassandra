"use strict";

var assert = require('assert');
var dbu = require('../../lib/dbutils');
var fs = require('fs');
var makeClient = require('../../lib/index');
var yaml = require('js-yaml');

var testTable0 = {
    table: 'replicationTest',
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
};

describe('Table creation', () => {
    var db;
    before(() => {
        return makeClient({
            log: (level, info) => {
                if (/^error|fatal/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/../utils/test_client.conf.yaml'))
        })
        .then((newDb) => {
            db = newDb;
            return db.createTable('restbase.cassandra.test.local', testTable0);
        })
        .then((response) => {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
        });
    });
    after(() => {
        db.dropTable('restbase.cassandra.test.local', testTable0.table);
    });

    it('updates Cassandra replication', () => {
        const keyspace = db.keyspaceName('restbase.cassandra.test.local', testTable0.table);
        return db._getReplication(keyspace)
        .then((response) => {
            assert.ok(response, 'undefined response');
            assert.strictEqual(Object.keys(response).length, 1, 'incorrect number of results');

            // Add one datacenter, and update.
            db.conf.datacenters.push('new_dc');
            return db.updateReplicationIfNecessary(keyspace);
        })
        .then(() => {
            return db._getReplication(keyspace);
        })
        .then((response) => {
            assert.ok(response, 'undefined response');
            assert.strictEqual(Object.keys(response).length, 2, 'incorrect number of results');
            assert.ok(response.new_dc, 'additional datacenter not present');
            assert.strictEqual(response.new_dc, 3, 'incorrect replica count');
        });
    });
});
