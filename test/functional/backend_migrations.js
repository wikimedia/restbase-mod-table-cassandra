"use strict";


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
    ]
};

describe('Backend migration', () => {
    var db;
    before(() => {
        return makeClient({
            log: (level, info) => {
                if (!/^info|warn|verbose|debug|trace/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/../utils/test_client.conf.yaml'))
        })
        .then((newDb) => {
            db = newDb;
        })
        .then(() => {
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

    it('persists a backend version', () => {
        return db.getTableSchema('restbase.cassandra.test.local', testTable0.table)
        .then((response) => {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.schema.table, testTable0.table);
            assert.deepEqual(response.schema._backend_version, dbu.CURRENT_BACKEND_VERSION);
        });
    });
});
