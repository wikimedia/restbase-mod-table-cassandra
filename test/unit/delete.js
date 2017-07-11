"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

const assert = require('assert');
var utils = require('restbase-mod-table-spec-ng').testUtils;
var fs = require('fs');
var yaml = require('js-yaml');
var makeDB = require('../../lib/index.js');
const P = require('bluebird');
var db;

describe('Delete', function() {
    before(function() {
        var defautOpts = {
            log: function(level, info) {
                if (!/^info|verbose|debug|trace|warn/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/../utils/test_client.conf.yaml'))
        };
        return makeDB(defautOpts)
        .then(function(newdb) {
            db = newdb;
            return newdb.createTable('restbase.cassandra.test.local', {
                table: 'simple-table',
                options: {
                    durability: 'low',
                    compression: [
                        {
                            algorithm: 'deflate',
                            block_size: 256
                        }
                    ]
                },
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
                index: [
                    {attribute: 'key', type: 'hash'},
                    {attribute: 'latestTid', type: 'static'},
                    {attribute: 'tid', type: 'range', order: 'desc'}
                ]
            })
            .then(() => {
                return newdb.createTable('restbase.cassandra.test.local', {
                    table: 'even-simpler-table',
                    options: {
                        durability: 'low',
                        compression: [
                            {
                                algorithm: 'deflate',
                                block_size: 256
                            }
                        ]
                    },
                    attributes: {
                        key: 'string',
                        rev: 'int',
                        value: 'string'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'rev', type: 'range', order: 'desc'}
                    ]
                });
            });
        });
    });

    after(function() {
        return P.map(['simple-table', 'even-simpler-table'], (i) => {
            db.dropTable('restbase.cassandra.test.local', i);
        });
    });

    // TODO: move to functional tests when delete rest endpoint
    // is implemented and dependency from impl db can be removed
    it('deletes row', function() {
        return db.delete('restbase.cassandra.test.local', {
            table: "simple-table",
            attributes: {
                tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                key: "testing"
            }
        });
    });

    it('puts the lotion on its skin', () => {
        return P.map(Array.from(new Array(20), (x, i) => i), (rev) => {
            return db.put('restbase.cassandra.test.local', {
                table: 'even-simpler-table',
                attributes: {
                    key: 'key00',
                    rev: rev,
                    value: `val${rev}`
                }
            });
        })
        .then(() => {
            return db.get('restbase.cassandra.test.local', {
                table: 'even-simpler-table',
                attributes: {
                    key: 'key00'
                }
            });
        })
        .then((res) => {
            assert.strictEqual(res.items.length, 20, 'wrong number of values stored');

            return db.delete('restbase.cassandra.test.local', {
                table: 'even-simpler-table',
                attributes: {
                    key: 'key00',
                    rev: { lt: 10 }
                }
            });
        })
        .then((res) => {
            assert.strictEqual(res.status, 201);

            return db.get('restbase.cassandra.test.local', {
                table: 'even-simpler-table',
                attributes: {
                    key: 'key00'
                }
            });
        })
        .then((res) => {
            assert.strictEqual(res.items.length, 10, 'wrong number of values stored');
            assert.strictEqual(res.items[0].rev, 19, 'incorrect range of values returned');
            assert.strictEqual(res.items[9].rev, 10, 'incorrect range of values returned');
        });
    });
});
