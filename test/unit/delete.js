"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var deepEqual = require('../utils/test_utils.js').deepEqual;
var utils = require('../utils/test_utils.js');
var fs = require('fs');
var yaml = require('js-yaml');
var makeDB = require('../../lib/index.js');
var db;

describe('Delete', function() {
    before(function() {
        var defautOpts = {
            log: function(level, info) {
                if (!/^info|verbose|debug|trace|warn/.test(level)) {
                    console.log(level, info);
                }
            },
            conf: yaml.safeLoad(fs.readFileSync(__dirname + '/../utils/test_router.conf.yaml'))
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
            });
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
});