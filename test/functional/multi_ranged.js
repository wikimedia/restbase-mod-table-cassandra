"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var deepEqual = require('../utils/test_utils.js').deepEqual;
var dbu = require('../../lib/dbutils.js');
var router = require('../utils/test_router.js');

describe("Table operation on Multiranged table", function() {

    before(function () { return router.setup(); });

    context('Create', function() {
        this.timeout(15000);
        it('successfully create table with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'multiRangeTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        restrictions: 'set<string>',
                    },
                    index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'latestTid', type: 'static' },
                    { attribute: 'tid', type: 'range', order: 'desc' },
                        { attribute: 'uri', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
    });

    context('Put', function() {
        it('successfully insert a row with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'put',
                body: {
                    table: "multiRangeTable",
                    attributes: {
                        key: 'testing',
                        tid: dbu.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        uri: "test"
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
    });

    context('Drop', function() {
        this.timeout(15000);
        it('successfully drop table', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/multiRangeTable",
                method: "delete",
                body: {}
            });
        });
    });
});
