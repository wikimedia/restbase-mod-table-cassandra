"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = require('../utils/test_router.js');
var utils = require('../utils/test_utils.js');
var deepEqual = utils.deepEqual;

describe("Multiranged tables", function() {
    this.timeout(15000);

    before(function () { return router.setup(); });
    
    it('creates table with more than one range key', function() {
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

    it('inserts a row with more than one range key', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
            method: 'put',
            body: {
                table: "multiRangeTable",
                attributes: {
                    key: 'testing',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    uri: "test"
                },
            }
        })
        .then(function(response) {
            deepEqual(response, {status:201});
        });
    });


    it('drops table', function() {
        return router.request({
            uri: "/restbase.cassandra.test.local/sys/table/multiRangeTable",
            method: "delete",
            body: {}
        });
    });
});
