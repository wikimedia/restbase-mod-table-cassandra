"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = require('../utils/test_router.js');
var assert = require('assert');
var utils = require('../utils/test_utils.js');

var testSchema = {
    table: 'revPolicyLatestTest',
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
    },
    revisionRetentionPolicy: {
        type: 'latest',
        count: 2,
        grace_ttl: 10
    }
};

describe('MVCC revision policy', function() {
    before(function() {
        return router.setup()
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/' + testSchema.table,
                method: 'put',
                body: testSchema
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
            });
        });
    });

    after(function() {
        return router.request({
            uri: '/domains_test/sys/table/revPolicyLatestTest',
            method: 'delete',
            body: {}
        });
    });

    it('sets a TTL on all but the latest N entries', function() {
        this.timeout(12000);
        return router.request({
            uri: '/domains_test/sys/table/revPolicyLatestTest/',
            method: 'put',
            body: {
                table: 'revPolicyLatestTest',
                consistency: 'localQuorum',
                attributes: {
                    title: 'Revisioned',
                    rev: 1000,
                    tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                    comment: 'once',
                    author: 'jsmith'
                }
            }
        })
        .then(function(response) {
            assert.deepEqual(response.status, 201);
        })
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/revPolicyLatestTest/',
                method: 'put',
                body: {
                    table: 'revPolicyLatestTest',
                    consistency: 'localQuorum',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:01-0500")),
                        comment: 'twice',
                        author: 'jsmith'
                    }
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response, {status:201});

            return router.request({
                uri: '/domains_test/sys/table/revPolicyLatestTest/',
                method: 'put',
                body: {
                    table: 'revPolicyLatestTest',
                    consistency: 'localQuorum',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:02-0500")),
                        comment: 'thrice',
                        author: 'jsmith'
                    }
                }
            });
        })
        // Delay long enough for the background updates to complete, then
        // for the grace_ttl to expire.
        .delay(11000)
        .then(function(response) {
            assert.deepEqual(response, {status: 201});

            return router.request({
                uri: '/domains_test/sys/table/revPolicyLatestTest/',
                method: 'get',
                body: {
                    table: 'revPolicyLatestTest',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response.body);
            assert.ok(response.body.items);
            assert.deepEqual(response.body.items.length, 2);
        });
    });
});

