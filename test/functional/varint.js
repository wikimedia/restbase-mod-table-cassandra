"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = require('../utils/test_router.js');
var deepEqual = require('../utils/test_utils.js').deepEqual;

describe('Varint tables', function() {
    before(function () { return router.setup(); });
    it('creates varint table', function() {
        this.timeout(10000);
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable',
            method: 'put',
            body: {
                // keep extra redundant info for primary bucket table reconstruction
                domain: 'restbase.cassandra.test.local',
                table: 'varintTable',
                options: { durability: 'low' },
                attributes: {
                    key: 'string',
                    rev: 'varint',
                },
                index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'rev', type: 'range', order: 'desc' }
                ]
            }
        })
        .then(function(response) {
            deepEqual(response.status, 201);
        });
    });
    it('retrieves using varint predicates', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'put',
            body: {
                table: 'varintTable',
                consistency: 'localQuorum',
                attributes: {
                    key: 'testing',
                    rev: 1
                }
            }
        })
        .then(function(item) {
            deepEqual(item, {status:201});
        })
        .then(function () {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'put',
                body: {
                    table: 'varintTable',
                    attributes: {
                        key: 'testing',
                        rev: 5
                    }
                }
            });
        })
        .then(function(item) {
            deepEqual(item, {status:201});
        })
        .then(function () {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'get',
                body: {
                    table: 'varintTable',
                    limit: 3,
                    attributes: {
                        key: 'testing',
                        rev: 1
                    }
                }
            });
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
        })
        .then(function () {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'get',
                body: {
                    table: 'varintTable',
                    limit: 3,
                    attributes: {
                        key: 'testing',
                        rev: { gt: 1 }
                    }
                }
            });
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
        })
        .then(function () {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'get',
                body: {
                    table: 'varintTable',
                    limit: 3,
                    attributes: {
                        key: 'testing',
                        rev: { ge: 1 }
                    }
                }
            });
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 2);
        });
    });
    it('drops table', function() {
        this.timeout(15000);
        return router.request({
            uri: "/restbase.cassandra.test.local/sys/table/varintTable",
            method: "delete",
            body: {}
        });
    });
});
