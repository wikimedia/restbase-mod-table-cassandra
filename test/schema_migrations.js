"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');
var dbu = require('../lib/dbutils');
var extend = require('extend');
var fs = require('fs');
var makeClient = require('../lib/index');
var router = require('./test_router.js');
var yaml = require('js-yaml');

var hash = dbu.makeSchemaHash;

function clone(obj) {
    return extend(true, {}, obj);
}

var testTable0 = {
    domain: 'restbase.cassandra.test.local',
    table: 'testTable0',
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

describe('Schema migration', function() {
    before(function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: testTable0
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);

            router.makeRouter();
        });
    });
    after(function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'DELETE',
            body: {}
        });
    });

    it('migrates revision retention policies', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 2;
        newSchema.revisionRetentionPolicy = {
            type: 'latest',
            count: 5,
            grace_ttl: 86400
        };

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);

            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET',
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(hash(response.body), hash(newSchema));
        });
    });

    it('requires monotonically increasing versions', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 1;
        newSchema.revisionRetentionPolicy = { type: 'all' };

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 400);
            assert.ok(
                /version must be higher/.test(response.body.title),
                'error message looks wrong');
        });
    });

    it('handles column additions', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 3;
        newSchema.attributes.email = 'string';

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);

            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET',
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(hash(response.body), hash(newSchema));
        });
    });

    it('handles column removals', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 4;
        delete newSchema.attributes.author;

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);

            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET',
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(hash(response.body), hash(newSchema));
        });
    });

    it('refuses to remove indexed columns', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 5;
        delete newSchema.attributes.title;

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.deepEqual(response.status, 500);
            assert.ok(
                    /is not in attributes/.test(response.body.stack),
                    'error message looks wrong');
        });
    });
});
