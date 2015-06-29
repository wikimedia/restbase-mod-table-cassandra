"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = require('../utils/test_router.js');
var testU = require('../utils/test_utils.js');

var deepEqual = testU.deepEqual;
var roundDecimal = testU.roundDecimal;

describe('Types', function() {

    before(function () { return router.setup(); });

    context('Standard', function() {
        this.timeout(5000);
        it('creates table with various types', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeTable',
                    options: { durability: 'low' },
                    attributes: {
                        string: 'string',
                        blob: 'blob',
                        set: 'set<string>',
                        'int': 'int',
                        varint: 'varint',
                        decimal: 'decimal',
                        'float': 'float',
                        'double': 'double',
                        'boolean': 'boolean',
                        timeuuid: 'timeuuid',
                        uuid: 'uuid',
                        timestamp: 'timestamp',
                        json: 'json',
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ],
                    secondaryIndexes: {
                        test: [
                            { attribute: 'int', type: 'hash' },
                            { attribute: 'string', type: 'range' },
                            { attribute: 'boolean', type: 'range' }
                        ]
                    }
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('inserts row with various types', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': -1,
                        varint: -4503599627370496,
                        decimal: '1.2',
                        'float': -1.1,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('insert additional rows with various types', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': 1,
                        varint: 1,
                        decimal: '1.4',
                        'float': -3.434,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it("retrieves rows of matching types", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string'
                    },
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'float', 'double','boolean','timeuuid','uuid',
                            'timestamp','json']
                }
            })
            .then(function(response){
                response.body.items[0].float = roundDecimal(response.body.items[0].float);
                response.body.items[1].float = roundDecimal(response.body.items[1].float);
                deepEqual(response.body.items, [{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': 1,
                    varint: 1,
                    decimal: '1.4',
                    'float': -3.43,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    },
                },{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': -1,
                    varint: -4503599627370496,
                    decimal: '1.2',
                    'float': -1.1,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    }
                }]);
            });
        });
        it("retrieves matching types from index", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    attributes: {
                        int: '1'
                    },
                    index: 'test',
                    proj: ['int', 'boolean']
                }
            })
            .then(function(response){
                response.body.items[0].int = 1;
                response.body.items[0].boolean = true;
            });
        });
        it('drops table', function() {
            this.timeout(15000);
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/typeTable",
                method: "delete",
                body: {}
            });
        });
    });

    context('Sets', function() {
        this.timeout(5000);
        it('creates table with various sets types', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeSetsTable',
                    options: { durability: 'low' },
                    attributes: {
                        string: 'string',
                        set: 'set<string>',
                        blob: 'set<blob>',
                        'int': 'set<int>',
                        varint: 'set<varint>',
                        decimal: 'set<decimal>',
                        'float': 'set<float>',
                        'double': 'set<double>',
                        'boolean': 'set<boolean>',
                        timeuuid: 'set<timeuuid>',
                        uuid: 'set<uuid>',
                        timestamp: 'set<timestamp>',
                        json: 'set<json>',
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ]
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('inserts nulls and equivalents', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'put',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'nulls',
                        set: [],
                        blob: [],
                        'int': [],
                        varint: null
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('inserts sets', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'put',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'string',
                        blob: [new Buffer('blob')],
                        set: ['bar','baz','foo'],
                        varint: [-4503599627370496,12233232],
                        decimal: ['1.2','1.6'],
                        'float': [1.3, 1.1],
                        'double': [1.2, 1.567],
                        'boolean': [true, false],
                        timeuuid: ['c931ec94-6c31-11e4-b6d0-0f67e29867e0'],
                        uuid: ['d6938370-c996-4def-96fb-6af7ba9b6f72'],
                        timestamp: ['2014-11-14T19:10:40.912Z', '2014-12-14T19:10:40.912Z'],
                        'int': [123456, 2567, 598765],
                        json: [
                            {one: 1, two: 'two'},
                            {foo: 'bar'},
                            {test: [{a: 'b'}, 3]}
                        ]
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('retrieves nulls and equivalents', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'get',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'nulls'
                    }
                }
            })
            .then(function(res) {
                deepEqual(res.body.items[0].string, 'nulls');
                deepEqual(res.body.items[0].blob, null);
            });
        });
        it("retrieves sets", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'get',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'string'
                    },
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'double','boolean','timeuuid','uuid', 'float',
                            'timestamp','json']
                }
            })
            .then(function(response){
                // note: Cassandra orders sets, so the expected rows are
                // slightly different than the original, supplied ones
                response.body.items[0].float = [roundDecimal(response.body.items[0].float[0]),
                                                roundDecimal(response.body.items[0].float[1])];
                deepEqual(response.body.items[0], {
                    string: 'string',
                    blob: [new Buffer('blob')],
                    set: ['bar','baz','foo'],
                    'int': [2567, 123456, 598765],
                    varint: [
                        -4503599627370496,
                        12233232
                    ],
                    decimal: [
                        '1.2',
                        '1.6'
                    ],
                    'double': [1.2, 1.567],
                    'boolean': [false, true],
                    timeuuid: ['c931ec94-6c31-11e4-b6d0-0f67e29867e0'],
                    uuid: ['d6938370-c996-4def-96fb-6af7ba9b6f72'],
                    'float': [1.1, 1.3],
                    timestamp: ['2014-11-14T19:10:40.912Z', '2014-12-14T19:10:40.912Z'],
                    json: [
                        {foo: 'bar'},
                        {one: 1, two: 'two'},
                        {test: [{a: 'b'}, 3]}
                    ]
                });
            });
        });
        it('drops table', function() {
            this.timeout(15000);
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/typeSetsTable",
                method: "delete",
                body: {}
            });
        });
    });
});
