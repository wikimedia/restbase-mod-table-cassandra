var deepEqual = require('../utils/test_utils.js').deepEqual;
var dbu = require('../../lib/dbutils.js');
var router = require('../utils/test_router.js');

describe("Db operation on a multiranged table", function() {

    before(function () { return router.setup(); });

    describe('Create table', function() {
        this.timeout(15000);
        it('table with more than one range keys', function() {
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

    describe('Put', function() {
        it('simple put insert query on table with more than one range keys', function() {
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

    describe('drop table', function() {
        this.timeout(15000);
        it('drop table', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/multiRangeTable",
                method: "delete",
                body: {}
            });
        });
    });
});