"use strict";

var assert = require('assert');
var TimeUuid = require('cassandra-uuid').TimeUuid;

var testUtils = {};

testUtils.deepEqual = function (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + JSON.stringify(expected, null, 2));
        console.log('Result:\n' + JSON.stringify(result, null, 2));
        throw e;
    }
};

testUtils.roundDecimal = function (item) {
    return Math.round( item * 100) / 100;
};

testUtils.testTidFromDate = function testTidFromDate(date, useCassTicks) {
    var tidNode = new Buffer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]);
    var tidClock = new Buffer([0x12, 0x34]);
    return new TimeUuid(date, useCassTicks ? null : 0, tidNode, tidClock).toString();
};

module.exports = testUtils;
