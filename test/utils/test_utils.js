"use strict";

var assert = require('assert');
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

module.exports = testUtils;
