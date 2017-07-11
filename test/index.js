"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, context, before, beforeEach, after, afterEach */

var yaml = require('js-yaml');
var fs = require("fs");

// Run jshint as part of normal testing
require('mocha-jshint')();
require('mocha-jscs')();
require('mocha-eslint')([
    'lib',
    'index.js'
]);


describe('Functional', function() {
    var conf = yaml.safeLoad(fs.readFileSync(__dirname + '/utils/test_client.conf.yaml'));
    var dbConstructor = require('../index.js');
    require('restbase-mod-table-spec-ng').test(function() {
        return dbConstructor({
            conf: conf,
            log: function() {}
        });
    });
});
