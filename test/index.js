"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, context, before, beforeEach, after, afterEach */

var yaml = require('js-yaml');
var fs = require("fs");

// Run jshint as part of normal testing
require('mocha-jshint')();
require('mocha-jscs')();


describe('Functional', function() {
    var conf = yaml.safeLoad(fs.readFileSync(__dirname + '/utils/test_client.conf.yaml'));
    var dbConstructor = require('../index.js');
    require('restbase-mod-table-spec').test(function() {
        return dbConstructor({
            conf: conf,
            log: function() {}
        });
    });
});