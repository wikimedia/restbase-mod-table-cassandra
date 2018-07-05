"use strict";

const yaml = require('js-yaml');
const fs = require("fs");

require('mocha-eslint')([
    'lib',
    'index.js'
]);


describe('Functional', () => {
    const conf = yaml.safeLoad(fs.readFileSync(`${__dirname}/utils/test_client.conf.yaml`));
    const dbConstructor = require('../index.js');
    require('restbase-mod-table-spec').test(() => dbConstructor({
        conf: conf,
        log: function () {
        }
    }));
});
