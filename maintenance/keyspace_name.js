"use strict";


var getConfig = require('./lib/index').getConfig;
var DB        = require('../lib/db');

var yargs = require('yargs')
    .usage('Usage: $0 [-c YAML] -d DOMAIN -t TABLE\n\n' +
           'Output Cassandra keyspace name for a given domain and table')
    .demand(['domain', 'table'])
    .options('h', {alias: 'help'})
    .options('d', {
        alias: 'domain',
        describe: 'Domain to match with storage group',
        type: 'string',
    })
    .options('t', {
        alias: 'table',
        describe: 'Logical table name (e.g. parsoid.html)',
        type: 'string',
    })
    .options('c', {
        alias: 'config',
        describe: 'RESTBase configuration file',
        type: 'string',
    });

var argv = yargs.argv;

if (argv.h) {
    yargs.showHelp();
    process.exit(0);
}

var conf = getConfig(argv.config);
var db = new DB({}, {conf: conf, log: function(){} });

console.log(db.keyspaceName(argv.domain, argv.table));

process.exit(0);
