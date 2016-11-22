"use strict";

/*
 * Known Issues:
 *   - The token must be supplied in the form --token=, to prevent yargs from
 *     parsing negative values as a flag.
 *
 */


var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var consistencies = cassandra.types.consistencies;
var ctypes = cassandra.types;
var getConfig = require('./lib/index').getConfig;
var iterateTable = require('./lib/index').iterateTable;
var makeClient = require('./lib/index').makeClient;
var DB = require('../lib/db');
var dbutil = require('../lib/dbutils');
var path = require('path');


var yargs = require('yargs')
    .usage('Usage: $0 -C path_to_rb_config [--token TOKEN]\n' +
           'Prune RESTBase Parsoid storage revisions')
    .options('h', {alias: 'help'})
    .options('d', {
        alias: 'domain',
        describe: 'Domain that will match to storage group',
        type: 'string',
    })
    .options('t', {
        alias: 'token',
        describe: 'Cassandra token ID to start from',
        type: 'string',
    })
    .options('T', {
        alias: 'title',
        describe: 'Page title to start from',
        type: 'string',
    })
    .options('s', {
        alias: 'pageState',
        describe: 'Driver page state to start from',
        type: 'string',
    })
    .options('U', {
        alias: 'upperBound',
        describe: 'Upper bound timestamp; Only one render of a single revision will be kept for entries older than this value.',
        type: 'string',
        default: '2015-12-31T23:59-0000',
    })
    .options('C', {
        alias: 'config',
        describe: 'Path to RESTBase config',
        type: 'string'
    });

var argv = yargs.argv;

if (argv.h) {
    yargs.showHelp();
    process.exit(0);
}

if ((argv.token && (argv.title || argv.pageState))
    || (argv.title && (argv.token || argv.pageState))
    || (argv.pageState && (argv.token || argv.title))) {
    yargs.showHelp();
    console.error('Error: You can only set one of --token, --title, or --pageState');
    process.exit(1);
}

var upperBound = Date.parse(argv.upperBound);

if (!upperBound) {
    yargs.showHelp();
    console.error('Error: Invalid timestamp for argument -U/--upperBound', argv.upperBound);
    process.exit(1);
}

function log() {
    var varArgs = arguments;
    var logArgs = [];
    logArgs.push('[' + new Date().toISOString() + ']');
    logArgs.push(path.basename(__filename) + '[' + process.pid + ']:');
    Object.keys(varArgs).map(function(v) {
        logArgs.push(varArgs[v]);
    });
    console.log.apply(null, logArgs);
}

var conf = getConfig(argv.config);

var client = makeClient({
    host: conf.hosts[0],
    credentials: {
        username: conf.username, password: conf.password,
    }
});

var db = new DB(client, {conf: conf, log: console.log});
var oldTable = dbutil.cassID(db.keyspaceName(argv.domain, 'restrictions')) + '.data';
var newTable = dbutil.cassID(db.keyspaceName(argv.domain, 'page_restrictions')) + '.data';

log('Old table', oldTable);
log('New table', newTable);

var total = 0;

// Parse optional start offsets
var startOffset = {
    token: null,
    key: null,
    pageState: null,
};

if (argv.token) {
    if (!(/^-?[0-9]{1,30}$/.test(argv.token) && parseInt(argv.token))) {
        yargs.showHelp();
        console.error("Invalid token:", argv.token);
        process.exit(1);
    }
    startOffset.token = ctypes.Long.fromString(argv.token);
} else if (argv.title) {
    startOffset.title = argv.title;
} else if (argv.pageState) {
    startOffset.pageState = argv.pageState;
}

function processRow (row) {
    // Keep track of our latest token
    startOffset.token = row._token;
    delete row._token;


    total++;
    if ((total % 500000) === 0) {
        log('Processed', total, 'total entries');
    }

    var definedProps = Object.keys(row).filter(function(keyName) {
        return row[keyName] !== null && row[keyName] !== undefined;
    });

    var insertQuery = 'INSERT INTO ' + newTable;
    insertQuery += ' (' + definedProps.map(function(prop) {
            return '"' + prop + '"';
        }) + ')';
    insertQuery += ' VALUES ('  + definedProps.map(() => '?').join(', ') + ');';
    // Thin-out
    return client.executeAsync(insertQuery, row, {
        prepare: true,
        consistency: cassandra.types.consistencies.one
    });
}

return iterateTable(client, oldTable, startOffset, processRow);
