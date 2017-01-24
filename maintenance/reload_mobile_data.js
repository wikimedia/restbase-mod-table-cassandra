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
var iterateTables = require('./lib/index').iterateTables;
var makeClient = require('./lib/index').makeClient;
var DB = require('../lib/db');
var dbutil = require('../lib/dbutils');
var path = require('path');


var yargs = require('yargs')
    .usage('Usage: $0 -H HOST -g GROUP [--token TOKEN]\n' +
           'Usage: $0 -H HOST -g GROUP [--title TITLE]\n' +
           'Usage: $0 -H HOST -g GROUP [--pageState STATE]\n\n' +
           'Prune RESTBase Parsoid storage revisions')
    .demand(['host', 'domain'])
    .options('h', {alias: 'help'})
    .options('H', {
        alias: 'host',
        describe: 'Cassandra hostname (contact node)',
        type: 'string'
    })
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

var conf = getConfig();

var client = makeClient({
    host: argv.host,
    credentials: {
        username: conf.username, password: conf.password,
    }
});

var db = new DB(client, {conf: conf, log: console.log});
var oldLeadTable = dbutil.cassID(db.keyspaceName(argv.domain, 'mobileapps.lead')) + '.data';
var newLeadTable = dbutil.cassID(db.keyspaceName(argv.domain, 'mobile-sections-lead')) + '.data';
var oldRemainingTable = dbutil.cassID(db.keyspaceName(argv.domain, 'mobileapps.remaining')) + '.data';
var newRemainingTable = dbutil.cassID(db.keyspaceName(argv.domain, 'mobile-sections-remaining')) + '.data';

log('Old lead table', oldLeadTable);
log('Old remaining table', newLeadTable);
log('New lead table', oldRemainingTable);
log('New remaining table', newRemainingTable);

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
    startOffset.key = argv.title;
} else if (argv.pageState) {
    startOffset.pageState = argv.pageState;
}

function processRow (row) {

    if (row[0] === null && row[1] === null) {
        console.log('Finished');
        process.exit(0);
    }

    // Keep track of our latest token
    startOffset.token = row[0]._token;

    total++;
    if ((total % 500000) === 0) {
        log('Processed', total, 'total entries');
    }
    console.log('Got it');

    function getDefinedProperties(row) {
        return Object.keys(row).filter(function(keyName) {
            return row[keyName] !== null && row[keyName] !== undefined;
        });
    }

    delete row[0]._token;
    delete row[1]._token;

    const lead = getDefinedProperties(row[0]);
    lead.push('rev');
    const remaining = getDefinedProperties(row[1]);
    remaining.push('rev');
    const insertQueries = [
        {
            query: `INSERT INTO ${newLeadTable} ( ${lead.map((prop) => '"' + prop + '"').join(', ')} ) VALUES ( ${lead.map(() => '?').join(', ')});`,
            params: lead.map((prop) => {
                if (prop === 'rev') {
                    return parseInt(JSON.parse(row[0].value).revision);
                }
                return row[0][prop];
            })
        },
        {
            query: `INSERT INTO ${newRemainingTable} ( ${remaining.map((prop) => '"' + prop + '"').join(', ')} ) VALUES ( ${remaining.map(() => '?').join(', ')});`,
            params: remaining.map((prop) => {
                if (prop === 'rev') {
                    return parseInt(JSON.parse(row[0].value).revision);
                }
                return row[1][prop]
            })
        }
    ];

    return client.batchAsync(insertQueries, {
        prepare: true,
        consistency: cassandra.types.consistencies.one
    });
}

return iterateTables(client, [ oldLeadTable, oldRemainingTable ], startOffset,  '"_domain", key, tid, "latestTid", value, "content-type", "content-sha256", "content-location", tags', processRow);
