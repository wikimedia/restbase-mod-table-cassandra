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
var iterateTable = require('./lib/index').iterateTable;
var makeClient = require('./lib/index').makeClient;
var DB = require('../lib/db');
var dbutil = require('../lib/dbutils');
var yaml = require('js-yaml');
var fs = require('fs');


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

var client = makeClient({
    host: argv.host,
    credentials: {
        username: 'cassandra', password: 'cassandra'
    }
});
var config = process.env.CONFIG || '/etc/restbase/config.yaml';
var confObj = yaml.safeLoad(fs.readFileSync(config));
confObj = confObj.default_project['x-modules'][0].options.table;
var db = new DB(client, {conf: confObj, log: console.log});
var htmlTable = dbutil.cassID(db._keyspaceName(argv.domain, 'parsoid.html')) + '.data';
var dataTable = dbutil.cassID(db._keyspaceName(argv.domain, 'parsoid.data-parsoid')) + '.data';
console.log('html:', htmlTable);
console.log('data:', dataTable);

// Row state, used to make row handling decisions in processRow
var counts = {
    title: 0,
    rev: 0,
    render: 0,
};

var keys = {
    title: null,
    rev: null,
};

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
    // Create a new set of keys
    var newKeys = {
        title: JSON.stringify([row._domain, row.key]),
        rev: JSON.stringify([row._domain, row.key, row.rev])
    };

    // Keep track of our latest token
    startOffset.token = row._token;

    // Diff the keys and update counters
    if (newKeys.title !== keys.title) {
        counts.title = 0;
        counts.rev = 0;
        counts.render = 0;
    } else if (newKeys.rev !==  keys.rev) {
        counts.rev++;
        counts.render = 0;
    } else {
        counts.render++;
    }
    keys = newKeys;

    total++;
    if ((total % 500000) === 0) {
        console.log(new Date() + ': processed ' + total + ' total entries');
    }

    // Thin-out
    if ((counts.rev > 0 && counts.render > 0)
        || (counts.rev === 0 && counts.render > 0
            // Enforce a grace_ttl of 86400
            && (Date.now() - row.tid.getDate()) > 86400000)
        || (counts.rev > 0 && row.tid.getDate() <  Date.parse('2015-12-31T23:59-0000'))) {
        console.log('-- Deleting:', row._token.toString(), row.tid.getDate().toISOString(), keys.rev);

        var delQuery1 = 'DELETE FROM ' + htmlTable + ' WHERE "_domain" = ? AND key = ? AND rev = ? AND tid = ?';
        var delQuery2 = 'DELETE FROM ' + dataTable + ' WHERE "_domain" = ? AND key = ? AND rev = ? AND tid = ?';
        var params = [row._domain, row.key, row.rev, row.tid];
        var delQueries = [
            { query: delQuery1, params: params },
            { query: delQuery2, params: params }
        ];

        return client.batchAsync(delQueries, {
            prepare: true,
            consistency: cassandra.types.consistencies.localQuorum
        });
    }

    // Else: nothing to do
    return P.resolve();
}

return iterateTable(client, htmlTable, startOffset, processRow);
