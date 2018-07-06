"use strict";

/*
 * Known Issues:
 *   - The token must be supplied in the form --token=, to prevent yargs from
 *     parsing negative values as a flag.
 *
 */


var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var ctypes = cassandra.types;
var getConfig = require('./lib/index').getConfig;
var iterateTable = require('./lib/index').iterateTable;
var makeClient = require('./lib/index').makeClient;
var DB = require('../lib/db');
var dbutil = require('../lib/dbutils');
var path = require('path');
var mwTitle = require("mediawiki-title");
var preq = require('preq');


var yargs = require('yargs')
.usage('Usage: $0 -H HOST -d domain -u mw_api_uri [--token TOKEN]\n' +
    'Usage: $0 -H HOST -d domain -u mw_api_uri [--title TITLE]\n' +
    'Usage: $0 -H HOST -d domain -u mw_api_uri [--pageState STATE]\n\n' +
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
    describe: 'Domain to thin out',
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
.options('u', {
    alias: 'mwApiUri',
    describe: 'uri for mw api',
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

if (!argv.mwApiUri) {
    yargs.showHelp();
    console.error('Error: you must provide MW API URI');
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
var htmlTable = dbutil.cassID(db.keyspaceName(argv.domain, 'parsoid.html')) + '.data';
var dataTable = dbutil.cassID(db.keyspaceName(argv.domain, 'parsoid.data-parsoid')) + '.data';
var offsetsTable = dbutil.cassID(db.keyspaceName(argv.domain, 'parsoid.section.offsets')) + '.data';

log('HTML table', htmlTable);
log('Data table', dataTable);
log('Offsets table', dataTable);

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

var siteInfo;
function _getSiteInfo() {
    if (!siteInfo) {
        siteInfo = preq.post({
            uri: argv.mwApiUri,
            headers: {
                host: yargs.domain
            },
            body: {
                format: 'json',
                action: 'query',
                meta: 'siteinfo',
                siprop: 'general|namespaces|namespacealiases'
            }
        })
        .then(function (res) {
            return {
                general: {
                    lang: res.body.query.general.lang,
                    legaltitlechars: res.body.query.general.legaltitlechars,
                    case: res.body.query.general.case
                },
                namespaces: res.body.query.namespaces,
                namespacealiases: res.body.query.namespacealiases
            };
        })
        .catch(function(e) {
            console.error('Unable to fetch site info', e);
            process.exit(1);
        });
    }
    return siteInfo;
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
    if ((total % 5000) === 0) {
        log('Processed', total, 'total entries');
    }

    return _getSiteInfo()
    .then(function(siteInfo) {
        if (row.key !== mwTitle.Title.newFromText(row.key, siteInfo).getPrefixedDBKey()) {
            // Delete it
            log('Deleting:', row._token.toString(), row.key);
            var delHtml = 'DELETE FROM ' + htmlTable + ' WHERE "_domain" = ? AND key = ?';
            var delData = 'DELETE FROM ' + dataTable + ' WHERE "_domain" = ? AND key = ?';
            var delOffsets = 'DELETE FROM ' + offsetsTable + ' WHERE "_domain" = ? AND key = ?';
            var params = [row._domain, row.key];
            var delQueries = [
                { query: delHtml, params: params },
                { query: delData, params: params },
                { query: delOffsets, params: params }
            ];

            return client.batchAsync(delQueries, {
                prepare: true,
                consistency: cassandra.types.consistencies.localQuorum
            });
        }
    });
}

return iterateTable(client, htmlTable, startOffset, processRow);
