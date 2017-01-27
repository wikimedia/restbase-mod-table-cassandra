"use strict";

/**
 * Simple script to delete all but the newest entry per revision in a
 * key_rev_value table.
 */

var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var consistencies = cassandra.types.consistencies;
var ctypes = cassandra.types;
var preq = require('preq');
var iterateTable = require('./lib/index').iterateTable;
var makeClient = require('./lib/index').makeClient;
var dbu = require('../lib/dbutils');

var keyspace = process.argv[3];

if (!keyspace) {
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace>');
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace> [token]');
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace> [<domain> <key>]');
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace> [pageState]');
    process.exit(1);
}

// Force a re-render of a revision by sending no-cache headers. Can be used to
// fix up stored content after temporary snafus in Parsoid or RB.
function reRender(row) {
    var uri = 'http://rest.wikimedia.org/' + row._domain + '/v1/page/html/'
            + encodeURIComponent(row.key) + '/' + row.rev;
    console.log(uri);
    return preq.get({
        uri: uri,
        headers: {
            'if-unmodified-since': 'Fri Apr 24 2015 13:00:00 GMT-0700 (PDT)',
            'cache-control': 'no-cache'
        }
    })
    .catch(console.log);
}

// Fully qualified table name.
var table = dbu.cassID(keyspace) + '.data';

// Cassandra driver client object.
var client = makeClient({
    host: process.argv[2],
    credentials: {
        username: 'cassandra', password: 'cassandra'
    }
});

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
    domain: null,
    key: null,
    pageState: null,
};

if (/^-?[0-9]{1,30}$/.test(process.argv[4])
        && parseInt(process.argv[4])
        && !process.argv[5]) {
    startOffset.token = ctypes.Long.fromString(process.argv[4]);
} else if (process.argv[4] && process.argv[5]) {
    startOffset.domain = process.argv[4];
    startOffset.key = process.argv[5];
} else if (process.argv[4]) {
    startOffset.pageState = process.argv[4];
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

    // Now figure out what to do with this row
    if (false && counts.title === 0 && counts.rev === 0
            && counts.render === 0) {
        var rowDate = row.tid.getDate();
        if (false && /parsoid_html$/.test(keyspace)
            && rowDate > new Date('2015-04-23T23:30-0700')
            && rowDate < new Date('2015-04-24T13:00-0700')) {
            return reRender(row);
        } else {
            return P.resolve();
        }
        console.log(row.tid.getDate());
        //console.log(row);
        // Don't delete the most recent render for this revision
        return P.resolve();
    // Thin-out
    } else if ((counts.rev > 0 && counts.render > 0)
        || (counts.rev === 0 && counts.render > 0
            // Enforce a grace_ttl of 86400
            && (Date.now() - row.tid.getDate()) > 86400000)
        || (counts.rev > 0 && row.tid.getDate() <  Date.parse('2015-12-31T23:59-0000'))) {
        console.log('-- Deleting:', row._token.toString(), row.tid.getDate().toISOString(), keys.rev);
        var delQuery = 'delete from ' + table + 'where "_domain" = :domain and key = :key and rev = :rev and tid = :tid';
        row.domain = row._domain;
        return client.execute(delQuery, row, {
            prepare: true,
            consistency: cassandra.types.consistencies.quorum
        });
    }

    // Else: nothing to do
    return P.resolve();
}

return iterateTable(client, table, startOffset, processRow);
