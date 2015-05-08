"use strict";

/**
 * Simple script to delete all but the newest entry per revision in a
 * key_rev_value table.
 */

var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var util = require('util');
var preq = require('preq');

if (!process.argv[3]) {
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace>');
    process.exit(1);
}

// A custom retry policy
function AlwaysRetry () {}
util.inherits(AlwaysRetry, cassandra.policies.retry.RetryPolicy);
var ARP = AlwaysRetry.prototype;
// Always retry.
ARP.onUnavailable = function(requestInfo) {
    // Reset the connection
    requestInfo.handler.connection.close(function() {
        requestInfo.handler.connection.open(function(){});
    });
    return { decision: 1 };
};
ARP.onWriteTimeout = function() { return { decision: 2 }; };
ARP.onReadTimeout = function(requestInfo) {
    // Reset the connection
    requestInfo.handler.connection.close(function() {
        requestInfo.handler.connection.open(function(){});
    });
    console.log('read retry');
    return { decision: 1 };
};


function makeClient() {
    return new cassandra.Client({
        contactPoints: [process.argv[2]],
        keyspace: process.argv[3],
        authProvider: new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra'),
        socketOptions: { connectTimeout: 10000 },
        //policies: {
        //     retry: new AlwaysRetry()
        //},
    });
}
var client = makeClient();


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

function processRow (row) {
    // Create a new set of keys
    var newKeys = {
        title: JSON.stringify([row._domain, row.key]),
        rev: JSON.stringify([row._domain, row.key, row.rev])
    };

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

    // Now figure out what to do with this row
    if (false && counts.title === 0 && counts.rev === 0
            && counts.render === 0) {
        var rowDate = row.tid.getDate();
        if (false && /parsoid_html$/.test(process.argv[3])
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
    } else if ((counts.rev > 0 && counts.render > 0)
        || (counts.rev === 0 && counts.render > 0
            // Enforce a grace_ttl of 86400
            && (Date.now() - row.tid.getDate()) > 86400)) {
        console.log(keys.rev, row.tid);
        var delQuery = 'delete from data where "_domain" = :domain and key = :key and rev = :rev and tid = :tid';
        row.domain = row._domain;
        return client.executeAsync(delQuery, row, { prepare: true });
    }

    // Else: nothing to do
    return P.resolve();
}

//var query = 'select "_domain", key, rev, tid from data where token("_domain",key) > -6004964422032836805';
var query = 'select "_domain", key, rev, tid from data';

function nextPage(pageState, retries) {
    //console.log(pageState);
    return client.executeAsync(query, [], {
        prepare: true,
        fetchSize: retries ? 1 : 50,
        pageState: pageState,
    })
    .catch(function(err) {
        console.log(retries, err);
        console.log(pageState);
        if (retries > 10) {
            process.exit(1);
        }
        return nextPage(pageState, (retries || 0) + 1);
    });
}

function processRows(pageState) {
    return nextPage(pageState)
    .then(function(res) {
        return P.resolve(res.rows)
        .each(processRow)
        .then(function() {
            process.nextTick(function() { processRows(res.pageState); });
        })
        .catch(function(e) {
            console.log(res.pageState);
            throw e;
        });
    });
}

var pageState;
if (process.argv[4]) {
    var pageState = process.argv[4];
}
return processRows(pageState);
