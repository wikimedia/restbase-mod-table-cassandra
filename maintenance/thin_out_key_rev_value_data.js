"use strict";

/**
 * Simple script to delete all but the newest entry per revision in a
 * key_rev_value table.
 */

var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var consistencies = cassandra.types.consistencies;
var ctypes = cassandra.types;
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
            && (Date.now() - row.tid.getDate()) > 86400000)
        || (counts.rev > 0 && row.tid.getDate() <  Date.parse('2015-07-10T13:00-0700'))) {
        console.log('-- deleting:', row._token.toString(), row.tid.getDate().toISOString(), keys.rev);
        var delQuery = 'delete from data where "_domain" = :domain and key = :key and rev = :rev and tid = :tid';
        row.domain = row._domain;
        return client.executeAsync(delQuery, row, {
            prepare: true,
            consistency: cassandra.types.consistencies.quorum
        });
    }

    // Else: nothing to do
    return P.resolve();
}


function getQuery () {
    var cql = 'select "_domain", key, rev, tid, token("_domain",key) as "_token" from data';
    var params = [];
    if (startOffset.token) {
        cql += ' where token("_domain",key) >= ?';
        params.push(startOffset.token);
    } else if (startOffset.domain) {
        cql += ' where token("_domain",key) >= token(?, ?)';
        params.push(startOffset.domain);
        params.push(startOffset.key);
    }
    return {
        cql: cql,
        params: params,
    };
}


function nextPage(pageState, retryDelay) {
    //console.log(pageState);
    var query = getQuery();
    return client.executeAsync(query.cql, query.params, {
        prepare: true,
        fetchSize: retryDelay ? 1 : 50,
        pageState: pageState,
        consistency: retryDelay ? consistencies.one : consistencies.quorum,
    })
    .catch(function(err) {
        retryDelay = retryDelay || 1; // ms
        if (retryDelay < 20 * 1000) {
            retryDelay *= 2 + Math.random();
        } else if (startOffset.token) {
            // page over the problematic spot
            console.log('Skipping over problematic token:',
                startOffset.token.toString());
            startOffset.token = startOffset.token.add(500000000);
            console.log('Retrying with new token:',
                startOffset.token.toString());
            return nextPage(null, retryDelay);
        }

        console.log('Error:', err);
        console.log('PageState:', pageState);
        console.log('Last token:', startOffset.token.toString());
        console.log('Retrying in', Math.round(retryDelay) / 1000, 'seconds...');
        return new P(function(resolve, reject) {
            setTimeout(function() {
                nextPage(pageState, retryDelay)
                    .then(resolve)
                    .catch(reject);
            }, retryDelay);
        });
    });
}

function processRows(pageState) {
    return nextPage(pageState)
    .then(function(res) {
        return P.resolve(res.rows)
        .each(processRow)
        .then(function() {
            process.nextTick(function() {
                processRows(res.pageState);
            });
        })
        .catch(function(e) {
            console.log(res.pageState);
            console.log(e);
            throw e;
        });
    });
}

return processRows(startOffset.pageState);
