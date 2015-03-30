"use strict";

/**
 * Simple script to delete all but the newest entry per revision in a
 * key_rev_value table.
 */

var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var util = require('util');

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

var lastKey;
function processRow (row) {
    // We include the revision in the key, so that we keep one render per
    // revision.
    var key = JSON.stringify([row._domain, row.key, row.rev]);
    if (key !== lastKey) {
        lastKey = key;
        //console.log(row);
        // Don't delete the most recent render for this revision
        return P.resolve();
    } else {
        console.log(key, row.tid);
        var delQuery = 'delete from data where "_domain" = :domain and key = :key and rev = :rev and tid = :tid';
        row.domain = row._domain;
        return client.executeAsync(delQuery, row, { prepare: true });
    }

}

// For parallelization, can change this to start from a specific token value
//var query = 'select "_domain", key, rev, tid from data where token("_domain",key) > -6004964422032836805';
var query = 'select "_domain", key, rev, tid from data';

function nextPage(pageState, retries) {
    //console.log(pageState);
    return client.executeAsync(query, [], {
        prepare: true,
        fetchSize: retries ? 1 : 200,
        pageState: pageState,
    })
    .catch(function(err) {
        console.log(retries, err);
	console.log(pageState);
	if (retries > 10) {
		// big hammer: replace the client
		client.shutdown();
		client = makeClient();
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
