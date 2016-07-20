"use strict";


var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));
var consistencies = cassandra.types.consistencies;
var util = require('util');


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

function makeClient(options) {
    var creds = options.credentials;
    return new cassandra.Client({
        contactPoints: [options.host],
        authProvider: new cassandra.auth.PlainTextAuthProvider(creds.username, creds.password),
        socketOptions: { connectTimeout: 10000 },
    });
}

function getQuery(tableName, offsets) {
    var cql = 'SELECT "_domain", key, rev, tid, token("_domain",key) AS "_token" FROM ' + tableName;
    var params = [];
    if (offsets.token) {
        cql += ' WHERE token("_domain",key) >= ?';
        params.push(offsets.token);
    } else if (offsets.domain) {
        cql += ' WHERE token("_domain",key) >= token(?, ?)';
        params.push(offsets.domain);
        params.push(offsets.key);
    }
    return {
        cql: cql,
        params: params,
    };
}

function nextPage(client, tableName, offsets, retryDelay) {
    //console.log(offsets);
    var query = getQuery(tableName, offsets);
    return client.executeAsync(query.cql, query.params, {
        prepare: true,
        fetchSize: retryDelay ? 1 : 50,
        pageState: offsets.pageState,
        consistency: retryDelay ? consistencies.one : consistencies.quorum,
    })
    .catch(function(err) {
        retryDelay = retryDelay || 1; // ms
        if (retryDelay < 20 * 1000) {
            retryDelay *= 2 + Math.random();
        } else if (offsets.token) {
            // page over the problematic spot
            console.log('Skipping over problematic token:',
                offsets.token.toString());
            offsets.token = offsets.token.add(500000000);
            console.log('Retrying with new token:',
                offsets.token.toString());
            return nextPage(client, tableName, offsets, retryDelay);
        }

        console.log('Error:', err);
        console.log('PageState:', offsets.pageState);
        console.log('Last token:', offsets.token.toString());
        console.log('Retrying in', Math.round(retryDelay) / 1000, 'seconds...');
        return new P(function(resolve, reject) {
            setTimeout(function() {
                nextPage(client, tableName, offsets, retryDelay)
                    .then(resolve)
                    .catch(reject);
            }, retryDelay);
        });
    });
}

/**
 * Iterate rows in a key-rev-value table.
 *
 * @param {cassandra#Client} client - Cassandra client instance.
 * @param {string}   tableName - Cassandra table name.
 * @param {Object}   offsets   - Offset information (token, domain, key, and pageState).
 * @param {Function} func      - Function called with result rows.
 */
function processRows(client, tableName, offsets, func) {
    return nextPage(client, tableName, offsets)
    .then(function(res) {
        return P.resolve(res.rows)
        .each(func)
        .then(function() {
            process.nextTick(function() {
                offsets.pageState = res.pageState;
                processRows(client, tableName, offsets, func);
            });
        })
        .catch(function(e) {
            console.log(res.pageState);
            console.log(e);
            throw e;
        });
    });
}

module.exports = {
    iterateTable: processRows,
    makeClient: makeClient,
};
