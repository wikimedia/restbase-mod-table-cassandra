"use strict";

var cass = require('cassandra-driver');
var DB = require('./db');

Promise.promisifyAll(cass, { suffix: '_p' });

function makeClient (options) {
    var clientOpts = {};
    var conf = options.conf;
    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;
    // Also see
    // http://www.datastax.com/documentation/developer/nodejs-driver/1.0/common/drivers/reference/clientOptions.html
    clientOpts.reconnection = new cass.policies
        // Retry immediately, then delay by 100ms, back off up to 2000ms
        .reconnection.ExponentialReconnectionPolicy(100, 2000, true);
    if (conf.username && conf.password) {
        clientOpts.authProvider = new cass.auth.PlainTextAuthProvider(
                conf.username, conf.password);
    }

    var client = new cass.Client(clientOpts);

    client.on('log', options.log);

    return Promise.resolve(new DB(client, options));
}

module.exports = makeClient;
