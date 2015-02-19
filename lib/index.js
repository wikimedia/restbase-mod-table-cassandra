"use strict";

var P = require('bluebird');
var cass = require('cassandra-driver');
var DB = require('./db');

P.promisifyAll(cass, { suffix: '_p' });

function makeClient (options) {
    var clientOpts = {};
    var conf = options.conf;
    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;
    // Default to 'datacenter1'
    if (!conf.localDc) { conf.localDc = 'datacenter1'; }
    // See http://www.datastax.com/drivers/nodejs/1.0/module-policies_loadBalancing-DCAwareRoundRobinPolicy.html
    clientOpts.loadBalancing = new cass.policies
        .loadBalancing.DCAwareRoundRobinPolicy(conf.localDc);
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

    return P.resolve(new DB(client, options));
}

module.exports = makeClient;
