"use strict";

var P = require('bluebird');
var cass = require('cassandra-driver');
var loadBalancing = cass.policies.loadBalancing;
var reconnection = cass.policies.reconnection;
var DB = require('./db');

P.promisifyAll(cass, { suffix: '_p' });

function makeClient (options) {
    var clientOpts = {};
    var conf = options.conf;
    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;
    // Default to 'datacenter1'
    if (!conf.localDc) { conf.localDc = 'datacenter1'; }
    // See http://www.datastax.com/drivers/nodejs/2.0/module-policies_loadBalancing-DCAwareRoundRobinPolicy.html
    clientOpts.policies = {
        loadBalancing: new loadBalancing.TokenAwarePolicy(
            new loadBalancing.DCAwareRoundRobinPolicy(conf.localDc)
        ),
        // Also see
        // http://www.datastax.com/documentation/developer/nodejs-driver/2.0/common/drivers/reference/clientOptions.html
        // Retry immediately, then delay by 100ms, back off up to 2000ms
        reconnection: new reconnection.ExponentialReconnectionPolicy(100, 2000, true)
    };

    // Increase the schema agreement wait period from the default of 10s
    clientOpts.protocolOptions = {
        maxSchemaAgreementWaitSeconds: 30
    };

    if (conf.username && conf.password) {
        clientOpts.authProvider = new cass.auth.PlainTextAuthProvider(
                conf.username, conf.password);
    }

    // Up the maximum number of prepared statements. Driver default is 500.
    clientOpts.maxPrepared = conf.maxPrepared || 50000;

    var client = new cass.Client(clientOpts);

    client.on('log', function(level, message, info) {
        // Re-map levels
        switch (level) {
            case 'warning': level = 'warn'; break;
            case 'verbose': level = 'trace'; break;
            default: break; // other levels correspond to ours
        }

        level += '/table/cassandra/driver';
        options.log(level, {
            message: message,
            info: info
        });
    });

    return client.connect_p()
    .then(function() {
        return new DB(client, options);
    });
}

module.exports = makeClient;
