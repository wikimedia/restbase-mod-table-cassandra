"use strict";

const P = require('bluebird');
const cass = require('cassandra-driver');
const fs = require('fs');
const loadBalancing = cass.policies.loadBalancing;
const reconnection = cass.policies.reconnection;
const DB = require('./db');

P.promisifyAll(cass, { suffix: '_p' });

function validateAndNormalizeDcConf(conf) {
    // Default to 'datacenter1'
    if (!conf.localDc) { conf.localDc = 'datacenter1'; }
    if (!conf.datacenters) { conf.datacenters = ['datacenter1']; }
    if (!(conf.datacenters instanceof Array)) {
        throw new Error('invalid datacenters configuration (not an array)');
    }
    if (conf.datacenters.indexOf(conf.localDc) < 0) {
        throw new Error('localDc not in configured datacenters');
    }
}

// sync
function sslOptions(sslConf) {
    const sslOpts = {};

    if (sslConf.cert) { sslOpts.cert = fs.readFileSync(sslConf.cert); }
    if (sslConf.key)  { sslOpts.key  = fs.readFileSync(sslConf.key);  }

    if (sslConf.ca) {
        sslOpts.ca = [];
        if (sslConf.ca instanceof Array) {
            sslConf.ca.forEach((ca) => {
                sslOpts.ca.push(fs.readFileSync(ca));
            });
        } else {
            sslOpts.ca.push(fs.readFileSync(sslConf.ca));
        }
    }

    return sslOpts;
}

function makeClient(options) {
    const clientOpts = {};
    const conf = options.conf;
    validateAndNormalizeDcConf(conf);

    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;

    // See http://www.datastax.com/drivers/nodejs/2.0/module-policies_loadBalancing-DCAwareRoundRobinPolicy.html
    clientOpts.policies = {
        loadBalancing: new loadBalancing.TokenAwarePolicy(
            new loadBalancing.DCAwareRoundRobinPolicy(conf.localDc)
        ),
        // Also see
        // http://www.datastax.com/documentation/developer/nodejs-driver/2.0/common/drivers/reference/clientOptions.html
        // Retry immediately, then delay by 100ms, back off up to 120s
        reconnection: new reconnection.ExponentialReconnectionPolicy(100, 120000, true)
    };

    if (conf.tls) {
        try {
            clientOpts.sslOptions = sslOptions(conf.tls);
        } catch (e) {
            return P.reject(e);
        }
    }

    // Increase the schema agreement wait period from the default of 10s
    clientOpts.protocolOptions = {
        maxSchemaAgreementWaitSeconds: 30,
        maxVersion: 3
    };

    if (conf.username && conf.password) {
        clientOpts.authProvider = new cass.auth.PlainTextAuthProvider(
                conf.username, conf.password);
    }

    // Up the maximum number of prepared statements. Driver default is 500.
    clientOpts.maxPrepared = conf.maxPrepared || 50000;

    const client = new cass.Client(clientOpts);

    client.on('log', (level, message, info) => {
        // Re-map levels
        /* eslint-disable indent */
        switch (level) {
            case 'warning':
                level = 'warn';
                break;
            case 'verbose':
                level = 'trace';
                break;
            default:
                break; // other levels correspond to ours
        }
        /* eslint-enable indent */

        level += '/table/cassandra/driver';
        options.log(level, {
            message,
            info
        });
    });

    return client.connect_p()
    .then(() => new DB(client, options));
}

module.exports = makeClient;
