"use strict";

var cass = require('cassandra-driver');
var DB = require('./db');

Promise.promisifyAll(cass, { suffix: '_p' });

function makeClient (options) {
    var clientOpts = {};
    var conf = options.conf;
    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;
    if (conf.username && conf.password) {
        clientOpts.authProvider = new cass.auth.PlainTextAuthProvider(
                conf.username, conf.password);
    }

    var client = new cass.Client(clientOpts);

    client.on('log', options.log);

    return Promise.resolve(new DB(client, options));
}

module.exports = makeClient;
