"use strict";

var cass = require('cassandra-driver');
var DB = require('./db');

function promisifyClient (client, options) {
    var methods = ['connect', 'shutdown', 'execute', 'batch'];
    methods.forEach(function(method) {
        //console.log(method, client[method]);
        client[method + '_p'] = Promise.promisify(client[method].bind(client));
    });

    return client;
}


function makeClient (options) {
    var clientOpts = {};
    var conf = options.conf;
    clientOpts.keyspace = conf.keyspace || 'system';
    clientOpts.contactPoints = conf.hosts;
    if (conf.username && conf.password) {
        clientOpts.authProvider = new cass.auth.PlainTextAuthProvider(
                conf.username, conf.password);
    }

    var client = promisifyClient(new cass.Client(clientOpts));

    var reconnectCB = function(err) {
        if (err) {
            // keep trying each 500ms
            options.log('error/cassandra/connection', err,
                    'Cassandra connection error, retrying..');
            setTimeout(client.connect.bind(client, reconnectCB), 500);
        }
    };
    client.on('connection', reconnectCB);
    client.on('log', options.log);

    return client.connect_p()
    .then(function() {
        return new DB(client, options);
    });
}

module.exports = makeClient;
