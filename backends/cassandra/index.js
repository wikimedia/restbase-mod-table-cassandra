"use strict";

var cass = require('node-cassandra-cql');
var fs = require('fs');
var util = require('util');

var keyspaceCQL = "CREATE KEYSPACE IF NOT EXISTS %s"
    + " WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': 3}";

var tableCQL = fs.readFileSync(__dirname + '/tables.cql').toString();

function useKeyspace(execute, keyspace) {
    return execute(util.format('use %s', keyspace));
}

function promisifyClient (client) {
    var methods = ['connect', 'executeAsPrepared', 'execute', 'executeBatch'];
    methods.forEach(function(method) {
        client[method + '_p'] = Promise.promisify(client[method], false, client);
    });
    return client;
}

function makeClient (options) {
    var client = new cass.Client(options);
    var resolve, reject;
    var pr = new Promise(function (res, rej) {
        resolve = res;
        reject = rej;
    });

    var firstRun = true;
	var reconnectCB = function(err) {
		if (err) {
			// keep trying each 500ms
            if (firstRun) {
                firstRun = false;
                var keySpace = options.keyspace;
                options.keyspace = 'system';
                var tmpClient = new cass.Client(options);
                var execute = Promise.promisify(tmpClient.execute.bind(tmpClient));
                tmpClient.on('connection', function(err) {
                    console.error('Creating keyspace and storoid tables..');
                    execute(util.format(keyspaceCQL, keySpace), [], 'one')
                    .then(function() {
                        return execute(util.format('use %s', keySpace));
                    })
                    .then(function() {
                        return execute(tableCQL, [], 'one');
                    })
                    .catch(function(err) {
                        reject(err);
                    });
                });
                tmpClient.connect();
            } else {
			    console.error('Cassandra connection error @',
                        options.hosts, ':', err, '\nretrying..');
            }
			setTimeout(client.connect.bind(client, reconnectCB), 500);
		} else {
            resolve(promisifyClient(client));
        }
	};
	client.on('connection', reconnectCB);
	client.connect();

    return pr;
}

module.exports = makeClient;
