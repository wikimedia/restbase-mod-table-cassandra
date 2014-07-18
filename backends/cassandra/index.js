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
        //console.log(method, client[method]);
        client[method + '_p'] = Promise.promisify(client[method].bind(client));
    });
    /*
     * client.connect_p = function() {
        return new Promise(function(resolve, reject) {
            client.connect(function(err) {
                if (err) { reject(err); }
                else { resolve(); }
            })
        });
    };*/
    return client;
}

function createTables(options) {
    console.log('Creating keyspace and storoid tables..');
    var origKeyspace = options.keyspace;
    options.keyspace = 'system';
    var tmpClient = promisifyClient(new cass.Client(options));
    options.keyspace = origKeyspace;
    return tmpClient.connect_p()
    .return(tmpClient.execute_p(util.format(keyspaceCQL, origKeyspace), [], 'one'))
    .then(function() {
        console.log('tmpClient connected');
        return tmpClient.execute_p(util.format('use %s', origKeyspace));
    })
    .return( tmpClient.execute_p(tableCQL, [], 'one') );
}

function makeClient (options) {
    var client = promisifyClient(new cass.Client(options));

	var reconnectCB = function(err) {
		if (err) {
            // keep trying each 500ms
            console.error('Cassandra connection error @',
                    options.hosts, ':', err, '\nretrying..');
			setTimeout(client.connect.bind(client, reconnectCB), 500);
		}
	};
	client.on('connection', reconnectCB);

    return client.connect_p()
    .catch(function(err) {
        return createTables(options);
    })
    .return(client)
}

module.exports = makeClient;
