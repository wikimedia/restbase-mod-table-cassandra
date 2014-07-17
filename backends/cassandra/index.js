"use strict";

var cass = require('node-cassandra-cql');

function makeClient (options) {
    var client = new cass.Client(options);

	var reconnectCB = function(err) {
		if (err) {
			// keep trying each 500ms
			console.error('pool connection error, scheduling retry!');
			setTimeout(client.connect.bind(client, reconnectCB), 500);
		}
	};
	client.on('connection', reconnectCB);
	client.connect();
    return client;
}

module.exports = makeClient;
