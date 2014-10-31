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
    .then(function() {
        return new DB(client);
    });
}

module.exports = makeClient;
