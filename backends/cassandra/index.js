"use strict";

var cass = require('node-cassandra-cql');
var defaultConsistency = cass.types.consistencies.localQuorum;
var fs = require('fs');
var util = require('util');
var crypto = require('crypto');


var tableCQL = fs.readFileSync(__dirname + '/tables.cql').toString();

// Hash a key into a valid Cassandra key name
function hashKey (key) {
    return crypto.Hash('sha1')
        .update(key)
        .digest()
        .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
        .replace(/[+\/]/g, '_')
        // Remove base64 padding, has no entropy
        .replace(/=+$/, '');
}

/**
 * Derive a valid keyspace name from a random bucket name. Try to use valid
 * chars as far as possible, but fall back to a sha1 if not possible. Also
 * respect Cassandra's limit of 32 or fewer alphanum chars & first char being
 * an alpha char.
 *
 * @param {string} prefix, a string of up to 8 ascii chars starting with an
 * alpha char.
 * @param {string} key, the bucket name to derive the key of
 * @return {string} Valid Cassandra keyspace key
 */
function keyspaceName (prefix, key) {
    if (prefix.length > 8) {
        throw new Error('The master store keyspace needs to be shorter than 8 chars. Got ' + prefix);
    }
    // "Keyspace names are 32 or fewer alpha-numeric characters and
    // underscores, the first of which is an alpha character."
    if (/^[a-zA-Z0-9_]*$/.test(key)) {
        if (key.length < 23) {
            return prefix + '_' + key;
        } else {
            return prefix + '_' + key.substr(0, 11) + hashKey(key).substr(0, 12);
        }
    } else {
        // Try to use an alphanumeric prefix
        var asciiPrefix = /^[a-zA-Z0-9_]+/.exec(key);
        if (asciiPrefix) {
            return prefix + '_' + asciiPrefix[0].substr(0, 11) + hashKey(key).substr(0, 12);
        } else {
            return prefix + '_' + hashKey(key).substr(0, 23);
        }
    }
}

// CQL for the creation of a new keyspace
var keyspaceCQLTemplate = "CREATE KEYSPACE IF NOT EXISTS %s"
    + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}";

function promisifyClient (client, options) {
    var methods = ['connect', 'shutdown', 'executeAsPrepared', 'execute', 'executeBatch'];
    methods.forEach(function(method) {
        //console.log(method, client[method]);
        client[method + '_p'] = Promise.promisify(client[method].bind(client));
    });

    // Add some utility methods
    client.createKeyspace_p = function (name, consistency) {
        var keyspaceCQL = util.format(keyspaceCQLTemplate, name);
        return client.execute_p(keyspaceCQL, [], consistency || defaultConsistency);
    };

    client.useKeyspace_p = function(keyspace) {
        return client.execute_p(util.format('use %s', keyspace));
    };

    client.keyspaceName = keyspaceName;

    return client;
}


function createTables(options) {
    console.log('Creating keyspace and storoid tables..');
    var origKeyspace = options.keyspace;
    options.keyspace = 'system';
    var tmpClient = promisifyClient(new cass.Client(options));
    options.keyspace = origKeyspace;
    return tmpClient.connect_p()
    .return(tmpClient.createKeyspace_p(origKeyspace, 1))
    .then(function() {
        console.log('tmpClient connected');
        return tmpClient.execute_p(util.format('use %s', origKeyspace));
    })
    .then(function() {
        var newTableCQL = util.format(tableCQL, origKeyspace);
        return tmpClient.execute_p(newTableCQL, [], 2);
    });
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
    .return({
        type: 'cassandra',
        client: client,
        options: options
    });
}

module.exports = makeClient;
