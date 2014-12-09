"use strict";

/*
*  Test for server to test using http interface.
*/

var http = require('http');
var RouteSwitch = require('routeswitch');

var app = {
    // The global proxy object
    proxy: null
};

var qs = require('querystring');
var url = require('url');
var SIMPLE_PATH = /^(\/(?!\/)[^\?#\s]*)(\?[^#\s]*)?$/;
var parseURL = function parseURL (uri) {
    // Fast path for simple path uris
    var fastMatch = SIMPLE_PATH.exec(uri);
    if (fastMatch) {
        return {
            protocol: null,
            slashes: null,
            auth: null,
            host: null,
            port: null,
            hostname: null,
            hash: null,
            search: fastMatch[2] || '',
            pathname: fastMatch[1],
            path: fastMatch[1],
            query: fastMatch[2] && qs.parse(fastMatch[2]) || {},
            href: uri
        };
    } else {
        return url.parse(uri, true);
    }
};


function setupConfigDefaults(conf) {
    if (!conf) { conf = {
            // module name
            type: "restbase-cassandra",
            hosts: ["localhost"],
            keyspace: "system",
            username: "cassandra",
            password: "cassandra",
            defaultConsistency: 'one' // use localQuorum in production
        };
    }
    return conf;
}


function handleRequest(router, req, res) {
	// Create a new, clean request object
		var body = req.body;
		var urlData = parseURL(req.url);
		if (/^application\/json/i.test(req.headers['content-type'])) {
			try	{
				body = JSON.parse(req.body.toString());
			} catch (e) {
				console.log('error/request/json-parsing', e);
			}
		}
		var newReq = {
			uri: urlData.pathname,
			query: urlData.query,
			method: req.method.toLowerCase(),
			headers: req.headers,
			body: body
		};
		//return app.restbase.request(newReq);
		var match = router.match(newReq.uri);
		if (match) {
			var handler = match.methods[req.method.toLowerCase()];
			console.log(handler, match, req.url);
			if (handler && handler.request_handler) {
				return handler.request_handler({}, req)
				.then(function(response) {
					return response;
				});
			}
		}

	/*
	return {
		status: 404,
		body: {
			type: 'not_found#proxy_handler',
			title: 'Not found.',
			uri: req.uri,
			method: req.method,
			}
		};*/
}

function main() {
	var conf = setupConfigDefaults();
	var opt = {
		log: console.log,
		conf: conf
	};

	return require('../index.js')(opt)
	.then(function(handler){
		var router = new RouteSwitch.fromHandlers([handler]);
		var server = http.createServer(handleRequest.bind(null, router));
		var port = conf.port || 7231;
        server.listen(port, null, 6000);
        //opts.log('info', 'listening on port ' + port);
	})
	.catch(function(e) {
        console.log('Error during RESTBase startup: ', e);
    });
}

if (module.parent === null) {
    main();
} else {
    module.exports = main;
}