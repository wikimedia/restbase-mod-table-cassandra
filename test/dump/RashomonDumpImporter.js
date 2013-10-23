#!/usr/bin/env node
/*jslint node: true */
"use strict";
/**
 * Cassandra write test.
 */

var dumpReader = require('./dumpReader.js'),
	request = require('request'),
	FormData = require('form-data'),
	http = require('http');

function testCassandra () {
	var reader = new dumpReader.DumpReader(),
		requests = 0,
		maxConcurrency = 100;
	http.globalAgent.maxSockets = maxConcurrency;

	reader.on( 'revision', function ( revision ) {
		// Up to 10 retries
		var retries = 10;

		requests++;
		if (requests > maxConcurrency) {
			process.stdin.pause();
		}
		var timestamp = new Date(revision.timestamp).toISOString(),
			name = encodeURIComponent(revision.page.title.replace(/ /g, '_')),
			form = new FormData(),
			reqOptions = {
				method: 'POST',
				uri: 'http://localhost:8000/enwiki/page/' + name + '?rev/',
			};
			form.append('_timestamp', timestamp);
			form.append('_rev', revision.id);
			form.append('wikitext', revision.text);
		reqOptions.headers = form.getHeaders();
		function requestCB (err, response, body) {
			if (err) {
				if (--retries) {
					// retry
					return form.pipe(request(reqOptions, requestCB));
				}

				console.error(err.toString());
				process.exit(1);
			}
			console.log(name);
			requests--;
			if (requests < maxConcurrency) {
				// continue reading
				process.stdin.resume();
			}
		}
		form.pipe(request(reqOptions, requestCB));
	});

	process.stdin.on('data', reader.push.bind(reader) );
	process.stdin.setEncoding('utf8');
	process.stdin.resume();
}

testCassandra();
