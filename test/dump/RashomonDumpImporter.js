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
		totalSize = 0,
		revisions = 0,
		intervalDate = new Date(),
		startDate = intervalDate,
		requests = 0,
		maxConcurrency = 100;
	http.globalAgent.maxSockets = maxConcurrency;

	reader.on( 'revision', function ( revision ) {

		// Up to 50 retries
		var retries = 50;

		requests++;
		if (requests > maxConcurrency) {
			process.stdin.pause();
		}
		var timestamp = new Date(revision.timestamp).toISOString(),
			name = encodeURIComponent(revision.page.title.replace(/ /g, '_'));

		function requestCB (err, response, body) {
			if (err) {
				console.error(err.toString());
				if (--retries) {
					// retry after 10 seconds
					setTimeout(doPost, 10000);
					return;
				}

				process.exit(1);
			}
			totalSize += revision.text.length;
			revisions++;
			var interval = 1000;
			if(revisions % interval === 0) {

				var newIntervalDate = new Date(),
					rate = interval / (newIntervalDate - intervalDate) * 1000,
					totalRate = revisions / (newIntervalDate - startDate) * 1000;
				console.log(revisions + ' ' + Math.round(rate) + '/s; ' +
						'avg ' + Math.round(totalRate) + '/s');
				intervalDate = newIntervalDate;
			}

			requests--;
			if (requests < maxConcurrency) {
				// continue reading
				process.stdin.resume();
			}
		}

		function doPost () {
			var form = new FormData(),
				reqOptions = {
					method: 'POST',
					uri: 'http://localhost:8000/enwiki/page/' + name + '?rev/',
				};
			form.append('_timestamp', timestamp);
			form.append('_rev', revision.id);
			form.append('wikitext', revision.text);
			reqOptions.headers = form.getHeaders();
			form.pipe(request(reqOptions, requestCB));
		}

		// send it off
		doPost();
	});
	reader.on('end', function() {
		console.log('####################');
		var delta = Math.round((new Date() - startDate) * 1000);
		console.log('Processed ' + revisions + ' revisions in ' + delta + 's, at a rate of ' +
			revisions / delta + '/s');
		console.log('Total size: ' + totalSize);
		process.exit();
	});

	process.stdin.on('data', reader.push.bind(reader) );
	process.stdin.setEncoding('utf8');
	process.stdin.resume();
}

testCassandra();
