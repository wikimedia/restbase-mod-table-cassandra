#!/usr/bin/env node
'use strict';
/**
 * Rashomon write test.
 */

var dumpReader = require('./dumpReader.js'),
	request = require('request'),
	FormData = require('form-data'),
	http = require('http');

function testWrites() {
	var reader = new dumpReader.DumpReader(),
		totalSize = 0,
		revisions = 0,
		totalRetries = 0,
		intervalDate = new Date(),
		startDate = intervalDate,
		requests = 0,
		maxConcurrency = 50;
	http.globalAgent = false;

	reader.on('revision', (revision) => {

		// Up to 50 retries
		var retries = 50,
			retryDelay = 0.5; // start with 0.5 seconds

		requests++;
		if (requests > maxConcurrency) {
			process.stdin.pause();
		}
		var timestamp = new Date(revision.timestamp).toISOString(),
			name = encodeURIComponent(revision.page.title.replace(/ /g, '_'));

		function handlePostResponse(err, response, body) {
			if (err || response.statusCode !== 200) {
				if (!err) {
					err = response.statusCode + ' ' + body;
				}
				console.error(err.toString());
				totalRetries++;
				if (--retries) {
					// retry after retryDelay seconds
					setTimeout(doPost, retryDelay * 1000);
					// Exponential back-off
					retryDelay = retryDelay * 2;
					return;
				}
				process.exit(1);
			}
			totalSize += revision.text.length;
			revisions++;
			var interval = 1000;
			if (revisions % interval === 0) {

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

		function doPost() {
			var form = new FormData(),
				reqOptions = {
					method: 'POST',
					uri: 'http://localhost:8000/enwiki/page/' + name + '?rev/'
				};
			form.append('_timestamp', timestamp);
			form.append('_rev', revision.id);
			form.append('wikitext', revision.text);
			reqOptions.headers = form.getHeaders();
			form.pipe(request(reqOptions, handlePostResponse));
		}

		// send it off
		doPost();
	});
	reader.on('end', () => {
		console.log('####################');
		var delta = Math.round((new Date() - startDate) / 1000);
		console.log(revisions + ' revisions in ' + delta +
			's (' + revisions / delta + '/s); ' +
			'Total size: ' + totalSize);
		console.log(totalRetries, 'retries total');
		process.exit();
	});

	process.stdin.on('data', reader.push.bind(reader));
	process.stdin.setEncoding('utf8');
	process.stdin.resume();
}

testWrites();
