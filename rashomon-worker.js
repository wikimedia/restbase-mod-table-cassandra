/*
 * Rashomon worker.
 *
 * Configure in rashomon.config.json.
 */

// global includes
var express = require('express'),
	async = require('async'),
	cluster = require('cluster'),
	fs = require('fs'),
	CassandraRevisionStore = require('./CassandraRevisionStore'),
	uuid = require('node-uuid');

var config;

// Get the config
try {
	config = JSON.parse(fs.readFileSync('./rashomon.config.json', 'utf8'));
} catch ( e ) {
	// Build a skeleton localSettings to prevent errors later.
	console.error("Please set up your rashomon.config.js from the example " +
			"rashomon.config.json.example");
	process.exit(1);
}

/*
 * Backend setup
 */

var setups = {};
setups.cassandra = function(name, options, cb) {
	return new CassandraRevisionStore(name, config.handlers[name], cb);
};


var handlers = {};
async.forEach(Object.keys(config.handlers), function(prefix, cb) {
	console.log( 'Registering backend for ' + prefix );
	var backend = config.handlers[prefix].backend;
	// TODO: make this properly async!
	handlers[prefix] = setups[backend.type](prefix, backend, cb);
});

/* -------------------- Web service --------------------- */

/**
 * Generic CORS checking / handling
 */
function handleCors (req, res) {
	// TODO: verify origin
	res.setHeader('Access-Control-Allow-Origin',
		config.allowCORS);
	return true;
}

var app = express.createServer();

// Increase the form field size limit from the 2M default.
app.use(express.bodyParser({maxFieldsSize: 25 * 1024 * 1024}));
app.use( express.limit( '25mb' ) );

app.get('/', function(req, res){
	res.write('<html><body>\n');
	res.write('Welcome to Rashomon.');
	res.end('</body></html>');
});

// robots.txt: no indexing.
app.get(/^\/robots.txt$/, function ( req, res ) {
	res.end( "User-agent: *\nDisallow: /\n" );
});

app.post(/^(\/[^\/]+\/page\/)(.+)$/, function ( req, res ) {
	var title = req.params[1];
	console.log(title);
	if (req.query['rev/'] !== undefined) {
		// Atomically create a new revision with several properties
		if (req.body._rev && req.body._timestamp) {
			var revision = {
					page: {
						title: title
					},
					id: Number(req.body._rev),
					timestamp: req.body._timestamp,
					props: {
						wikitext: {
							value: new Buffer(req.body.wikitext)
						}
					}
				},
				store = handlers[req.params[0]];
			if (!store) {
				res.writeHead(400);
				return res.end(JSON.stringify({error: 'Invalid entry point'}));
			}
			store.addRevision(revision,
				function (err, result) {
					if (err) {
						// XXX: figure out whether this was a user or system
						// error
						res.writeHead(500);
						return res.end(JSON.stringify({'error': err}));
					}
					res.end(JSON.stringify({'status': 'Added revision ' + result.tid}));
				});
		} else {
				res.end('Page request');
		}
	} else {
			res.end('Page request');
	}
});

app.get(/^(\/[^\/]+\/page\/)([^?]+)$/, function ( req, res ) {
	var queryKeys = Object.keys(req.query);

	// First some rudimentary input validation
	if (queryKeys.length !== 1) {
		res.writeHead(400);
		return res.end(JSON.stringify({error: "Exactly one query parameter expected."}));
	}
	if (!handlers[req.params[0]]) {
		res.writeHead(400);
		return res.end(JSON.stringify({error: 'Invalid entry point'}));
	}

	var store = handlers[req.params[0]],
		page = req.params[1],
		query = queryKeys[0],
		queryComponents = query.split(/\//g);
	if (/^rev\//.test(query)) {
		if (queryComponents.length >= 2) {
			var revString = queryComponents[1],
				// sanitized / parsed rev
				rev = null,
				// 'wikitext', 'html' etc
				prop = queryComponents[2] || null;

			if (revString === 'latest') {
				// latest revision
				rev = revString;
			} else if (/^\d+$/.test(revString)) {
				// oldid
				rev = Number(revString);
			} else if (/^\d{4}-\d{2}-\d{2}/.test(revString)) {
				// timestamp
				rev = new Date(revString);
				if (isNaN(rev.valueOf())) {
					// invalid date
					res.writeHead(400);
					return res.end(JSON.stringify({error: 'Invalid date'}));
				}
			} else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(revString)) {
				// uuid
				rev = revString;
			}

			if (page && prop && rev) {
				//console.log(query);
				store.getRevision(page, rev, prop, function (err, results) {
					if (err) {
						res.writeHead(400);
						return res.end(JSON.stringify({error: err.toString()}));
					}
					if (!results.length) {
						res.writeHead(404);
						return res.end(JSON.stringify({error: 'Not found'}));
					}
					return res.end(results[0][0]);
				});
				return;
			}
		}
	}


	res.end('Unhandled page request', 400);
	// Get the storage backend for req.params[0], '/page/'
	// var res = store.get(req.params[1], req.query);
});


module.exports = app;

