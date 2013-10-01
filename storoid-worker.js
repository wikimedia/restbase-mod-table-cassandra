/*
 * Storoid worker.
 *
 * Configure in storoid.config.json.
 */

// global includes
var express = require('express'),
	helenus = require('helenus'),
	async = require('async'),
	cluster = require('cluster'),
	fs = require('fs'),
	CassandraRevisionStore = require('./CassandraRevisionStore');

var config;

// Get the config
try {
	config = JSON.parse(fs.readFileSync('./storoid.config.json', 'utf8'));
} catch ( e ) {
	// Build a skeleton localSettings to prevent errors later.
	console.error("Please set up your storoid.config.js from the example " +
			"storoid.config.json.example");
	process.exit(1);
}

/**
 * The name of this instance.
 * @property {string}
 */
var instanceName = cluster.isWorker ? 'worker(' + process.pid + ')' : 'master';

console.log( ' - ' + instanceName + ' loading...' );



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
	res.write('Welcome to Storoid.');
	res.end('</body></html>');
});

// robots.txt: no indexing.
app.get(/^\/robots.txt$/, function ( req, res ) {
	res.end( "User-agent: *\nDisallow: /\n" );
});

app.post(/^(\/[^\/]+\/page\/)(.+)$/, function ( req, res ) {
	console.log('post');
	if (req.query['rev/'] !== undefined) {
		// Atomically create a new revision with several properties
		if (req.body._rev && req.body._timestamp) {
			var revision = {
					page: {
						title: req.params[1]
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
				return res.end(JSON.stringify({error: 'Invalid entry point'}), 400);
			}
			store.addRevision(revision,
				function (err, result) {
					if (err) {
						// XXX: figure out whether this was a user or system
						// error
						res.end(JSON.stringify({'error': err}), 500);
					}
					res.end(JSON.stringify({'status': 'Added revision.'}), 200);
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
		return res.end(JSON.stringify({error: "Exactly one query parameter expected."}), 400);
	}
	if (!handlers[req.params[0]]) {
		return res.end(JSON.stringify({error: 'Invalid entry point'}), 400);
	}

	var store = handlers[req.params[0]],
		query = queryKeys[0];
	if (/^rev\/latest\/wikitext$/.test(query)) {
		console.log(query);
		store.getLatest(req.params[1], 'wikitext', function (err, results) {
			if (err) {
				return res.end(JSON.stringify({error: err.toString()}), 400);
			}
			if (results.count !== 1) {
				return res.end(JSON.stringify({error: 'Not found'}), 404);
			}
			return res.end(results[0][0].value, 200);
		});
		return;
	}


	res.end('Unhandled page request', 400);
	// Get the storage backend for req.params[0], '/page/'
	// var res = store.get(req.params[1], req.query);
});


console.log( ' - ' + instanceName + ' ready' );

module.exports = app;

