/*
 * Rashomon worker.
 *
 * Configure in rashomon.config.json.
 */

// global includes
var async = require('async'),
	restify = require('restify'),
	cluster = require('cluster'),
	busboy = require('connect-busboy'),
	fs = require('fs'),
	CassandraRevisionStore = require('./CassandraRevisionStore');

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

function getHandler(prefix, bucket) {
	var storeKey = '/' + prefix + '/' + bucket + '/';
	return handlers[storeKey];
}


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

var server = restify.createServer();

server.use(restify.gzipResponse());

// Increase the form field size limit from the 2M default.
server.use(restify.queryParser());
//server.use(restify.bodyParser());

// form parsing
server.use(busboy({
	immediate: true,
	// Increase the form field size limit from the 1M default.
	limits: { fieldSize: 15 * 1024 * 1024 }
}));

server.use(function (req, res, next) {
	if ( !req.busboy ) {
		return next();
	}
	req.body = req.body || {};
	req.busboy.on('field', function ( field, val ) {
		req.body[field] = val;
	});
	req.busboy.on('end', function () {
		next();
	});
});

server.get('/', function(req, res, next){
	res.write('<html><body>\n');
	res.write('Welcome to Rashomon.');
	res.end('</body></html>');
	next();
});

// robots.txt: no indexing.
server.get(/^\/robots.txt$/, function (req, res, next) {
	res.end( "User-agent: *\nDisallow: /\n" );
	next();
});

server.post({
	path: '/:user/:bucket/:title',
	// Accept () too
	urlParamPattern: '[^\/]*'
}, function (req, res, next) {
	var title = req.params.title;
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
				store = getHandler(req.params.user, req.params.bucket);
			if (!store) {
				return next(new restify.ResourceNotFoundError('Store not found.'));
			}
			store.addRevision(revision,
				function (err, result) {
					if (err) {
						// XXX: figure out whether this was a user or system
						// error
						return next(new restify.InternalError('Something when wrong ' +
								'while adding the revsion: ' + err));
					}
					res.json({'message': 'Added revision ' + result.tid, id: result.tid});
					return next();
				});
		} else {
			// We don't support _rev or _timestamp-less revisions yet
			return next(new restify.MissingParameterError('_rev or _timestamp are missing!'));
		}
	} else {
	console.log(title, req.params);
		return next(new restify.ResourceNotFoundError());
	}
});

server.get({
	path: '/:user/:bucket/:title',
	urlParamPattern: '[^\/]*'
}, function (req, res, next) {
	var queryKeys = Object.keys(req.query);

	// First some rudimentary input validation
	if (queryKeys.length !== 1) {
		return next(new restify.MissingParameterError('Exactly one query parameter expected.'));
	}
	var store = getHandler(req.params.user, req.params.bucket);
	if (!store) {
		return next(new restify.ResourceNotFoundError());
	}

	var page = req.params.title,
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
					return next(new restify.InvalidArgumentError('Invalid date'));
				}
			} else if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(revString)) {
				// uuid
				rev = revString;
			}

			if (page && prop && rev) {
				//console.log(query);
				store.getRevision(page, rev, prop, function (err, results) {
					if (err) {
						return next(new restify.InternalError('Ouch: ' + err.toString()));
					}
					if (!results.length) {
						return next(new restify.ResourceNotFoundError());
					}
					res.writeHead(200, {'Content-type': 'text/plain'});
					res.end(results[0][0]);
					return next();
				});
				return;
			}
		}
	}

	return next(new restify.ResourceNotFoundError());
});


module.exports = server;

