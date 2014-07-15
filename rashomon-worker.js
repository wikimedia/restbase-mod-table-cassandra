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

/**
 * Global store accounts
 */
var accounts = new Map({
    enwiki: {
        buckets: new Map({
            pages: {
                type: 'revision'
            }
        })
    }
});

// XXX: load accounts from Cassandra
// Accounts.load()
// .then(newAccounts) {
//     accounts = newAccounts;
// }

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
	req.body = req.body || {};
	if ( !req.busboy ) {
		return next();
	}
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

var bucketPattern = /\/v1\/([^\/]+)\/([^\/]+)(\/[^\/]+)*$/;

// Hook up the bucket handlers for all methods
server.get(bucketPattern, bucketHandler);
server.post(bucketPattern, bucketHandler);
server.head(bucketPattern, bucketHandler);
server.put(bucketPattern, bucketHandler);
server.del(bucketPattern, bucketHandler);
server.opts(bucketPattern, bucketHandler);
server.patch(bucketPattern, bucketHandler);

/**
 * Universal bucket handler
 *
 * Looks up account & bucket, authenticates the request and calls the bucket
 * handler for the method if found.
 */
function bucketHandler (req, res, next) {
    var account = accounts.get(req.params[0]);
    if (account) {
        var bucket = account.buckets.get(req.params[1]);
        if (bucket) {
            // XXX: authenticate against bucket ACLs
            var handler = bucket.handlers[req.method];
            if (handler) {

                // Yay! All's well. Go for it!
                // Drop the non-bucket parts of the path / url
                req.path = req.params[2];
                req.url = req.params[2];
                req.params = req.params.slice(2);
                return handler(req, res, next);
            } else {
                res.setHeader('Allow', Object.keys(bucket.handlers).join(' '));
                res.json('405', {
                    "code":"MethodNotAllowedError",
                    "message":req.method + " is not allowed"
                });
            }
        } else {
            res.json('404', {
                "code":"NotFoundError",
                "message": "Bucket " + req.params[1] + " not found"
            });
        }
    } else {
        res.json('404', {
            "code":"NotFoundError",
            "message": "Account " + req.params[0] + " not found"
        });
    }
};




module.exports = server;

