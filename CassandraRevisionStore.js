/**
 * Simple Cassandra-based revisioned storage implementation
 *
 * Implements (currently a subset of) the functionality documented in
 * https://www.mediawiki.org/wiki/User:GWicke/Notes/Storage#Strawman_backend_API
 *
 * Interface is intended to be general, so that other backends can be dropped
 * in. We don't want inheritance though, just implementing the interface is
 * enough.
 */

var util = require('util'),
	events = require('events'),
	cass = require('node-cassandra-cql'),
	consistencies = cass.types.consistencies,
	uuid = require('node-uuid');

function CassandraRevisionStore (name, config, cb) {
	// call super
	events.EventEmitter.call(this);

	this.name = name;
	this.config = config;
	this.client = new cass.Client(config.backend.options);
	//this.client.on('log', function(level, message) {
	//	console.log('log event: %s -- %j', level, message);
	//});
	cb();
}

util.inherits(CassandraRevisionStore, events.EventEmitter);

var CRSP = CassandraRevisionStore.prototype;

/**
 * Add a new revision with several properties
 */
CRSP.addRevision = function (revision, cb) {
	var tid;
	if(revision.timestamp) {
		// Create a new, deterministic timestamp
		tid = uuid.v1({
			node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
			clockseq: 0x1234,
			msecs: new Date(revision.timestamp).getTime(),
			nsecs: 0
		});
	} else {
		tid = uuid.v1();
	}
	// Build up the CQL
	// Simple revison table insertion only for now
	var cql = 'BEGIN BATCH ',
		args = [],
		props = Object.keys(revision.props);
	cql += 'insert into revisions (name, prop, tid, revtid, value) ' +
			'values(?, ?, ?, ?, ?);\n';
	args = args.concat([
			revision.page.title,
			'_rev',
			tid,
			tid,
			new Buffer(JSON.stringify({rev:revision.id}))]);
	props.forEach(function(prop) {
		cql += 'insert into revisions (name, prop, tid, revtid, value) ' +
			'values(?, ?, ?, ?, ?);\n';
		args = args.concat([
			revision.page.title,
			prop,
			tid,
			tid,
			revision.props[prop].value]);
	});
	cql += 'APPLY BATCH;';
	function tidPasser(err, res) {
		cb(err, {tid: tid});
	}
	this.client.execute(cql, args, consistencies.one, tidPasser);
};

/**
 * Get the latest version of a given property of a page
 *
 * Takes advantage of the latest-first clustering order.
 */
CRSP.getLatest = function (name, prop, cb) {
	// Build the CQL
	var cql = 'select value from revisions where name = ? and prop = ? limit 1;',
		args = [name, prop];
	this.client.execute(cql, args, consistencies.one, cb);
};

module.exports = CassandraRevisionStore;
