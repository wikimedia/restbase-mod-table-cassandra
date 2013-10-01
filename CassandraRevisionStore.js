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
	helenus = require('helenus');

function CassandraRevisionStore (name, config, cb) {
	// call super
	events.EventEmitter.call(this);

	this.name = name;
	this.config = config;
	var options = new Object(config.backend.options);
	options.consistencylevel = helenus.ConsistencyLevel.ONE;

	this.pool = new helenus.ConnectionPool(options);
	this.pool.on('error', function(err){
		console.error(err.name, err.message);
		// emit error event so that the store can remove the backend or the
		// like
		this.emit('error', err);
	});
	this.pool.connect(cb);
}

util.inherits(CassandraRevisionStore, events.EventEmitter);

var CRSP = CassandraRevisionStore.prototype;

CRSP.addRevision = function (revision, cb) {
	// Create a new timestamp
	var tid = new helenus.TimeUUID.fromTimestamp(new Date(revision.timestamp));
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
	this.pool.cql(cql, args, cb);
};

CRSP.getLatest = function (name, prop, cb) {
	// Build the CQL
	var cql = 'select value from revisions where name = ? and prop = ? limit 1;',
		args = [name, prop];
	this.pool.cql(cql, args, cb);
};

module.exports = CassandraRevisionStore;
