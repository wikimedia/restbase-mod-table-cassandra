"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');

/**
 * Applies a revision retention policy to a sequence of rows.
 *
 * @param {object} db; instance of DB
 * @param {object} request; request to use as baseline
 * @param {object} schema; the table schema
 */
function RevisionPolicyManager(db, request, schema) {
    this.db = db;
    this.request = request;
    this.schema = schema;
    this.policy = schema.revisionRetentionPolicy;
    this.noop = this.policy.type === 'all';
    this.count = 0;
}

/**
 * Process one row in the sequence.
 *
 * @param {object} row; a row object.
 * @return a promise that resolves when the corresponding update is complete.
 */
RevisionPolicyManager.prototype.handleRow = function(row) {
    var self = this;

    if (self.noop) {
        return P.resolve();
    }

    if (self.count < self.policy.count) {
        self.count++;
        return P.resolve();
    }

    var request = self.request.extend({ ttl: self.policy.grace_ttl });
    Object.keys(request.query.attributes).forEach(function(key) {
        request.query.attributes[key] = row[key];
    });
    request.query.timestamp = null;

    var query = dbu.buildPutQuery(request);
    var queryOptions = { consistency: request.consistency, prepare: true };

    return self.db.client.execute_p(query.cql, query.params, queryOptions)
    .catch(function(e) {
        self.db.log('error/table/cassandra/revisionRetentionPolicyUpdate', e);
    });
};

module.exports = {
    RevisionPolicyManager: RevisionPolicyManager
};
