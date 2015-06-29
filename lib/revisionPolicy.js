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
    this.schema = schema;
    this.policy = schema.revisionRetentionPolicy;
    this.request = dbu.makeRawRequest(request, { ttl: this.policy.grace_ttl });
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

    // We want to update the current iteration row, so what needs to
    // be done is to resend the same row we have received here with a
    // new timestamp. Modifying self.request in-place is alright since
    // the requests are sequentially sent, and once executed, the request
    // object itself is not needed. Also, note that dbu.makeRawRequest()
    // creates a deep clone of self.request.query, so we are sure not to
    // modify the original incoming request
    var attrs = self.request.query.attributes;
    Object.keys(attrs).forEach(function(key) {
        attrs[key] = row[key];
    });
    self.request.query.timestamp = null;

    var query = dbu.buildPutQuery(self.request, true);
    var queryOptions = { consistency: self.request.consistency, prepare: true };

    return self.db.client.execute_p(query.cql, query.params, queryOptions)
    .catch(function(e) {
        self.db.log('error/table/cassandra/revisionRetentionPolicyUpdate', e);
    });
};

module.exports = {
    RevisionPolicyManager: RevisionPolicyManager
};
