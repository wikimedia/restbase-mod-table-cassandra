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
    // The latest / just inserted row is not processed by the retentionPolicy,
    // so start with count 1.
    this.count = 1;
    if (this.policy.type === 'interval') {
        var interval = this.policy.interval * 1000;
        var tidTime = this.request.query.timestamp;
        this.intervalLimitTime = tidTime - tidTime % interval;
    } else {
        this.intervalLimitTime = null;
    }
}

RevisionPolicyManager.prototype._setTtl = function(item) {
    var self = this;
    self.request.query.attributes = item;
    self.request.query.timestamp = null;

    var query = dbu.buildPutQuery(self.request, true);
    var queryOptions = {consistency: self.request.consistency, prepare: true};

    return self.db.client.execute_p(query.cql, query.params, queryOptions)
    .catch(function(e) {
        self.db.log('error/table/cassandra/revisionRetentionPolicyUpdate', e);
    });
};

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

    if (this.policy.type === 'latest') {
        // We want to update the current iteration row, so what needs to
        // be done is to resend the same row we have received here with a
        // new timestamp. Modifying self.request in-place is alright since
        // the requests are sequentially sent, and once executed, the request
        // object itself is not needed. Also, note that dbu.makeRawRequest()
        // creates a deep clone of self.request.query, so we are sure not to
        // modify the original incoming request
        if (row._ttl && row._ttl <= this.policy.grace_ttl) {
            return P.resolve();
        }
        return self._setTtl(row);
    } else if (this.policy.type === 'interval') {
        if (row[this.schema.tid].getDate() >= this.intervalLimitTime && !row._ttl) {
            return self._setTtl(row);
        } else {
            return P.resolve();
        }
    }
};

module.exports = {
    RevisionPolicyManager: RevisionPolicyManager
};
