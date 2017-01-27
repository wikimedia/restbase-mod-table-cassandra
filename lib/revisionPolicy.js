"use strict";

const dbu = require('./dbutils');
const P = require('bluebird');

/**
 * Applies a revision retention policy to a sequence of rows.
 *
 * @param {object} db; instance of DB
 * @param {object} request; request to use as baseline
 * @param {object} schema; the table schema
 * @param {Date} reqTime; the request time derived from request tid
 */
class RevisionPolicyManager {
    constructor(db, request, schema, reqTime) {
        this.db = db;
        this.schema = schema;
        this.policy = schema.revisionRetentionPolicy;
        this.request = dbu.makeRawRequest(request, { ttl: this.policy.grace_ttl });
        this.noop = this.policy.type === 'all' ||
            (this.policy.type === 'latest' && this.policy.count === 0);
        this.count = 0;
        if (this.policy.type === 'interval') {
            const interval = this.policy.interval * 1000;
            this.intervalLimitTime = reqTime - reqTime % interval;
        } else {
            this.intervalLimitTime = null;
        }
    }

    _setTtl(item) {
        this.request.query.attributes = item;

        const query = dbu.buildPutQuery(this.request, true);
        const queryOptions = { consistency: this.request.consistency, prepare: true };

        return this.db.client.execute(query.cql, query.params, queryOptions)
        .catch((e) => {
            this.db.log('error/table/cassandra/revisionRetentionPolicyUpdate', e);
        });
    }

    /**
     * Process one row in the sequence.
     *
     * @param {object} row; a row object.
     * @return a promise that resolves when the corresponding update is complete.
     */
    handleRow(row) {
        if (this.noop) {
            return P.resolve();
        }

        if (this.count < this.policy.count) {
            this.count++;
            return P.resolve();
        }

        if (this.policy.type === 'latest' || this.policy.type === 'latest_hash') {
            // We want to update the current iteration row, so what needs to
            // be done is to resend the same row we have received here with a
            // new timestamp. Modifying this.request in-place is alright since
            // the requests are sequentially sent, and once executed, the request
            // object itthis is not needed. Also, note that dbu.makeRawRequest()
            // creates a deep clone of this.request.query, so we are sure not to
            // modify the original incoming request
            if (row._ttl && row._ttl <= this.policy.grace_ttl) {
                return P.resolve();
            }
            return this._setTtl(row);
        } else if (this.policy.type === 'interval') {
            if (row[this.schema.tid].getDate() >= this.intervalLimitTime && !row._ttl) {
                return this._setTtl(row);
            } else {
                return P.resolve();
            }
        }
    }
}

module.exports = {
    RevisionPolicyManager
};
