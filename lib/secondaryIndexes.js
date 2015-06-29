"use strict";

var P = require('bluebird');
var cass = require('cassandra-driver');
var TimeUuid = cass.types.TimeUuid;
var dbu = require('./dbutils');

function IndexRebuilder (db, req, secondaryKeys, timestamp) {
    this.db = db;
    this.req = dbu.makeRawRequest(req);
    this.primaryKeys = this.req.schema.iKeys;
    this.secondaryKeys = secondaryKeys;

    this.prevRow = null;
    this.delWriteTimestamp = timestamp || dbu.tidNanoTime(TimeUuid.now());
}

/*
 * Diff non-primary attributes for index construction.
 * We assume reverse chronological order.
 * for each diff: find / update indexes
 *   _del is included to handle deleted primary table items
 */
IndexRebuilder.prototype.diffRow = function (row) {
    var diff = {};
    var i, att;
    for (i = 0; i < this.primaryKeys.length; i++) {
        att = this.primaryKeys[i];
        if (this.prevRow[att] !== row[att]) {
            // Different data table primary key; return the full row
            return {
                diff: row,
                newKey: true
            };
        }
    }

    // Same data table primary, but possibly different non-index
    // attributes.
    for (i = 0; i < this.secondaryKeys.length; i++) {
        att = this.secondaryKeys[i];
        if (this.prevRow[att] !== row[att]) {
            diff[att] = row[att];
        }
    }
    if (row._del) {
        // Row is a tombstone; set the _del attribute in the index row to
        // the new row's timeuuid, which is always its last primary key
        // element.
        diff._del = row[this.primaryKeys[this.primaryKeys.length - 1]];
    }
    return {
        diff: diff
    };
};

/**
 * Diff each row against the preceding row. If they differ, then for each index
 * affected by the difference, update _del for old value using the revision's
 * timestamp.
 *
 * @param {object} row; a row object
 * @return a promise that resolves when the update is complete
 */
IndexRebuilder.prototype.handleRow = function (row) {
    if (!this.prevRow) {
        // In normal operation there is no need to update the index for the
        // first row, as we are only interested in diffs, and the new data was
        // already written as part of the data write batch. This also does the
        // right thing for the data added by the request itself.
        //
        // We might however want to force an update for the first row in a
        // full index rebuild (see comments about reqTid in
        // db._rebuildIndexes).
        this.prevRow = row;
        return P.resolve();
    }
    var self = this;
    // diff each row, return object of differing non-primary attributes
    var diffRes = this.diffRow(row);
    var diff = diffRes.diff;
    var idxSet = {};
    // Figure out which indexes need to be updated
    for (var diffAtt in diff) {
        var idxes = this.req.schema.attributeIndexes[diffAtt];
        if (idxes) {
            idxes.forEach(function(idx) {
                idxSet[idx] = true;
            });
        }
    }
    var queries = [];
    for (var idx in idxSet) {
        var reqAttributes = {};
        var secondarySchema = this.req.schema.secondaryIndexes[idx];
        for (var att in secondarySchema.attributes) {
            reqAttributes[att] = row[att];
        }

        // Write everything but _del with the corresponding data row's
        // timestamp
        var writeTime = dbu.tidNanoTime(row[this.req.schema.tid]);
        var idxReq = self.req.extend({
            query: {
                attributes: reqAttributes,
                // Add the timestamp clause
                timestamp: writeTime
            },
            columnfamily: dbu.idxColumnFamily(idx),
            schema: secondarySchema
        });
        var queryObj = dbu.buildPutQuery(idxReq, true);
        queries.push(
            self.db.client.execute_p(queryObj.cql, queryObj.params,
                { consistency: cass.types.consistencies.one, prepare: true })
            .catch(function(e) {
                self.db.log('error/table/cassandra/secondaryIndexUpdate', e);
            })
        );

        // Update _del, as this row doesn't match the index entry any more
        if (!diff.newKey) {
            var delReqAttributes = {};
            secondarySchema.iKeys.forEach(function(att) {
                delReqAttributes[att] = row[att];
            });
            delReqAttributes._del = self.prevRow[self.req.schema.tid];
            var delReq = idxReq.extend({
                query: {
                    attributes: delReqAttributes,
                    timestamp: self.delWriteTimestamp
                }
            });
            var delQueryObj = dbu.buildPutQuery(delReq, true);
            queries.push(
                this.db.client.execute_p(delQueryObj.cql, delQueryObj.params,
                    { consistency: cass.types.consistencies.one, prepare: true })
                .catch(function(e) {
                    self.db.log('error/table/cassandra/secondaryIndexUpdate', e);
                })
            );
        }

    }
    this.prevRow = row;

    return P.all(queries);
};

module.exports = {
    IndexRebuilder: IndexRebuilder
};
