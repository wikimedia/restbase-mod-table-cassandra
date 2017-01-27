"use strict";

const P = require('bluebird');
const cass = require('cassandra-driver');
const TimeUuid = cass.types.TimeUuid;
const dbu = require('./dbutils');

class IndexRebuilder {
    constructor(db, req, secondaryKeys, timestamp) {
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
    diffRow(row) {
        const diff = {};
        let att;
        let i;
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
            diff
        };
    }

    /**
     * Diff each row against the preceding row. If they differ, then for each index
     * affected by the difference, update _del for old value using the revision's
     * timestamp.
     *
     * @param {object} row; a row object
     * @return a promise that resolves when the update is complete
     */
    handleRow(row) {
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
        // diff each row, return object of differing non-primary attributes
        const diffRes = this.diffRow(row);
        const diff = diffRes.diff;
        const idxSet = {};
        // Figure out which indexes need to be updated
        Object.keys(diff).forEach((diffAtt) => {
            const idxes = this.req.schema.attributeIndexes[diffAtt];
            if (idxes) {
                idxes.forEach((idx) => {
                    idxSet[idx] = true;
                });
            }
        });
        const queries = [];
        Object.keys(idxSet).forEach((idx) => {
            const reqAttributes = {};
            const secondarySchema = this.req.schema.secondaryIndexes[idx];
            Object.keys(secondarySchema.attributes).forEach((att) => {
                reqAttributes[att] = row[att];
            });
            // Write everything but _del with the corresponding data row's
            // timestamp
            const writeTime = dbu.tidNanoTime(row[this.req.schema.tid]);
            const idxReq = this.req.extend({
                query: {
                    attributes: reqAttributes,
                    // Add the timestamp clause
                    timestamp: writeTime
                },
                columnfamily: dbu.idxColumnFamily(idx),
                schema: secondarySchema
            });
            const queryObj = dbu.buildPutQuery(idxReq, true);
            queries.push(
                this.db.client.execute(queryObj.cql, queryObj.params,
                    { consistency: cass.types.consistencies.localOne, prepare: true })
                .catch((e) => {
                    this.db.log('error/table/cassandra/secondaryIndexUpdate', e);
                })
            );

            // Update _del, as this row doesn't match the index entry any more
            if (!diff.newKey) {
                const delReqAttributes = {};
                secondarySchema.iKeys.forEach((att) => {
                    delReqAttributes[att] = row[att];
                });
                delReqAttributes._del = this.prevRow[this.req.schema.tid];
                const delReq = idxReq.extend({
                    query: {
                        attributes: delReqAttributes,
                        timestamp: this.delWriteTimestamp
                    }
                });
                const delQueryObj = dbu.buildPutQuery(delReq, true);
                queries.push(
                    this.db.client.execute(delQueryObj.cql, delQueryObj.params,
                        { consistency: cass.types.consistencies.localOne, prepare: true })
                    .catch((e) => {
                        this.db.log('error/table/cassandra/secondaryIndexUpdate', e);
                    })
                );
            }

        });
        this.prevRow = row;

        return P.all(queries);
    }
}

module.exports = {
    IndexRebuilder
};
