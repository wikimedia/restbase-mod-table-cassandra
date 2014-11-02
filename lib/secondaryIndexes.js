"use strict";

var cass = require('cassandra-driver');
var uuid = require('node-uuid');
var dbu = require('./dbutils');

function IndexRebuilder (db, keyspace, schema, secondaryKeys) {
    this.db = db;
    this.keyspace = keyspace;
    this.schema = schema;
    this.primaryKeys = schema.iKeys;
    this.secondaryKeys = secondaryKeys;

    this.prevRow = null;
}

/*
 * Diff non-primary attributes for index construction.
 * We assume reverse chronological order.
 * for each diff: find / update indexes
 *   _del is included to handle deleted primary table items
 */
IndexRebuilder.prototype.diffRow = function (row) {
    var res;
    if (!this.prevRow) {
        res = row;
    } else {
        var i, att;
        for (i = 0; i < this.primaryKeys.length; i++) {
            att = this.primaryKeys[i];
            if (this.prevRow[att] !== row[att]) {
                // Different data table primary key; return the full row
                return row;
            }
        }

        // Same data table primary, but possibly different non-index
        // attributes.
        res = {};
        for (i = 0; i < this.secondaryKeys.length; i++) {
            att = this.secondaryKeys[i];
            if (this.prevRow[att] !== row[att]) {
                res[att] = row[att];
            }
        }
    }
    if (row._del) {
        // Row is a tombstone; set the _del attribute in the index row to
        // the new row's timeuuid, which is always its last primary key
        // element.
        res._del = row[this.primaryKeys[this.primaryKeys.length - 1]];
    }
    return res;
};


IndexRebuilder.prototype.handleRow = function (n, row) {
    // diff each row, return object of differing non-primary attributes
    var diff = this.diffRow(row);
    var idxSet = {};
    // Figure out which indexes need to be updated
    for (var diffAtt in diff) {
        var idxes = this.schema.attributeIndexes[diffAtt];
        if (idxes) {
            idxes.forEach(function(idx) {
                idxSet[idx] = true;
            });
        }
    }
    for (var idx in idxSet) {
        var reqAttributes = {};
        var secondarySchema = this.schema.secondaryIndexes[idx];
        secondarySchema.iKeys.forEach(function(att) {
            reqAttributes[att] = row[att];
            if (reqAttributes[att] === undefined) {
                // default to null
                reqAttributes[att] = null;
            }
        });
        var idxReq = {
            attributes: reqAttributes
        };
        var queryObj = dbu.buildPutQuery(idxReq, this.keyspace,
                dbu.idxTable(idx), secondarySchema);
        // Add the writetime clause
        queryObj.query += ' USING TIMESTAMP ?';
        queryObj.params.push(uuid.v1time(row[this.schema.tid]));
        this.db.client.execute_p(queryObj.query, queryObj.params,
                { consistency: cass.types.consistencies.one, prepared: true })
        .catch(function(e) {
            console.error(e);
        });
    }
};

module.exports = {
    IndexRebuilder: IndexRebuilder
};
