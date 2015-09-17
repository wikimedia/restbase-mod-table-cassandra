"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var util = require('util');
var stringify = require('json-stable-stringify');


/**
 * Base migration handler for unsupported schema
 */
function Unsupported(attr, current, proposed) {
    this.attr = attr;
    this.current = current;
    this.proposed = proposed;
}

Unsupported.prototype.validate = function() {
    if (dbu.makeSchemaHash(this.current) !== dbu.makeSchemaHash(this.proposed)) {
        throw new Error(this.attr + ' attribute migrations are unsupported');
    }
};

Unsupported.prototype.migrate = function() {
    return P.resolve();
};

/**
 * Table name handler
 */
function Table(parentMigrator, current, proposed) {
    Unsupported.call(this, 'table', current, proposed);
}

util.inherits(Table, Unsupported);

/**
 * options object migration handler
 */
function Options(parentMigrator, current, proposed) {
    Unsupported.call(this, 'options', current, proposed);
}

util.inherits(Options, Unsupported);

/**
 * attributes object migration handler
 */
function Attributes(parentMigrator, current, proposed) {
    this.client = parentMigrator.db.client;
    this.log = parentMigrator.db.log;
    this.table = dbu.cassID(parentMigrator.req.keyspace)+'.'+dbu.cassID(parentMigrator.req.columnfamily);
    this.consistency = parentMigrator.req.consistency;
    this.proposedSchema = parentMigrator.proposed;
    this.current = current;
    this.proposed = proposed;

    var currSet = new Set(Object.keys(this.current));
    var propSet = new Set(Object.keys(this.proposed));

    this.addColumns = Array.from(propSet).filter(function(x) { return !currSet.has(x); });
    this.delColumns = Array.from(currSet).filter(function(x) { return !propSet.has(x); });
}

// The only constraint here: that any attribute being dropped must not
// be part of an existing index, something which the standard schema
// validation already covers.
Attributes.prototype.validate = function() {
    return;
};

Attributes.prototype._alterTable = function() {
    return 'ALTER TABLE '+this.table;
};

Attributes.prototype._colType = function(col) {
    return dbu.schemaTypeToCQLType(this.proposed[col]);
};

Attributes.prototype._alterTableAdd = function(col) {
    var cql = this._alterTable()+' ADD '+dbu.cassID(col)+' '+this._colType(col);
    if (this.proposedSchema.index && this.proposedSchema.staticKeyMap[col]) {
        cql += ' static';
    }
    return cql;
};

Attributes.prototype._alterTableDrop = function(col) {
    return this._alterTable()+' DROP '+dbu.cassID(col);
};

Attributes.prototype.migrate = function() {
    var self = this;
    return P.each(self.addColumns, function(col) {
        self.log('warn/schemaMigration/attributes', {
            message: 'adding column' + col,
            column: col
        });
        var cql = self._alterTableAdd(col);
        return self.client.execute_p(cql, [], { consistency: self.consistency })
        .catch(function(e) {
            if (!new RegExp('Invalid column name ' + col
                        + ' because it conflicts with an existing column').test(e.message)) {
                throw(e);
            }
            // Else: Ignore the error if the column already exists.
        });

    })
    .then(function() {
        return P.each(self.delColumns, function(col) {
            self.log('warn/schemaMigration/attributes', {
                message: 'dropping column ' + col,
                column: col
            });
            var cql = self._alterTableDrop(col);
            return self.client.execute_p(cql, [], { consistency: self.consistency })
            .catch(function(e) {
                if (e.message !== 'Column ' + col + ' was not found in table data') {
                    throw(e);
                }
                // Else: Ignore the error if the column was already removed.
            });
        });
    });
};

/**
 * Index definition migrations
 */
function Index(parentMigrator, current, proposed) {
    var self = this;
    self.current = current;
    self.proposed = proposed;
    self.currentSchema = parentMigrator.current;
    self.proposedSchema = parentMigrator.proposed;

    self.addIndex = proposed.filter(function(x) { return !self._hasSameIndex(self.current, x); });
    self.delIndex = current.filter(function(x) { return !self._hasSameIndex(self.proposed, x); });

    self.alteredColumns = [];

    // If index added and the column existed previously, need to remove it and add back to change index.
    // Not supported.
    self.addIndex.forEach(function(index) {
        if (self.currentSchema.attributes[index.attribute]) {
            self.alteredColumns.push(index.attribute);
        }
    });

    // If index deleted the column is not deleted, need to remove it and add back to change index.
    // Not supported.
    self.delIndex.forEach(function(index) {
        if (self.proposedSchema.attributes[index.attribute]) {
            self.alteredColumns.push(index.attribute);
        }
    });
}

Index.prototype.validate = function() {
    var self = this;
    if (self.addIndex.some(function(index) { return index.type !== 'static'; })
            || self.delIndex.some(function(index) { return index.type !== 'static'; })) {
        throw new Error('Only static index additions and removals supported');
    }
    if (self.alteredColumns.length > 0) {
        throw new Error('Changing index on existing column not supported');
    }
};

Index.prototype._hasSameIndex = function(indexes, proposedIndex) {
    return indexes.some(function(idx) {
        return idx.attribute === proposedIndex.attribute
                    && idx.type === proposedIndex.type
                    && idx.order === proposedIndex.order;
    });
};

Index.prototype.migrate = function() {
};


/**
 * Secondary index definition migrations
 */
function SecondaryIndexes(parentMigrator, current, proposed) {
    var self = this;
    this.client = parentMigrator.db.client;
    this.log = parentMigrator.db.log;
    this.keyspace = dbu.cassID(parentMigrator.req.keyspace);
    this.consistency = parentMigrator.req.consistency;

    self.addedIndexes = [];
    self.deletedIndexes = [];
    self.changedIndexes = [];

    new Set(Object.keys(current).concat(Object.keys(proposed))).forEach(function(indexName) {
        if (!proposed[indexName]) {
            self.deletedIndexes.push(indexName);
        } else if (!current[indexName]) {
            self.addedIndexes.push(indexName);
        } else if (!self._isEqual(current[indexName], proposed[indexName])) {
            self.changedIndexes.push(indexName);
        }
    });
}

SecondaryIndexes.prototype.validate = function() {
    var self = this;
    if (self.addedIndexes.length > 0) {
        throw new Error('Adding secondary indices is not supported');
    }
    if (self.changedIndexes.length > 0) {
        throw new Error('Altering of secondary indices is not supported');
    }
};

SecondaryIndexes.prototype.migrate = function() {
    // Just update the metadata, actual table shouldn't be deleted
    // to avoid index update failtures on other nodes.
};

SecondaryIndexes.prototype._isEqual = function(currentIndex, proposedIndex) {
    return stringify(currentIndex) === stringify(proposedIndex);
};

SecondaryIndexes.prototype._removeIndexTable = function(indexName) {
    var self = this;
    return 'drop table ' + self.keyspace + '.' + dbu.cassID(dbu.secondaryIndexTableName(indexName));
};

/**
 * Revision retention policy definiation migrations
 */
function RevisionRetentionPolicy(parentMigrator, current, proposed) {
    this.db = parentMigrator.db;
    this.current = current;
    this.proposed = proposed;
}

// Nothing to do; There are no constraints on moving from one (valid)
// retention policy, to another.
RevisionRetentionPolicy.prototype.validate = function() {
    return;
};

// Nothing to do.
RevisionRetentionPolicy.prototype.migrate = function() {
    this.db.log('warn/schemaMigration/revisionRetentionPolicy', {
        message: 'applying migration',
        proposed: this.proposed,
    });
    return P.resolve();
};

/**
 * Version handling
 */
function Version(parentMigrator, current, proposed) {
    this.db = parentMigrator.db;
    this.current = current;
    this.proposed = proposed;
}

// versions must be monotonically increasing.
Version.prototype.validate = function() {
    if (this.current >= this.proposed) {
        throw new Error('new version must be higher than previous');
    }
};

Version.prototype.migrate = function() {
    this.db.log('warn/schemaMigration/version', {
        current: this.current,
        proposed: this.proposed,
    });
    return P.resolve();
};

var migrationHandlers = {
    table: Table,
    options: Options,
    attributes: Attributes,
    index: Index,
    secondaryIndexes: SecondaryIndexes,
    revisionRetentionPolicy: RevisionRetentionPolicy,
    version: Version
};

/**
 * Schema migration.
 *
 * Accepts arguments for the current, and proposed schema as schema-info
 * objects (hint: the output of dbu#makeSchemaInfo).  Validation of the
 * proposed migration is performed, and an exception raised if necessary.
 * Note: The schemas themselves are not validated, only the migration; schema
 * input should be validated ahead of time using
 * dbu#validateAndNormalizeSchema).
 *
 * @param  {object] client; an instance of DB
 * @param  {object} schemaFrom; current schema info object.
 * @param  {object} schemaTo; proposed schema info object.
 * @throws  {Error} if the proposed migration fails to validate
 */
function SchemaMigrator(db, req, current, proposed) {
    this.db = db;
    this.req = req;
    this.current = current;
    this.proposed = proposed;

    var self = this;
    this.migrators = Object.keys(migrationHandlers).map(function(key) {
        return new migrationHandlers[key](self, current[key], proposed[key]);
    });

    this._validate();
}

SchemaMigrator.prototype._validate = function() {
    this.migrators.forEach(function(migrator) {
        migrator.validate();
    });
};

/**
 * Perform any required migration tasks.
 *
 * @return a promise that resolves when the migration tasks are complete
 */
SchemaMigrator.prototype.migrate = function() {
    return P.each(this.migrators, function(migrator) {
        return migrator.migrate();
    });
};

module.exports = SchemaMigrator;
