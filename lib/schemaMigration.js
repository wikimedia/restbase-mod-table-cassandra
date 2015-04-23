"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var util = require('util');

var hash = dbu.makeSchemaHash;

/**
 * Base migration handler for unsupported schema
 */
function Unsupported(attr, current, proposed) {
    this.attr = attr;
    this.current = current;
    this.proposed = proposed;
}

Unsupported.prototype.validate = function() {
    if (hash(this.current) !== hash(this.proposed)) {
        throw new Error(this.attr + ' attribute migrations are unsupported');
    }
};

Unsupported.prototype.migrate = function() {
    return P.resolve();
};

/**
 * Table name handler
 */
function Table(db, current, proposed) {
    Unsupported.call(this, 'table', current, proposed);
}

util.inherits(Table, Unsupported);

/**
 * options object migration handler
 */
function Options(db, current, proposed) {
    Unsupported.call(this, 'options', current, proposed);
}

util.inherits(Options, Unsupported);

/**
 * attributes object migration handler
 */
function Attributes(db, current, proposed) {
    Unsupported.call(this, 'attributes', current, proposed);
}

util.inherits(Attributes, Unsupported);

/**
 * Index definition migrations
 */
function Index(db, current, proposed) {
    Unsupported.call(this, 'index', current, proposed);
}

util.inherits(Index, Unsupported);

/**
 * Secondary index definition migrations
 */
function SecondaryIndexes(db, current, proposed) {
    Unsupported.call(this, 'secondaryIndexes', current, proposed);
}

util.inherits(SecondaryIndexes, Unsupported);

/**
 * Revision retention policy definiation migrations
 */
function RevisionRetentionPolicy(db, current, proposed) {
    this.db = db;
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
    this.db.log('warn/schemaMigration/revisionRetentionPolicy', 'applying', this.proposed);
    return P.resolve();
};

/**
 * Version handling
 */
function Version(db, current, proposed) {
    this.db = db;
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
    this.db.log('warn/schemaMigration/version', this.current, '->', this.proposed);
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
function SchemaMigrator(db, current, proposed) {
    this.db = db;
    this.current = current;
    this.proposed = proposed;

    var self = this;
    this.migrators = Object.keys(migrationHandlers).map(function(key) {
        return new migrationHandlers[key](db, self.current[key], self.proposed[key]);
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
