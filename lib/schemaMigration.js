"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var util = require('util');
var stringify = require('json-stable-stringify');

/**
 * Check if a schema part differs, and if it does, if the version was
 * incremented.
 *
 * @param {object} current: { conf: {object}, version: {number} }
 * @param {object} proposed: { conf: {object}, version: {number} }
 * @return {boolean} Whether the attribute has changed.
 * @throws {Error} If the schema fragment changed, but the version was not
 * incremented.
 */
function confChanged(current, proposed) {
    if (stringify(current.conf) !== stringify(proposed.conf)) {
        if (current.version >= proposed.version) {
            var e = new Error('Schema change, but no version increment.');
            e.current = current;
            e.proposed = proposed;
            throw e;
        }
        return true;
    } else {
        return false;
    }
}


/**
 * Base migration handler for unsupported schema
 */
function Unsupported(attr, options) {
    this.attr = attr;
    this.options = options;
}

Unsupported.prototype.validate = function(req, current, proposed) {
    if (stringify(current[this.attr]) !== stringify(proposed[this.attr])) {
        throw new Error(this.attr + ' migrations are unsupported');
    }
};

Unsupported.prototype.migrate = function() {
    return P.resolve();
};

/**
 * Table name handler
 */
function Table(options) {
    Unsupported.call(this, 'table');
}
util.inherits(Table, Unsupported);

/**
 * options object migration handler
 */
function Options(options) {
    this.options = options;
}

Options.prototype.validate = function(req, current, proposed) {
    if (confChanged({ conf: current.options, version: current.version },
                { conf: proposed.options, version: proposed.version })) {
        // Try to generate the options CQL, which implicitly validates it.
        dbu.getOptionCQL(proposed.options, this.options.db);
        return true;
    }
};

Options.prototype.migrate = function(req, current, proposed) {
    var table = dbu.cassID(req.keyspace) + '.' + dbu.cassID(req.columnfamily);
    var cql = 'ALTER TABLE ' + table + ' WITH ' + dbu.getOptionCQL(proposed.options,
            this.options.db);
    return this.options.client.execute_p(cql, [], { consistency: req.consistency });
};


/**
 * attributes object migration handler
 */
function Attributes(options) {
    this.options = options;
}

// The only constraint here: that any attribute being dropped must not
// be part of an existing index, something which the standard schema
// validation already covers.
Attributes.prototype.validate = function(req, current, proposed) {
    return confChanged({ conf: current.attributes, version: current.version },
                    { conf: proposed.attributes, version: proposed.version });
};

Attributes.prototype._alterTableAdd = function(proposed, table, col) {
    var cql = 'ALTER TABLE ' + table + ' ADD '
        + dbu.cassID(col) + ' ' + dbu.schemaTypeToCQLType(proposed.attributes[col]);
    if (proposed.index && proposed.staticKeyMap[col]) {
        cql += ' static';
    }
    return cql;
};

Attributes.prototype.migrate = function(req, current, proposed) {
    var table = dbu.cassID(req.keyspace) + '.' + dbu.cassID(req.columnfamily);
    var currSet = new Set(Object.keys(current.attributes));
    var propSet = new Set(Object.keys(proposed.attributes));
    var addColumns = Array.from(propSet).filter(function(x) { return !currSet.has(x); });
    var delColumns = Array.from(currSet).filter(function(x) { return !propSet.has(x); });
    var self = this;
    return P.each(addColumns, function(col) {
        self.options.log('warn/schemaMigration/attributes', {
            message: 'adding column' + col,
            column: col
        });
        var cql = self._alterTableAdd(proposed, table, col);
        return self.options.client.execute_p(cql, [], { consistency: req.consistency })
        .catch(function(e) {
            if (!new RegExp('Invalid column name ' + col
                        + ' because it conflicts with an existing column').test(e.message)) {
                throw(e);
            }
            // Else: Ignore the error if the column already exists.
        });

    })
    .then(function() {
        return P.each(delColumns, function(col) {
            self.options.log('warn/schemaMigration/attributes', {
                message: 'dropping column ' + col,
                column: col
            });
            var cql = 'ALTER TABLE ' + table + ' DROP ' + dbu.cassID(col);
            return self.options.client.execute_p(cql, [], { consistency: req.consistency })
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
function Index(options) {
    this.options = options;
}

Index.prototype.validate = function(req, current, proposed) {
    if (confChanged({ conf: current.index, version: current.version },
                { conf: proposed.index, version: proposed.version })) {
        var self = this;
        var addIndexes = proposed.index.filter(function(x) {
            return !self._hasSameIndex(current.index, x);
        });
        var delIndexes = current.index.filter(function(x) {
            return !self._hasSameIndex(proposed.index, x);
        });

        var alteredColumns = [];

        // If index added and the column existed previously, need to remove it and
        // add back to change index.  Not supported.
        addIndexes.forEach(function(index) {
            if (current.attributes[index.attribute]) {
                alteredColumns.push(index.attribute);
            }
        });

        // If index deleted the column is not deleted,
        // need to remove it and add back to change index.
        // Not supported.
        delIndexes.forEach(function(index) {
            if (proposed.attributes[index.attribute]) {
                alteredColumns.push(index.attribute);
            }
        });
        if (addIndexes.some(function(index) { return index.type !== 'static'; })
                || delIndexes.some(function(index) { return index.type !== 'static'; })) {
            throw new Error('Only static index additions and removals supported');
        }
        if (alteredColumns.length > 0) {
            throw new Error('Changing index on existing column not supported');
        }
        return true;
    } else {
        return false;
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
function SecondaryIndexes(options) {
    this.options = options;
}

SecondaryIndexes.prototype.validate = function(req, current, proposed) {
    if (confChanged({ conf: current.secondaryIndexes, version: current.version },
                { conf: proposed.secondaryIndexes, version: proposed.version })) {
        var addedIndexes = [];
        var deletedIndexes = [];
        var changedIndexes = [];

        new Set(Object.keys(current.secondaryIndexes)
            .concat(Object.keys(proposed.secondaryIndexes)))
            .forEach(function(indexName) {
                if (!proposed[indexName]) {
                    deletedIndexes.push(indexName);
                } else if (!current[indexName]) {
                    addedIndexes.push(indexName);
                } else if (stringify(current[indexName]
                            !== stringify(proposed[indexName]))) {
                    changedIndexes.push(indexName);
                }
            });
        if (addedIndexes.length > 0) {
            throw new Error('Adding secondary indices is not supported');
        }
        if (changedIndexes.length > 0) {
            throw new Error('Altering of secondary indices is not supported');
        }
        return true;
    }
};

SecondaryIndexes.prototype.migrate = function() {
    // Just update the metadata, actual table shouldn't be deleted
    // to avoid index update failtures on other nodes.
};


/**
 * Revision retention policy definition migrations
 */
function RevisionRetentionPolicy(options) {
    this.options = options;
}

// Nothing to do; There are no constraints on moving from one (valid)
// retention policy, to another.
RevisionRetentionPolicy.prototype.validate = function(req, current, proposed) {
    return confChanged({
        conf: current.revisionRetentionPolicy,
        version: current.version
    },
    {
        conf: proposed.revisionRetentionPolicy,
        version: proposed.version
    });
};

// Nothing to do.
RevisionRetentionPolicy.prototype.migrate = function() {
    this.options.log('warn/schemaMigration/revisionRetentionPolicy', {
        message: 'applying migration',
        proposed: this.proposed,
    });
    return P.resolve();
};


/**
 * Migrator for the db module config.
 *
 * Primarily concerned with replication factor updates. Only triggers a
 * migration if the config version was incremented.
 */
function ConfigMigrator(options) {
    this.options = options;
}

ConfigMigrator.prototype.validate = function(req, current, proposed) {
    if (current._config_version > proposed._config_version) {
        throw new dbu.HTTPError({
            status: 400,
            body: {
                type: 'bad_request',
                title: 'Unable to downgrade storage module configuration to version '
                    + proposed._config_version,
                keyspace: req.keyspace,
                schema: proposed
            }
        });
    } else {
        // TODO: Catch config changes without version increments.
        return proposed._config_version > current._config_version;
    }
};

ConfigMigrator.prototype.migrate = function(req, current, proposed) {
    return this.options.db.updateReplicationIfNecessary(req.domain,
                    req.query.table, req.query.options);
};


/**
 * Migrate the backend version.
 *
 * This is for internal upgrades, and should be applied before any other
 * upgrades.
 */
function BackendMigrator(options) {
    this.options = options;
}

BackendMigrator.prototype.validate = function(req, current, proposed) {
    if (current._backend_version > proposed._backend_version) {
        throw new dbu.HTTPError({
            status: 400,
            body: {
                type: 'bad_request',
                title: 'Unable to downgrade storage backend to version '
                    + proposed._backend_version,
                keyspace: req.keyspace,
                schema: proposed
            }
        });
    } else {
        // TODO: Catch config changes without version increments.
        return proposed._backend_version > current._backend_version;
    }
};

BackendMigrator.prototype.migrate = function(req, current, proposed) {
    return this.options.db.migrateBackend(req, current, proposed);
};



var migrationHandlers = [
    // First, migrate the backend version.
    BackendMigrator,
    // Then, the config.
    ConfigMigrator,
    // Finally, the remaining schema elements.
    Table,
    Options,
    Attributes,
    Index,
    SecondaryIndexes,
    RevisionRetentionPolicy
];

/**
 * Schema migrator.
 *
 */
function SchemaMigrator(options) {
    this.options = options;

    this.migrators = migrationHandlers.map(function(Klass) {
        return new Klass(options);
    });
}


/**
 * Perform any required migration tasks.
 *
 * @param
 * @throws  {Error} if the proposed migration fails to validate
 * @return a promise that resolves when the migration tasks are complete
 */
SchemaMigrator.prototype.migrate = function(req, current, proposed) {
    // First phase: validate everything.
    var toMigrate = this.migrators.filter(function(migrator) {
        // Migrators signal that something needs to be done by returning true,
        // and we are interested in the migrations that need to be applied.
        // If validation fails, the validator will throw & abort all
        // migrations.
        return migrator.validate(req, current, proposed);
    });

    // Everything went fine (no exceptions).
    if (!toMigrate.length) {
        // Nothing to do. Let the caller know.
        return Promise.resolve(false);
    } else {
        // Perform the migrations.
        var self = this;
        return P.each(toMigrate, function(migrator) {
            return migrator.migrate(req, current, proposed);
        })
        .then(function() {
            // Indicate that we did indeed perform some migrations.
            return true;
        });
    }
};

module.exports = SchemaMigrator;
