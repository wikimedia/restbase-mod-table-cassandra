'use strict';

const dbu = require('./dbutils');
const P = require('bluebird');
const stringify = require('fast-json-stable-stringify');

/**
 * Check if a schema part differs, and if it does, if the version was
 * incremented.
 * @param {Object} current { conf: {object}, version: {number} }
 * @param {Object} proposed { conf: {object}, version: {number} }
 * @return {boolean} Whether the attribute has changed.
 * @throws {Error} If the schema fragment changed, but the version was not
 * incremented.
 */
function confChanged(current, proposed) {
    if (stringify(current.conf) !== stringify(proposed.conf)) {
        if (current.version >= proposed.version) {
            const e = new Error('Schema change, but no version increment.');
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
class Unsupported {
    constructor(attr, options) {
        this.attr = attr;
        this.options = options;
    }

    validate(req, current, proposed) {
        if (stringify(current[this.attr]) !== stringify(proposed[this.attr])) {
            throw new Error(`${this.attr} migrations are unsupported`);
        }
    }
}

Unsupported.prototype.migrate = () => P.resolve();

/**
 * Table name handler
 */
class Table extends Unsupported {
    constructor() {
        super('table');
    }
}

/**
 * options object migration handler
 */
class Options {
    constructor(options) {
        this.options = options;
    }

    validate(req, current, proposed) {
        if (current._backend_version === proposed._backend_version &&
                confChanged({ conf: current.options, version: current.version },
                    { conf: proposed.options, version: proposed.version })) {
            return true;
        }
    }

    migrate(req, current, proposed) {
        const table = `${dbu.cassID(req.keyspace)}.${dbu.cassID(req.columnfamily)}`;
        const cql = `ALTER TABLE ${table} WITH ${dbu.getOptionCQL(proposed.options,
            this.options.db)}`;
        this.options.log('trace/alter_schema', cql);
        if (this.options.skip_schema_update) {
            return P.resolve();
        }
        return this.options.client.execute(cql, [], { consistency: req.consistency });
    }
}

/**
 * attributes object migration handler
 */
class Attributes {
    constructor(options) {
        this.options = options;
    }

    migrate(req, current, proposed) {
        const table = `${dbu.cassID(req.keyspace)}.${dbu.cassID(req.columnfamily)}`;
        const currSet = new Set(Object.keys(current.attributes));
        const propSet = new Set(Object.keys(proposed.attributes));
        const addColumns = Array.from(propSet).filter((x) => !currSet.has(x));
        const delColumns = Array.from(currSet).filter((x) => !propSet.has(x));
        return P.each(addColumns, (col) => {
            this.options.log('warn/schemaMigration/attributes', {
                message: `adding column${col}`,
                column: col
            });
            const cql = this._alterTableAdd(proposed, table, col);
            this.options.log('trace/alter_schema', cql);
            if (this.options.skip_schema_update) {
                return P.resolve();
            }
            return this.options.client.execute(cql, [], { consistency: req.consistency })
            .catch((e) => {
                if (!new RegExp(`Invalid column name ${col} because it ` +
                        'conflicts with an existing column').test(e.message)) {
                    throw (e);
                }
                // Else: Ignore the error if the column already exists.
            });

        })
        .then(() => P.each(delColumns, (col) => {
            this.options.log('warn/schemaMigration/attributes', {
                message: `dropping column ${col}`,
                column: col
            });
            const cql = `ALTER TABLE ${table} DROP ${dbu.cassID(col)}`;
            this.options.log('trace/alter_schema', cql);
            if (this.options.skip_schema_update) {
                return P.resolve();
            }
            return this.options.client.execute(cql, [], { consistency: req.consistency })
            .catch({ message: `Column ${col} was not found in table data` }, () => {
                // Ignore the error if the column was already removed.
            });
        }));
    }
}

// The only constraint here: that any attribute being dropped must not
// be part of an existing index, something which the standard schema
// validation already covers.
Attributes.prototype.validate = (req, current, proposed) =>
    confChanged({ conf: current.attributes, version: current.version },
        { conf: proposed.attributes, version: proposed.version });

Attributes.prototype._alterTableAdd = (proposed, table, col) => {
    let cql = `ALTER TABLE ${table} ADD ${dbu.cassID(col)} ` +
        `${dbu.schemaTypeToCQLType(proposed.attributes[col])}`;
    if (proposed.index && proposed.staticKeyMap[col]) {
        cql += ' static';
    }
    return cql;
};

/**
 * Index definition migrations
 */
class Index {
    constructor(options) {
        this.options = options;
    }

    validate(req, current, proposed) {
        if (confChanged({ conf: current.index, version: current.version },
            { conf: proposed.index, version: proposed.version })) {
            const addIndexes = proposed.index.filter((x) => !this._hasSameIndex(current.index, x));
            const delIndexes = current.index.filter((x) => !this._hasSameIndex(proposed.index, x));

            const alteredColumns = [];

            // If index added and the column existed previously, need to remove it and
            // add back to change index.  Not supported.
            addIndexes.forEach((index) => {
                if (current.attributes[index.attribute]) {
                    alteredColumns.push(index.attribute);
                }
            });

            // If index deleted the column is not deleted,
            // need to remove it and add back to change index.
            // Not supported.
            delIndexes.forEach((index) => {
                if (proposed.attributes[index.attribute]) {
                    alteredColumns.push(index.attribute);
                }
            });
            if (addIndexes.some((index) => index.type !== 'static') ||
                    delIndexes.some((index) => index.type !== 'static')) {
                throw new Error('Only static index additions and removals supported');
            }
            if (alteredColumns.length > 0) {
                throw new Error('Changing index on existing column not supported');
            }
            return true;
        } else {
            return false;
        }
    }
}

Index.prototype._hasSameIndex = (indexes, proposedIndex) =>
    indexes.some((idx) => idx.attribute === proposedIndex.attribute &&
            idx.type === proposedIndex.type &&
            idx.order === proposedIndex.order);

Index.prototype.migrate = () => {
};

/**
 * Migrator for the db module config.
 *
 * Primarily concerned with replication factor updates. Only triggers a
 * migration if the config version was incremented.
 */
class ConfigMigrator {
    constructor(options) {
        this.options = options;
    }

    migrate(req) {
        return this.options.db.updateReplicationIfNecessary(req.domain,
            req.query.table, req.query.options);
    }
}

ConfigMigrator.prototype.validate = (req, current, proposed) => {
    if (current._config_version > proposed._config_version) {
        throw new dbu.HTTPError({
            status: 400,
            body: {
                type: 'bad_request',
                title: 'Unable to downgrade storage module ' +
                    `configuration to version ${proposed._config_version}`,
                keyspace: req.keyspace,
                schema: proposed
            }
        });
    } else {
        // TODO: Catch config changes without version increments.
        return proposed._config_version > current._config_version;
    }
};

/**
 * Migrate the backend version.
 *
 * This is for internal upgrades, and should be applied before any other
 * upgrades.
 */
class BackendMigrator {
    constructor(options) {
        this.options = options;
    }

    migrate(req, current, proposed) {
        return this.options.db._migrateBackend(req, current, proposed);
    }
}

BackendMigrator.prototype.validate = (req, current, proposed) => {
    if (current._backend_version > proposed._backend_version) {
        throw new dbu.HTTPError({
            status: 400,
            body: {
                type: 'bad_request',
                title: 'Unable to downgrade storage backend ' +
                    `to version ${proposed._backend_version}`,
                keyspace: req.keyspace,
                schema: proposed
            }
        });
    } else {
        // TODO: Catch config changes without version increments.
        return proposed._backend_version > current._backend_version;
    }
};

const migrationHandlers = [
    // First, migrate the backend version.
    BackendMigrator,
    // Then, the config.
    ConfigMigrator,
    // Finally, the remaining schema elements.
    Table,
    Options,
    Attributes,
    Index
];

/**
 * Schema migrator.
 *
 */
class SchemaMigrator {
    constructor(options) {
        this.options = options;

        this.migrators = migrationHandlers.map((Klass) => new Klass(options));
    }

    /**
     * Perform any required migration tasks.
     * @param {req} req the request
     * @param {Object} current the current schema
     * @param {Object} proposed the proposed schema
     * @throws {Error} if the proposed migration fails to validate
     * @return {Promise} a promise that resolves when the migration tasks are complete
     */
    migrate(req, current, proposed) {
        // First phase: validate everything.
        const toMigrate = this.migrators.filter((migrator) =>
            // Migrators signal that something needs to be done by returning true,
            // and we are interested in the migrations that need to be applied.
            // If validation fails, the validator will throw & abort all
            // migrations.
            migrator.validate(req, current, proposed));

        // Everything went fine (no exceptions).
        if (!toMigrate.length) {
            // Nothing to do. Let the caller know.
            return Promise.resolve(false);
        } else {
            // Perform the migrations.
            return P.each(toMigrate, (migrator) => migrator.migrate(req, current, proposed))
            .then(() => // Indicate that we did indeed perform some migrations.
                true);
        }
    }
}

module.exports = SchemaMigrator;
