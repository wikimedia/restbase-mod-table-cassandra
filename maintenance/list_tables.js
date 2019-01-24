'use strict';

const align     = require('string-align');
const P         = require('bluebird');
const cassandra = P.promisifyAll(require('cassandra-driver'));
const getConfig = require('./lib/index').getConfig;
const process   = require('process');

const PlainTextAuthProvider = cassandra.auth.PlainTextAuthProvider;

const yargs = require('yargs')
    .usage('Usage: $0 -H HOSTNAME\n' +
           'Print a Cassandra keyspace -to- RESTBase table name mapping')
    .options('h', { alias: 'help' })
    .options('H', {
        alias: 'hostname',
        describe: 'Cassandra hostname (contact node)',
        type: 'string',
        default: 'localhost'
    })
    .options('p', {
        alias: 'port',
        describe: 'Cassandra port number',
        type: 'number',
        default: 9042
    })
    .options('c', {
        alias: 'config',
        describe: 'RESTBase configuration file',
        type: 'string'
    });

const argv = yargs.argv;

if (argv.h) {
    yargs.showHelp();
    process.exit(0);
}

function getTableName(client, keyspace) {
    return client.execute(`SELECT value FROM "${keyspace}".meta WHERE key = 'schema' LIMIT 1`)
        .then((res) => {
            const val = res.rows[0].value;
            const obj = JSON.parse(val);
            return obj.table;
        });
}

const conf = getConfig(argv.config);

const clientOpts = {
    contactPoints: [ argv.hostname ],
    authProvider: new PlainTextAuthProvider(conf.username, conf.password),
    socketOptions: { connectTimeout: 10000 },
    promiseFactory: P.fromCallback
};

if (conf.tls) {
    clientOpts.sslOptions = conf.tls;
}

const client = new cassandra.Client(clientOpts);

return client.execute('SELECT keyspace_name,table_name FROM system_schema.tables;')
    .then((res) => {
        const keyspaces = [];
        for (const r of res.rows) {
            if (r.table_name === 'meta' && /\w+_T_\w+/.test(r.keyspace_name)) {
                keyspaces.push(r.keyspace_name);
            }
        }
        return keyspaces;
    })
    .then((keyspaces) => {
        const tuples = [];
        return P.each(keyspaces, (keyspace) => {
            return getTableName(client, keyspace)
                .then((table) => {
                    tuples.push([keyspace, table]);
                });
        }).then(() => { return tuples; });
    })
    .then((tuples) => {
        // Sort by value
        tuples.sort((a, b) => {
            return a[1] < b[1] ? -1 : (a[1] > b[1] ? 1 : 0);
        });

        // Print to console
        for (const i of tuples) {
            const keyspace = i[0];
            const table = i[1];
            console.log(align(keyspace, 50, 'left'), '|', table);
        }
    })
    .finally(() => client.shutdown());
