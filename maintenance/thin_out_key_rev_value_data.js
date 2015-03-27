"use strict";
var P = require('bluebird');
var cassandra = P.promisifyAll(require('cassandra-driver'));

if (!process.argv[3]) {
    console.error('Usage: node ' + process.argv[1] + ' <host> <keyspace>');
    process.exit(1);
}

var client = new cassandra.Client({
    contactPoints: [process.argv[2]],
    keyspace: process.argv[3]
});

var delay = 50; // One deletion every 50ms

var lastKey;
var lastKeyCount = 0;
function processRow (row) {
    // We include the revision in the key, so that we keep one render per
    // revision.
    var key = JSON.stringify([row._domain, row.key, row.rev]);
    if (key !== lastKey) {
        //if (lastKeyCount > 10) {
        //    console.log(lastKeyCount + ':' + lastKey);
        //}
        lastKey = key;
        lastKeyCount = 1;
        //console.log(row);
        // Don't delete the most recent render for this revision
        return P.resolve();
    } else {
        console.log(key, row.tid);
        lastKeyCount++;
        var delQuery = 'delete from data where "_domain" = :domain and key = :key and rev = :rev and tid = :tid';
        row.domain = row._domain;
        return client.executeAsync(delQuery, row, { prepare: true })
        .delay(delay);
    }

}

var query = 'select "_domain", key, rev, tid from data';

var stream = client.stream(query, [], {prepare: true, autoPage: true});

stream.once('readable', function consume () {
    function processRows() {
        var row = stream.read();
        if (row !== null) {
            return processRow(row)
            .then(processRows);
        } else {
            stream.once('readable', consume);
        }
    }
    return processRows();
});

stream.on('end', function() {
    console.log('All done!');
    process.exit(0);
});
