# Rashomon
  
Prototype storage front-end implementing a part of
<https://www.mediawiki.org/wiki/User:GWicke/Notes/Storage>

## Status

Early prototype. Minimal storage and retrieval of revisioned blobs in Cassandra.

### Performance
Initial testing with ab, rashomon and cassandra on an aging laptop gives these results:

* 1800req/s for very small revisions
* 2Gbit throughput for large wikitext revisions like Barack Obama

Tests were performed using

    ab -n10000 -c80 http://localhost:8000/enwiki/page/Foo?rev/latest/wikitext

## Setup and usage

* Download cassandra from
  <http://planetcassandra.org/Download/StartDownload>
* Using cqlsh, create the keyspace and tables as documented in
  cassandra-revisions.cql
* Assuming you have node and npm installed, all that is left to do is:

```sh 
      npm install
      node rashomon.js
      # add a new revision
      curl -d "_timestamp=`date -Iseconds`&_rev=1234&wikitext=some wikitext `date -Iseconds`"\
        http://localhost:8000/enwiki/page/Foo?rev/
      # fetch the latest revision
      curl http://localhost:8000/enwiki/page/Foo?rev/latest/wikitext
      # fetch a specific MediaWiki revision ID:
      curl http://localhost:8000/enwiki/page/Foo?rev/1234/wikitext
      # fetch the wikitext at or before a given date
      curl http://localhost:8000/enwiki/page/Foo?rev/`date -Iseconds`/wikitext
      # fetch a specific uuid (adjust to uid returned when you added the revision)
      curl http://localhost:8000/enwiki/page/Foo?rev/6c745300-eb62-11e0-9234-0123456789ab/wikitext
```

### Troubleshooting
#### The server connection to Cassandra hangs when testing on localhost
On Debian, open /etc/cassandra/cassandra-env.sh and uncomment/edit this line
(localhost is key here):

    JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=localhost"

Restart cassandra. This might involve using kill, as the init scripts use the
same rmi connection to control cassandra. After this fix, the command

    nodetool status

should return information and show your node as being up.

## Contributors
* Gabriel Wicke <gwicke@wikimedia.org>
