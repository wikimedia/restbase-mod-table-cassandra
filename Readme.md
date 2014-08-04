# Rashomon
  
Prototype storage front-end implementing a part of
<https://www.mediawiki.org/wiki/User:GWicke/Notes/Storage>

Implements a storage backend for
[RestFace](https://github.com/gwicke/restface). See the install instructions
there.

## Status

Early prototype. Minimal storage and retrieval of revisioned blobs in Cassandra.

[![Build
Status](https://travis-ci.org/gwicke/rashomon.svg?branch=master)](https://travis-ci.org/gwicke/rashomon)

## Performance
Initial testing with ab, rashomon and cassandra on an aging laptop gives these results:

* 1800req/s for very small revisions
* 2Gbit throughput for large wikitext revisions like Barack Obama

## Troubleshooting
### The server connection to Cassandra hangs when testing on localhost
On Debian, open /etc/cassandra/cassandra-env.sh and uncomment/edit this line
(localhost is key here):

    JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=localhost"

Restart cassandra. This might involve using kill, as the init scripts use the
same rmi connection to control cassandra. After this fix, the command

    nodetool status

should return information and show your node as being up.

### Contributors
* Gabriel Wicke <gwicke@wikimedia.org>
