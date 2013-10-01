# Storoid
  
  Prototype storage front-end implementing a part of
  <https://www.mediawiki.org/wiki/User:GWicke/Notes/Storage>

## Status
  Early prototype. Minimal storage and retrieval of revisioned blobs in Cassandra.

## Setup and usage
  * Download cassandra from
    <http://planetcassandra.org/Download/StartDownload>
  * Using cqlsh, create the keyspace and tables as documented in
    cassandra-revisions.cql

```sh 
      npm install
      node storoid.js
      # add a new revision
      curl -d "_timestamp=`date -Iseconds`&_rev=1234&wikitext=some wikitext `date -Iseconds`"\
        http://localhost:8000/enwiki/page/Foo?rev/
      # fetch the latest revision
      curl http://localhost:8000/enwiki/page/Foo?rev/latest/wikitext
```

## Contributors
* Gabriel Wicke <gwicke@wikimedia.org>
