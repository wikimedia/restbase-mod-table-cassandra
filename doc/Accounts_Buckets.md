# Design notes for domain & bucket support 
## Goals

### Domain goals

- Namespace buckets to avoid naming conflicts
- Define per domain:
    - ownership (can delete with all buckets when no longer needed)
    - bucket creation rights
    - quotas (perhaps)


### Bucket goals

- Can be created inside an account
- Provide a unit of storage of a specific `type`
- Namespace items to avoid naming conflicts
- Define per bucket:
    - ACLs
    - quotas (perhaps)

## Implementation ideas

### Item read request flow

- Check that domain exists (in-memory or primary bucket)
- Check that bucket exists (cached or primary bucket), load metadata (access rights)
    - Type of bucket: need to know type before accessing it to select handler
        - detailed config can then be retrieved through handler (options in
          particular, access rights)
    - Check access rights
- call handler for bucket
    - access bucket table(s) / backend

### Table naming scheme / mapping onto Cassandra

- keyspace is unit of replication
    - so map buckets onto keyspaces for per-bucket replication control
    - for more generality, use underlying DynamoDB-like tables which map to
      keyspaces
- dedicated system tables for domains & buckets, prefixed with system domain
- keyspace name: see code

- List keyspaces

```sql
SELECT * from system.schema_keyspaces;

 keyspace_name | durable_writes | strategy_class                              | strategy_options
---------------+----------------+---------------------------------------------+----------------------------
        system |           True |  org.apache.cassandra.locator.LocalStrategy |                         {}
        testdb |           True | org.apache.cassandra.locator.SimpleStrategy | {"replication_factor":"3"}
  testreducedb |           True | org.apache.cassandra.locator.SimpleStrategy | {"replication_factor":"3"}
 system_traces |           True | org.apache.cassandra.locator.SimpleStrategy | {"replication_factor":"1"}

(4 rows)
```

- List tables in a keyspace

```sql
cqlsh> SELECT * from system.schema_columns where keyspace_name = 'testreducedb';
                                                                                                                                                                                                    
 keyspace_name | columnfamily_name | column_name | component_index | index_name | index_options | index_type | type           | validator                                                           
---------------+-------------------+-------------+-----------------+------------+---------------+------------+----------------+----------------------------------------------                       
  testreducedb |           commits |        hash |            null |       null |          null |       null |  partition_key |    org.apache.cassandra.db.marshal.BytesType                        
  testreducedb |           commits |    keyframe |               0 |       null |          null |       null |        regular |  org.apache.cassandra.db.marshal.BooleanType                        
  testreducedb |           commits |         tid |               0 |       null |          null |       null |        regular | org.apache.cassandra.db.marshal.TimeUUIDType                        
  testreducedb |           results |      result |               1 |       null |          null |       null |        regular |     org.apache.cassandra.db.marshal.UTF8Type                        
  testreducedb |           results |        test |            null |       null |          null |       null |  partition_key |    org.apache.cassandra.db.marshal.BytesType                        
  testreducedb |           results |         tid |               0 |       null |          null |       null | clustering_key | org.apache.cassandra.db.marshal.TimeUUIDType                        
  testreducedb |  revision_summary |      errors |               0 |       null |          null |       null |        regular |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |  revision_summary |       fails |               0 |       null |          null |       null |        regular |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |  revision_summary |    numtests |               0 |       null |          null |       null |        regular |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |  revision_summary |    revision |            null |       null |          null |       null |  partition_key |    org.apache.cassandra.db.marshal.BytesType                        
  testreducedb |  revision_summary |       skips |               0 |       null |          null |       null |        regular |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |     test_by_score |      commit |            null |       null |          null |       null |  partition_key |    org.apache.cassandra.db.marshal.BytesType                        
  testreducedb |     test_by_score |       delta |               0 |       null |          null |       null | clustering_key |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |     test_by_score |       score |               2 |       null |          null |       null |        regular |    org.apache.cassandra.db.marshal.Int32Type                        
  testreducedb |     test_by_score |        test |               1 |       null |          null |       null | clustering_key |    org.apache.cassandra.db.marshal.BytesType                        
  testreducedb |             tests |        test |            null |       null |          null |       null |  partition_key |    org.apache.cassandra.db.marshal.BytesType                        
                                                                                                                                                                                                    
(16 rows)
```

## Account information

- names and types of non-cassandra buckets (queues in particular)
    - `buckets map<text, text>` with value being type/version like 'revisions_1.0'
        - can be updated atomically during upgrade; clients reload read
          mapping on 'bucket does not exist', writes go to both during upgrade
    - need map [<account, <bucket name>] -> <type> in front-end
    - load on start-up or cache
- description
- later: bucket creation rights
    - by type?
    - quota ?

## Bucket metadata

- type
- ACL
```javascript
{
    read: [
        // A publicly readable bucket
        [
            // Any path in bucket (default: anything - so can be omitted)
            {
                type: "pathRegExp",
                re: '*'
            },
            {
                type: "role",
                anyOf: [ '*', 'user', 'admin' ]
            }
        ]
    ],
    write: [
        // Require both the user group & the service signature for writes
        [
            {
                type: "role",
                anyOf: [ 'user', 'admin' ]
            },
            {
                type: "serviceSignature",
                // Can require several service signatures here, for example to
                // ensure that a request was sanitized by all of them.
                allOf: [ 'b7821dbca23b6f36db2bdcc3ba10075521999e6b' ]
            }
        ]
    ]
}
```

### Using buckets for metadata storage
- domains, buckets: need listings, ideally without paging
- need cheap poll (global / per-bucket tids)
    - static column
```txt
storoid/
    domains -- array of domain & domain tids
    en.wikipedia.org -- metadata on domain incl. bucket info
```
- poll domains with if-none-match
    - if response, figure out which domain changed & retrieve it (or all)
- update via if-match on individual domain, with update to 'domains' following
    - would want reliable dependent update / batch mechanism
        - POST to primary bucket, add-on requests
        - transaction JSON structure, or multipart/related

#### Creating a new domain
```
PUT /v1/en.wikipedia.org
```
- normally ```If-None-Match: \*```
- send some default config
    - validate that
    - insert it into the domains bucket

#### Creating a new bucket in a domain
```PUT /v1/en.wikipedia.org/bucket```
- Call bucket handler to create the bucket (PUT ''; If-None-Match: *)
    - rashomon needs to look at the type in the request body to figure out the
      handler
- Insert the bucket into the domain; include the type in metadata

### Data format
- store per item for spec versioning (headers)
- compresses rather well

## Metadata updates
- Option 1: poll every <n> seconds
- Option 2: subscribe to system queue
    - requires Kafka or other queue backend

## Structure
### File system
```txt
buckets
    revisioned-blob
        index.js // HTTP handlers
        cassandra/index.js // Cassandra backend
backends
    cassandra
        index.js // Cassandra connection code
```
### Data
```javascript
var bucketType = 'revisioned-blob/cassandra';
var defaultUUID = new uuid();
var backends = {
    'cassandra/'+defaultUUID: new CassandraBackend(cassandraOptions),
};
// Alias default uuid
backends['cassandra/default'] = backends['cassandra/'+defaultUUID];

var handlers = {
    'revisioned-blob': require('./buckets/revisioned-blob/index')
};

// Set up backends on start-up -> need handler factory or the like

// On request
// bucketMeta has acls, type etc
handlers['revisioned-blob'](req, res, bucketOptions, backends['cassandra/default'])
```

### Handler factory in RestFace
- static handler: module exports object
- constructor: module exports constructor
    - new Handler({ conf: handlerConf, registerHandler: fn, log: fn })
        - exports same data structures as object
    - could use ability to change registration later (cb in options)

### Bucket creation
- default value by bucket type: 
  `cassandra -> defaultBackends.cassandra -> 'cassandra/<uuid>'`
- request parameter for explicit selection
    - although it probably makes more sense to map this in the front-end,
      using a different domain / front-end API

## Consistency

See also [the RestFace implemention notes](https://github.com/gwicke/restface/blob/master/doc/Implementation.md#lightweight-http-transaction-requests).

### Writes to single objects
- tid for each CAS entry point (objects), even non-revisioned ones
    - return as ETAG
    - might want separate meta tid for metadata updates (ACLs, headers etc)
        - <content tid>/<meta tid>
- CAS updates on tid

### Renames
- CAS on destination tid
- batch entry of revision in source
    - if other revision wins on source, rename entry will still be in history
      (tid is unique)

### Queues
- no special consistency protection; app-level idempotence encouraged (in jobs
  etc)

### HTTP transactions
Idea: encode primary & secondary requests in a single JSON structure
- `multipart/related` doesn't clearly support full HTTP headers per entity,
  and is harder to use from clients
- if primary fails, none of the dependents are executed

## Access to buckets / bucket naming
- bucket creation / access per user restricted to specific domains
    - similar to [Google's domain verification policy](https://developers.google.com/storage/docs/bucketnaming#verification)
- bucket names: no `.` allowed, so that we can map them to single domain
  components later

- Path: `en.wikipedia.org/api/v1/<bucket>` -> `/v1/en.wikipedia.org/<bucket>`
  in storage backend
- DNS (optional, maybe later): `<bucket>.en.wikipedia.org`; ex: `pages.en.wikipedia.org`


