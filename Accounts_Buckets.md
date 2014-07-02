# Design notes for account & bucket support

## Goals

### Account goals

- Namespace buckets to avoid naming conflicts
- Define per account:
    - ownership (can delete with all buckets when no longer needed)
    - bucket creation rights
    - quotas (perhaps)


### Bucket goals

- Can be created inside an account
- Provide a unit of storage of a specific `type`
- Namespace items to avoid naming conflicts
- Define globally per bucket:
    - ownership (can delete when the owner is gone)
    - access rights
    - quotas (perhaps)

## Implementation ideas

### Item read request flow

- Check that account exists (in-memory)
- Check that bucket exists (cached), load metadata (access rights)
    - Type of bucket
    - Check access rights
- access bucket table(s)

### Table naming scheme / mapping onto Cassandra

- keyspace is unit of replication
    - so map buckets onto keyspaces for per-bucket replication control
- dedicated `accounts` keyspace
- keyspace name
    - "Keyspace names are 32 or fewer alpha-numeric characters and
      underscores, the first of which is an alpha character."
    - But would like to allow longer account / bucket names
      (MySQL for example allows 64 byte db names)
    - use hash of full name `B<account>_<bucket>`, i.e. `Benwiki_pages`
```javascript
// Results in a 27 byte string [a-zA-Z0-9_], starting with 'B' to ensure that
// it starts with a letter as required by Cassandra (and as mnemonic)
'B' + crypto.Hash('sha1')
    .update(Math.random().toString()) // would normally use the bucket path + version
    .digest()
    .toString('base64')
    // Replace [+/] from base64 with _ (illegal in Cassandra)
    .replace(/[+\/]/g, '_')
    // Remove base64 padding, has no entropy
    .replace(/=+$/, '')
```

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
    - action <- group + service signature(s)
```json
{
    read: [
        {
            userGroups: {
                oneOf: [ '*', 'user', 'admin' ]
            }
        }
    ],
    write: [
        {
            userGroups: {
                oneOf: [ 'user', 'admin' ]
            },
            serviceSignatures: {
                oneOf: [ 'b7821dbca23b6f36db2bdcc3ba10075521999e6b' ]
            }
        }
    ]
}
```

## Format
