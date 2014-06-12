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
    - so map buckets onto keyspaces
- dedicated `accounts` keyspace
- keyspace name: `B<account>_<bucket>`, i.e. `Benwiki_pages`
    - "Keyspace names are 32 or fewer alpha-numeric characters and
      underscores, the first of which is an alpha character."
    - Account `[a-zA-Z0-9]{1,15}`
    - Bucket `[a-zA-Z0-9]{1,15}`

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
