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
- keyspace name: `a/<account>/<bucket>`

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
SELECT * from system.schema_columns where keyspace_name = 'testreducedb';
```
