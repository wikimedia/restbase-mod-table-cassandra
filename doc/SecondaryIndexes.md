# Secondary index definition in table schema
Example:
```javascript
{
    // Attributes are typed key-value pairs
    attributes: {
        key: 'string',
        tid: 'timeuuid',
        length: 'varint',
        value: 'string'
    },
    // primary index structure
    index: [
        { type: 'hash', attribute: 'key' },
        { type: 'range', order: 'desc', attribute: 'tid' }
    },
    // Optional secondary indexes on the attributes
    secondaryIndexes: {
        by_tid: {
            { type: 'hash', attribute: 'tid' },
            range: key, // implicit, all primary index attributes are included
            // Project some additional attributes into the secondary index
            { type: 'proj', attribute: 'length' }
        }
    }
}
```

# Secondary index updates

## Time-bucketed secondary index table(s)
Design considerations:

- Most accesses are for latest data
    - separate hot 'latest' data from cold historical data
- Need compact index scan without timestamp dilution for range queries
    - start out with an index for all values ever
    - possibly time-bucket indexes in the future for quickly-updated indexes
      like page length

### Index schema
- If the data table does not have a timeuuid as its last primary key member (a
  *version tid*), then we add one named `_tid`. This adds versioning to
  otherwise unversioned tables, which lets us use the same index update
  algorithms for both. We can purge old versions each time after updating all
  corresponding indexes. We'll have to skip over old versions in queries on
  this table to still provide the illusion of an unversioned table.
- If the index definition includes the version tid, then we just insert an
  index entry on each modification in the data table. There is no need to
  insert a `_tid` field or a `_deleleted` field in the index, and there is
  also no need to run the index maintenance described below.
- If the index definition does *not* include the version tid, then we add both
  `_tid` and `_deleted` non-key attributes as shown here:

```javascript
{
    table: 'idx_foo_ever',  // could build additional indexes for time buckets
    attributes: {
        // index attributes
        // remaining primary key attributes
        // any projected attributes
        _tid: 'timeuuid',       // tid of last matching entry
        _deleted: 'timeuuid'    // tid of deletion change or null
    },
    index: {
        hash: '{defined hash column, or string column fixed to index name}',
        range: ['{defined ranges}', '{remaining main table primary keys}']
    }
}
```

This is the case we are concerned with for the remainder of this description.

### Index writes
- Write new index entries as part of a logged cassandra batch on each update
  (`_deleted` = null), using the TIMESTAMP corresponding to the entry's tid
  (plus some entropy from tid? - check that nanoseconds aren't all 0!) for
  idempotency. Use local quorum consistency.
- After the main write, look at sibling revisions to update the index with
  values that no longer match (by setting `_deleted`):
    - select sibling revisions: lets say 3 before, 1 after
    - walk results in ascending order and diff each row vs. preceding row
        - if diff: for each index affected by that diff, update `_deleted` for
          old value using that revision's TIMESTAMP (to make these updates
          idempotent). If the value matches again at a later point, this write
          will be a no-op as the TIMESTAMP will be lower than the later write.

This method can also be used to rebuild the index from scratch (by selecting /
streaming *all* entries (for a given time range) and writing each index entry
with its TIMESTAMP). In that case, the diffing can be interleaved with the
index writes while streaming the data.

The number of siblings to consider for the index update can be tuned to
produce a high enough probability of a consistent result in face of some
updating process failures. The cost is the size of the read, and duplicate
writes for changed indexed items.

### Index reads
- Scan the index. For each result, cross-check vs. data iff:
    - `_deleted` == null or `_deleted` > query time, and a consistent read is
      requested
    - `_tid` > query time
- Read repair:
    - if `_tid` < query time and `_deleted` = null, but data doesn't match:
      rebuild index for item versions since `_tid` (which implicitly sets
      `_deleted`). To rule out race conditions vs. writes, we could perform
      these check reads with local quorum.

### Fast eventually consistent reads
Read requests using an index could be satisfied from the index only if all
requested attributes are either part of the key, or were projected into the
index via the proj property in the schema.

The issue with doing this is consistency. We write out all index entries for a
new table row along with the data, but don't immediately update index entries
for an earlier version of the same row which might now non longer match the
row's updated data. This means that index reads can return some false
positives until the index is updated.

For many applications occasionally getting some false positives in results is
an acceptable trade-off for the performance gain of avoiding cross-checking
each index result, so it seems to make sense to default to eventually
consistent index reads & offer more consistent reads on request.

### Index time bucketing
For indexes with fast-changing values, a single `_ever` index will accumulate
a lot of cruft with `_deleted` entries over time, which queries need to step
over.  Additionally, queries in the past are likely to only match a small
subset of the latest index entries. To avoid this, we can build indexes for
static time windows, e.g. a month as in '2012-12' using both the raw data and
the `_ever` index. This looks like a bit of work, but we can tackle & refine
this later.

## REST interface
General idea: `bucket//indexName/key1/..`
```
/v1/en.wikipedia.org/pages.rev//indexName/key1/key2/
  ?gt=foo&limit=10&ts_ge=20140312T20:22:33.3Z&ts_lt=20140312T20:22:33.3Z
   ^^ key3 range limit  ^^ time limit
```

## Related
- https://github.com/Netflix/s3mper
- http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra
- [CASSANDRA-2897: Secondary indexes without read-before-write](https://issues.apache.org/jira/browse/CASSANDRA-2897)
- http://jyates.github.io/2012/07/09/consistent-enough-secondary-indexes.html

Still somewhat related:

- [Article about built-in secondary indexes](http://www.wentnet.com/blog/?p=77)
    - only support equality, and are only efficient if # of expected results
      is roughly equal to the number of nodes; each query goes to all nodes
- [Old presentation on indexing in Cassandra; slides 35 to 46 are
  interesting](http://www.slideshare.net/edanuff/indexing-in-cassandra)
