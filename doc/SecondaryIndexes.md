# Secondary index definition in table schema
Example:
```javascript
{
    attributes: {
        key: 'string',
        tid: 'timeuuid',
        length: 'varint',
        value: 'string'
    },
    // primary index structure
    index: {
        hash: 'key',
        range: 'tid'
    },
    // This is where all secondary indexes are defined.
    secondaryIndexes: {
        by_tid: {
            hash: tid,
            range: key, // implicit, all primary index attributes are included
            // Project some additional attributes into the secondary index
            proj: ['length']
        }
    }
}
```

# Secondary index updates

## Strategy 1: Un-versioned, but bucketed secondary index table
- Most accesses are for latest data
    - separate hot 'latest' data from cold historical data
- Need compact index scan without timestamp dilution
    - start out with an index for all values ever
    - possibly time-bucket indexes in the future for quickly-updated indexes
      like page length

```javascript
// Add a static _idx_updated: 'timeuuid' field to the primary data table to
// track index update status

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

### Updates
- Write to `_ever` on each update (`_deleted` = null), using the TIMESTAMP
  corresponding to the entry's tid (plus some entropy from tid? - check!) for
  idempotency
- Perform an index roll-up similar to the one discussed earlier:
    - if `_idx_updated` <= TID: select primary key & indexed columns from
      data table with tid >= `_idx_updated` (using key portion up to &
      including any tid column)
    - else: select tid sibling entries only (two queries, each limit 1)
    - walk results backwards and diff each row vs. preceding row
        - if diff: for each index affected by that diff, update `_deleted` for
          old value using that revision's TIMESTAMP
    - finally, if insertion was new, atomically update `_idx_updated` *if not
      changed* (CAS)
        - set to the tid of the highest indexed row
        - while this fails:
            - wait for a second or two
            - repeat the process from original `_idx_updated`
            - then CAS vs. newly learned value
                - if that fails, but `_idx_updated` now at latest tid: exit
                  (another job succeeded)

This method can also be used to rebuild the index from scratch (by selecting /
streaming *all* entries and writing each index entry with its TIMESTAMP), or
to rebuild the index around an insertion in the past.

### Insertion of an entry with an old tid
Call the index rebuild method between the neighboring tids. This means that we
rely on the client doesn't go down while doing this. The assumption is that
insertions in the past will be rare, and we can schedule an occasional index
check / rebuild to catch any remaining issues.

### Reads
- Scan `_ever`. For each result, cross-check vs. data iff:
    - `_deleted` == null and a consistent read is requested
    - `_tid` > query time

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
For indexes with fast-changing values, the `_ever` index will accumulate a lot
of cruft with `_deleted` entries over time, which queries need to step over.
Additionally, queries in the past are likely to only match a small subset of
the latest index entries. To avoid this, we can build indexes for static time
windows, e.g. a month as in '2012-12' using both the raw data and the `_ever`
index. This looks like a bit of work, but we can tackle & refine this later.

## Strategy 2: versioned index + read repair
Basic idea is to insert multiple index entries by `tid` (a `timeuuid`), so
that the index essentially becomes versioned. Index layout: 
```javascript
{ 
    attributes: {
        // index attributes
        // remaining primary index attributes
        // any projected attributes
        __consistentUpTo: 'timeuuid',
        __tombstone: 'boolean'
    }, 
    index: {
        hash: '{defined hash column, or string column fixed to index name}',
        range: ['{defined ranges}', '{remaining main table primary keys}', 'tid'],
        static: '__consistentUpTo'
    }
}
```

- All index entries are written in batch on each write

### Insertion of a new entry with a current tid
Run the following index rebuild procedure asynchronously after main write:

- select primary key & indexed columns from data table with tid >=
  consistentUpTo and <= now()
- diff each row vs. preceding row
    - if no diff in a given index: delete index entry (prune duplicates)
    - if diff: insert entry with __tombstone: true for old value; upsert
      index entry for new value (to account for concurrent updates)
- finally, atomically update consistentUpTo *if not changed* (CAS)
    - set to the tid of the highest indexed row
    - while this fails:
        - wait for a second or two
        - repeat the process from original consistentUpTo
        - then CAS vs. newly learned value
            - if that fails, but consistentUpTo now at latest tid: exit
              (another job succeeded)

This method can also be used to rebuild the index from scratch, or to rebuild
the index around an insertion in the past. For this we'll want to use a
streaming query, as supported in [the node-cassandra-cql eachRow
method](https://github.com/jorgebay/node-cassandra-cql#clienteachrowquery-params-consistency-rowcallback-endcallback).

### Insertion of an entry with an old tid
Call the index rebuild method between the neighboring tids. This means that we
rely on the client doesn't go down while doing this. The assumption is that
insertions in the past will be rare, and we can schedule an occasional index
check / rebuild to catch any remaining issues.

### Read
- check if consistentUpTo is >= tid; return result if it is (or no result if
  tombstone)
- if not: 
    - Double-check against the data row at the requested timestamp.
    - Schedule an index rebuild from consistentUpTo.

### Issue: range requests on secondary index of revisioned table
A range request on one of the defined range indexes will fetch all matching
index entries, including old ones if the underlying data is revisioned. This
could potentially be a lot of entries.

Coalescing time ranges with identical index entries as described in the index
rebuild algorithm significantly reduces the number of index versions for
rarely-changing attributes. It will however not do much for an index on a
boolean that flips all the time. An example for this could be template link
table entries for procedural pages / templates, e.g. {{editprotected}}.

#### Idea: bucket index entries into time ranges
The basic idea is to add a `timebucket` column to the index table, and then
define a cassandra-native secondary index on this column. Range queries on the
index can then be done with `and timebucket = '201401'`, which reduces the
number of index versions.

Details:

- *partition key* determines nodes to query, so normally only 1-2 nodes
- A predicate on a secondary index column triggers query evaluation using
  the secondary index. This seems to scale fairly well, which suggests
  that the ranges are narrowed down quickly without scanning the full
  secondary index. Benchmarking with a table of 8 million rows & a native
  secondary index of cardinality 1 indicates that a range query completes
  in ~5ms, so that's pretty efficient.
- can dynamically adjust the granularity based on the indexed timebucket;
  secondary index will be automatically rebuilt
- will need to insert one index entry in each time bucket -- algorithm TBD


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

Still somewhat related:

- [Article about built-in secondary indexes](http://www.wentnet.com/blog/?p=77)
    - only support equality, and are only efficient if # of expected results
      is roughly equal to the number of nodes; each query goes to all nodes
- [Old presentation on indexing in Cassandra; slides 35 to 46 are
  interesting](http://www.slideshare.net/edanuff/indexing-in-cassandra)
