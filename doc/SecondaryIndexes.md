# Secondary index definition in table schema
Example:
```javascrip
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

## Selected strategy: versioned index + read repair
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

### First iteration: Just double-check on read 
Double-check each index hit against the data row at requested timestamp, and
keep adding results from index query until enough have checked out to satisfy
the limit.

### Longer term: Actually maintain a proper index

#### Insertion of a new entry with a current tid
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

#### Insertion of an entry with an old tid
Call the index rebuild method between the neighboring tids. This means that we
rely on the client doesn't go down while doing this. The assumption is that
insertions in the past will be rare, and we can schedule an occasional index
check / rebuild to catch any remaining issues.

#### Read
- check if consistentUpTo is >= tid; return result if it is (or no result if
  tombstone)
- if not: 
    - Double-check against the data row at the requested timestamp.
    - Schedule an index rebuild from consistentUpTo.

### Issue: Write of old versions / tids ('back-fill')
- need to also insert tombstones *after* the new entry if next entry in the
  main table doesn't match the index

### Issue: range requests on secondary index of revisioned table
A range request on one of the defined range indexes will fetch all matching
index entries, including old ones if the underlying data is revisioned. This
could potentially be a lot of entries.

One approach to reduce the number of entries would be to prune sequential
identical entries into larger ranges as part of the read repair / tombstone
insertion process. This would drastically reduce the number of entries for
rarely-changing attributes.

It would however not do much for an index on a boolean that flips all the
time. This might however be rare enough to not matter?

Possible problem cases:

- template link table entries for procedural pages / templates, e.g.
  {{editprotected}}

Further ideas:
- use a built-in secondary index on a date range to narrow down the index entries
  (and timebucket = '201401')
    - *partition key* determines nodes to query, so normally only 1-2 nodes
    - A predicate on a secondary index column triggers query evaluation using
      the secondary index. This seems to scale fairly well, which suggests
      that the ranges are narrowed down quickly without scanning the full
      secondary index. Benchmarking with a table of 8 million rows & a native
      secondary index of cardinality 1 indicates that a range query completes
      in ~5ms, so that's pretty efficient.
    - can dynamically adjust the granularity based on the indexed timebucket;
      secondary index will be automatically rebuilt

### Pros / cons
- `++` big advantage: can use index for timestamp in past
- `++` easy to implement revision stuff like list of user contributions
- `+` no transactions required -> good for write throughput
- `-` more storage space required (but should compress well)
- `-` need to filter app-level tombstones on index read (but fairly
  straightforward)
- `-` some complexity especially around inserts of old tids

## Alternate considered strategy: Read before write
Retrieve original data (if any) & figure out necessary index updates by
looking for changed indexed attributes. Schedule those as dependent updates in
an internal 'light-weight' transaction.

### Consistency issue
Retried index updates should not result in an inconsistent index.

Example execution:

1. T1 partly successful
2. T2 successful
3. T1 secondary updates retried

Results:
- possibly lost index entries if T1 removed entries that T2 added
- possibly extra index entries if T1 added entries that T2 removed

#### Solution
Assign a writetime to the entire transaction, and use this for
both the primary & all retried dependent updates. The fixed writetime makes
dependent updates idempotent. Re-execute dependents of both T1 and T2
in-order.
    
### Performance issue
The Paxos run in each 'light-weight' transaction involves four round-trips
between the coordinator and the replicas, so is going to be quite slow. Writes
to every table with secondary indexes defined would take this substantial
performance hit. Can we avoid this if a CAS on the primary is normally not
needed (as in revisioned buckets)?

## Related work
- [CASSANDRA-2897: Secondary indexes without read-before-write](https://issues.apache.org/jira/browse/CASSANDRA-2897)

Still somewhat related:
- [Article about built-in secondary indexes](http://www.wentnet.com/blog/?p=77)
    - only support equality, and are only efficient if # of expected results
      is roughly equal to the number of nodes; each query goes to all nodes
- [Old presentation on indexing in Cassandra; slides 35 to 46 are
  interesting](http://www.slideshare.net/edanuff/indexing-in-cassandra)
