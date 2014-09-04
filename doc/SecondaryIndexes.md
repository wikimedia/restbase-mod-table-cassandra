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
## Strategy: Read before write
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

## Strategy: versioned index + read repair
Basic idea is to insert multiple index entries by `tid` (a `timeuuid`), so
that the index essentially becomes versioned. Index layout: 
```javascript
{ 
    attributes: {
        consistentUpTo: 'timeuuid'
        // any projected attributes
    }, 
    index: {
        hash: '{defined hash column, or string column fixed to index name}',
        range: ['{defined ranges}', '{remaining main table primary keys}', 'tid'],
        static: 'consistentUpTo'
    }
}
```

- All index entries are written in batch on each write
- After main write, select items > consistentUpTo and insert app-level
  tombstones into index & update consistentUpTo in a single batch

On read:

- check if consistentUpTo is >= tid; return result if it is
- if not: check if there is a newer primary tid that doesn't match the index
    - if found: insert app-level tombstones
    - else: update consistentUpTo & return result

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
  (and daterange = '201401')
    - *partition key* determines nodes to query, so normally only 1-2 nodes
    - cassandra then narrows using the secondary index to get list of primary
      key matches (including range query). Not sure if it traverses *all* rows
      matching the partition key in the process. XXX: figure this out
    - can dynamically adjust the granularity based on the index; secondary
      index will be automatically rebuilt

### Pros / cons
- `++` big advantage: can use index for timestamp in past
- `++` easy to implement revision stuff like list of user contributions
- `+` no transactions required -> good for write throughput
- `-` more storage space required (but should compress well)
- `-` need to filter app-level tombstones on index read (but fairly
  straightforward)
- `-` some complexity especially around inserts of old tids

## Related work
- [CASSANDRA-2897: Secondary indexes without read-before-write](https://issues.apache.org/jira/browse/CASSANDRA-2897)

Still somewhat related:
- [Article about built-in secondary indexes](http://www.wentnet.com/blog/?p=77)
    - only support equality, and are only efficient if # of expected results
      is roughly equal to the number of nodes; each query goes to all nodes
- [Old presentation on indexing in Cassandra; slides 35 to 46 are
  interesting](http://www.slideshare.net/edanuff/indexing-in-cassandra)
