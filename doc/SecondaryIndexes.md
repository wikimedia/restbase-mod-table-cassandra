# Secondary index definition
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
- Indexes are versioned (tid); have a static 'consistentUpTo' column
  defaulting to null
- All index entries are written in batch on each write
- After main write, select items > consistentUpTo and insert app-level
  tombstones into index & update consistentUpTo in a single batch

On read
- check if consistentUpTo is >= tid; return result if it is
- if not: check if there is a newer primary tid that doesn't match the index
    - if found: insert app-level tombstones
    - else: update consistentUpTo & return result

### Issue: Write of old versions / tids ('back-fill')
- need to also insert tombstones *after* the new entry

### Pros / cons
- `++` big advantage: can use index for timestamp in past
- `+` no transactions required -> good for write throughput
- `-` more storage space required (but should compress well)
- `-` need to filter app-level tombstones on index read (but fairly
  straightforward)
- `-` some complexity especially around inserts of old tids
