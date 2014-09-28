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

##### Variant: Additional index-only table
- add another table without versioning (no tid column), but with time
  bucketing to keep track of the available keys within this bucket:
  `<timebucket><primary key>`
- after establishing the possibly matching keys by querying on this table,
  proceed to perform one query per key including timestamp on the fully
  versioned index table (`<primary key><timeuuid>`) and filter out false
  positives
- need to figure out an algorithm to reliably maintain the bucket index
    - need to transfer entries from preceding bucket
    - could use union of matches while new bucket is being build; previous
      bucket will potentially have more false positives
- possible refinements:
    - keep some additional information about *latest* entry in unversioned
      table
- pro-con:
    - ++ small result set on first query (but some false positives likely)
    - ++ can do just enough timestamp-based lookups to satisfy limit for paging
    - + share versioned index table layout with non-range indexes
    - - more queries, likely higher latency for small result sets

#### WIP alternative: Summary table plus versions per partition key
- Most accesses are for latest data
    - separate hot 'latest' data from cold historical data
- Need compact index scan without timestamp dilution

```javascript
{
    table: 'idx_foo_ever',  // could build additional indexes for time buckets
    attributes: {
        // index attributes
        // remaining primary key attributes
        // any projected attributes
        _tid: 'timeuuid',       // tid of last matching entry
        // tid of last update to partition key; index entry deleted if > _tid
        _latest_tid: 'timeuuid' 
    },
    index: {
        hash: '{defined hash column, or string column fixed to index name}',
        range: ['{defined ranges}', '{remaining main table primary keys}'],
        // In the special case where the partition key of data matches that of
        // index we can make _latest_tid static, and don't have to check the
        // data table
        // static: _latest_tid
    }
}

{
    table: 'idx_foo_all',
    attributes: {
        // index attributes
        // remaining primary key attributes
        // any projected attributes
        _tid: 'timeuuid'
    },
    index: {
        hash: '{defined hash column, or string column fixed to index name}',
        range: ['{defined ranges}', '{remaining main table primary keys}',
        '_tid']
    }
}
```

##### Update algorithm
- Write to `_ever` and `_all` on each update, using timestamp & writetime for
  idempotency
- Perform an index roll-up on `_all` similar to the one discussed earlier; update `_latest_tid` in `_ever` whenever the indexed value was removed (again, with writetime matching the time of the deletion to avoid overwriting later insertions)

##### Read algorithm
- Scan `_ever`. For each result, compare `_tid` and `_latest_tid`. If
  `_latest_tid` < query time, check vs. data

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
