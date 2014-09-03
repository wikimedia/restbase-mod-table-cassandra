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

### Sample secondary index update request
```javascript
{
    

# Multi-item transactions
## Goals
- minimize contention: only conflict / retry if actual entries conflict
- ideally, allow transactions across an arbitrary set of entities within a
  domain

## Modified 2PL / 2PC

- prepare transaction by inserting using timeuuid into per-domain transaction
  table
    - limited validity period (1s?), can be extended by another second or so
      during transaction ('heartbeat'); goal: speedy failure detection while
      allowing slowish transaction execution during high traffic (up to 10s or so)
- transactional read & planned writes atomically update transactionid column
  in data table *if null*
- if not null
    - if other transaction timed out (plus clock uncertainty)
        - if not marked as committed: atomically replace timeuuid, clean up
          other transaction members
        - if marked as committed, but not marked as finished: replay
          transaction updates
    - abort *other* transaction if other timeuuid newer (atomic update on
      'abort' column)
    - abort / retry current transaction if other timeuuid is older
- once everything is properly locked, atomically mark transaction as
  *committed* by inserting now() tid
    - perform all updates using commit tid
    - reset transactionids to null
    - finally delete transaction
- background job looks for outdated transactions & cleans up (by tid)
    - if committed, as above
    - else, clean up transactiontids
    - delete transaction

### Per-item readers
- Read with tid <= now() for 
- No locking at any time

### Per-item writers
- CAS on *both* tid & transactiontid = null
- if both aren't equal:
    - check if transaction is still alive
        - run clean-up if 
    - else: transaction is in progress; return mismatch

### Table requirements
- static transactiontid column in all participating tables
- tid attribute uniformly named, so that it can be added on commit
    - maybe ignored when a table is not versioned?
- Transaction table per domain. Draft:

```javascript
{
    comment: 'Per-domain transaction table',
    name: 'domain.transactions',
    attributes: {
        tid: 'timeuuid',
        aborted: 'boolean', // false initially
        commitTimestamp: 'timeuuid', // null initially, non-null on commit
        members: 'set<json>', // cells locked in the transaction
        body: 'json'    // original transaction request, including potential
                        // post-transaction HTTP requests to be performed
                        // through restface
    },
    index: {
        hash: 'tid'
    }
}
```

## Cassandra transaction related
- [Original Cassandra CAS support bug](https://issues.apache.org/jira/browse/CASSANDRA-5062)
- [cages, zookeeper locking for Cassandra](https://code.google.com/p/cages/)
- [Wait chain algorithm](http://media.fightmymonster.com/Shared/docs/Wait%20Chain%20Algorithm.pdf)
- [Spinnaker paper](http://arxiv.org/pdf/1103.2408.pdf)
- [Scalaris transaction
  paper](http://eprints.sics.se/3453/01/AtomicCommitment.pdf)

## Commercial services
- [Google DataStore
  transactions](https://developers.google.com/datastore/docs/concepts/transactions)
- [DynamoDB transaction library](http://java.awsblog.com/post/Tx13H2W58QMAOA7/Performing-Conditional-Writes-Using-the-Amazon-DynamoDB-Transaction-Library)
