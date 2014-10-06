# Single-item transactions
CAS already supported for a single item, but dependent updates after success
not yet implemented. For this, need

- a log
- a replay job

Can likely share infrastructure with multi-item transactions below.

# Multi-item transactions
## Goals
- minimize contention: only conflict / retry if actual entries conflict
- ideally, allow transactions across an arbitrary set of entities within a
  domain

## 2PL with Wound/Wait

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
    - retry aquiring the lock (wound-wait) for a limited time or abort / retry
      current transaction if other timeuuid is older
- once everything is properly locked, atomically mark transaction as
  *committed* by inserting now() tid with CAS
    - perform all updates using commit tid
    - reset transactionids to null
    - finally delete transaction
- background job looks for outdated transactions & cleans up (by tid)
    - if committed, as above
    - else, clean up transactiontids
    - delete transaction

### Per-item readers
- Read latest (or at timestamp in past)
- No locking at any time

### Per-item writers
- CAS on *both* tid & transactiontid = null
- if both aren't equal:
    - check if transaction is still alive
        - run clean-up if it isn't
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

## Related
- [Original Cassandra CAS support bug](https://issues.apache.org/jira/browse/CASSANDRA-5062)
- [cages, zookeeper locking for Cassandra](https://code.google.com/p/cages/)
- [Wait chain algorithm](http://media.fightmymonster.com/Shared/docs/Wait%20Chain%20Algorithm.pdf)
- [Spinnaker paper](http://arxiv.org/pdf/1103.2408.pdf)
- [Google
  Spanner](https://www.usenix.org/system/files/conference/osdi12/osdi12-final-16.pdf)
- [Scalaris transaction
  paper](http://eprints.sics.se/3453/01/AtomicCommitment.pdf)

## Commercial services
- [Google DataStore
  transactions](https://developers.google.com/datastore/docs/concepts/transactions)
- [DynamoDB transaction library](http://java.awsblog.com/post/Tx13H2W58QMAOA7/Performing-Conditional-Writes-Using-the-Amazon-DynamoDB-Transaction-Library)
