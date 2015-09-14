# [RESTBase](https://github.com/wikimedia/restbase) table storage on Cassandra

This projects provides a [high-level table storage service abstraction][spec]
similar to Amazon DynamoDB or Google DataStore on top of Cassandra. As the
production table storage backend for [RESTBase][restbase], it powers the
Wikimedia REST APIs, such as this one for the [English
Wikipedia](https://en.wikipedia.org/api/rest_v1/?doc).

For testing and small installs, there is also [a sqlite backend][sqlite]
implementing the same interfaces.

[restbase]: https://github.com/wikimedia/restbase
[sqlite]: https://github.com/wikimedia/restbase-mod-table-sqlite
  
## Issue tracking

We use [Phabricator to track
issues](https://phabricator.wikimedia.org/maniphest/task/create/?projects=PHID-PROJ-xdgck5inpvozg2uwmj3f). See the [list of current issues in restbase-mod-table-cassandra](https://phabricator.wikimedia.org/tag/restbase-cassandra/).

## Status

In production since March 2015.

[![Build Status](https://travis-ci.org/wikimedia/restbase-mod-table-cassandra.svg?branch=master)](https://travis-ci.org/wikimedia/restbase-mod-table-cassandra)
[![coverage status](https://coveralls.io/repos/wikimedia/restbase-mod-table-cassandra/badge.svg)](https://coveralls.io/r/wikimedia/restbase-mod-table-cassandra)

Features:
- basic table storage service with REST interface, backed by Cassandra,
    implementing [the RESTBase table storage interface][spec]
- multi-tenant design: domain creation, prepared for per-domain ACLs
- table creation with declarative JSON schemas
- [global secondary
    indexes](https://github.com/wikimedia/restbase-mod-table-cassandra/blob/master/doc/SecondaryIndexes.md)
    - index entries written in batch with main data write, superseded entries
        removed from indexes asynchronously / eventually consistent
        - support for strongly consistent reads at the cost of extra cross-checks
            with the main data table (not implemented yet)
    - range queries
    - projections
- limited automatic schema migrations
- multiple retention policies for limiting the MVCC history
- paging

[spec]: https://github.com/wikimedia/restbase-mod-table-spec


### TODO
- Secondary index refinements:
    - queries for columns not projected into the secondary index
    - full index rebuilds
    - [sharded ordered range
        indexes](https://phabricator.wikimedia.org/T112031)
- Possibly, some amount of [transaction support](https://github.com/wikimedia/restbase-mod-table-cassandra/blob/master/doc/Transactions.md)
- [Leverage Cassandra 3 materialized
    views](https://phabricator.wikimedia.org/T111746) where it makes sense,
    once those have stabilized.
