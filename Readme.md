# [RESTBase](https://github.com/gwicke/restbase) table storage on Cassandra

Provides a high-level table storage service abstraction similar to Amazon
DynamoDB or Google DataStore, with a Cassandra backend. See [the design
docs](https://github.com/gwicke/restbase-cassandra/tree/master/doc) for
details and background.

This is the default table storage backend for
[RESTBase](https://github.com/gwicke/restbase), and automatically installed as
an npm module dependency (`restbase-cassandra`). See the install instructions
there.
  
## Issue tracking

We use [Phabricator to track
issues](https://phabricator.wikimedia.org/maniphest/task/create/?projects=PHID-PROJ-xdgck5inpvozg2uwmj3f). See the [list of current issues in RESTBase-cassandra](https://phabricator.wikimedia.org/tag/restbase-cassandra/).

## Status

Preparing for production.

[![Build Status](https://travis-ci.org/gwicke/restbase-cassandra.svg?branch=master)](https://travis-ci.org/gwicke/restbase-cassandra)

Features:
- basic table storage service with REST interface, backed by Cassandra
- multi-tenant design: domain creation, prepared for per-domain ACLs
- table creation with declarative JSON schemas
- secondary index creation and basic maintenance
- data insertion and retrieval by primary key, including range queries

### Next steps
- More refined [secondary
  index](https://github.com/gwicke/restbase-cassandra/blob/master/doc/SecondaryIndexes.md)
  implementation
    - range queries on secondary indexes
- Refine HTTP interface & response formats, especially paging
- Authentication (OAuth2 / JWT / JWS / auth service callbacks) and ACLs
- [Transactions](https://github.com/gwicke/restbase-cassandra/blob/master/doc/Transactions.md):
  light-weight CAS and 2PC
- Get ready for production: robustness, performance, logging
- Basic schema evolution support

## Contributors
* Gabriel Wicke <gwicke@wikimedia.org>
* Hardik Juneja <hardikjuneja.hj@gmail.com>
