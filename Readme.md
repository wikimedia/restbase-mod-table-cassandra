# [RESTBase](https://github.com/gwicke/restbase) table storage service backed by Cassandra 

Provides a high-level table storage service abstraction similar to Amazon
DynamoDB or Google DataStore, with a Cassandra backend. See [the design
docs](https://github.com/gwicke/restbase-cassandra/tree/master/doc) for
details and background.
  
## Status
Prototype, not quite ready for production yet. Is automatically installed
along with [RESTBase](https://github.com/gwicke/restbase).

Features:
- basic table storage service with REST interface, backed by Cassandra
- multi-tenant design: domain creation, prepared for per-domain ACLs
- table creation with declarative JSON schemas
- secondary index creation and basic maintenance
- data insertion and retrieval by primary key, including range queries

### Next steps
- More refined secondary index implementation
    - range queries on secondary indexes
- Refine HTTP interface & response formats, especially paging
- Authentication (OAuth2 / JWT / JWS) and ACLs
- [Transactions](https://github.com/gwicke/restbase-cassandra/blob/master/doc/Transactions.md): light-weight CAS and 2PC
- Get ready for production: robustness, performance, logging

## Contributors
* Gabriel Wicke <gwicke@wikimedia.org>
* Hardik Juneja <hardikjuneja.hj@gmail.com>
