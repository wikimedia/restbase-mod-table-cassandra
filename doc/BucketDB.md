# Buckets as DB
Idea very similar to DynamoDB: 
- bucket is a collection of typed attributes
- one attribute is selected as hash / partition key
- optionally, second attribute can be nominated for range queries
    - cassandra: PRIMARY KEY(<partition>, <range>)

## Secondary indexes
Additional secondary indexes can be defined on projections of the attribute,
very similar to [the DynamoDB table schema
definition](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestSyntax).

### Local
Secondary range index within partition, maps to primary range index value
- can be implemented as second table with 
  `PRIMARY KEY (<partition>, <alternate range>, <orig range>)`

### Global
Different partition & range index, maps to primary partition & range keys
- cassandra: second table with
  `PRIMARY KEY(<alternate partition>, <alternate range>, <orig
  partition>, <orig range>)`
    - multiple entries per (<alternate partition>, <alternate range>) pair
      possible, as orig primary key included in new primary key
- updates not atomic rel to primary, so ideally
    - create primary entry before adding index entries
    - remove index entries before deleting primary

### Accessing secondary indexes via HTTP
Main table layout is by revision id:
`/v1/en.wikipedia.org/pages.revisions/12345/<tid>`

All revisions for a given page, using by-page secondary index:
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo`

Equality match on a range key:
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo/bd7a5a00-1f04-11e4-b312-c1e90310049f`

Can also support range queries on the secondary index:
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo/?gt=<starttime>&lt=<endtime>

Rationale for `//` as delimiter:
- empty partition keys are not permitted in cassandra anyway
- don't want collisions with other keys

## Secondary indexes vs. revisioning
Question: Index latest revisioned object only vs. index all entries.
- *All* is easier to implement in storage backend
- *Only latest* requires some knowledge of semantics / integration with
  conditional updates. Possibly easier to implement in something like a KV
  bucket handler.

## Concistency
### Unconditional put
Can use either insert or update.

### Conditional insert
- if not exists: `insert .. if not exists`

### Conditional update
- if exists / specific value: `update .. if tid = <etag>
    - can also test for null, but only on otherwise existing rows

# Implementing KV buckets on top of db tables
- 'key' & 'value' attributes
    - really only index matters
    - if 'value' property: returned on GET
- entity headers as individual attributes
    - content-type
    - content-length
    - content-sha1
    - location (for redirects)
    - guid / timestamp uuid (etag & last-modified)
    - potentially others from
      http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html
- potentially per-item ACLs as attribute for filtering

