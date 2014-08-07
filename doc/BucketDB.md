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
Dfferent partition & range index, maps to primary partition & range keys
- cassandra: second table with
  `PRIMARY KEY(<alternate partition>, <alternate range>, <orig
  partition>, <orig range>)`
- updates not atomic rel to primary, so ideally
    - create primary entry before adding index entries
    - remove index entries before deleting primary

### Accessing secondary indexes via HTTP
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo`

Equality match on the a range key:
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo/Bar`

Can also support range queries on the secondary index:
`/v1/en.wikipedia.org/pages.revisions//by-page/Foo/?gt=a&lt=b

Rationale for `//` as delimiter:
- empty partition keys are not permitted in cassandra anyway
- don't want collisions with other keys

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
    - potentiall others from
      http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html
- potentially per-item ACLs as attribute for filtering

## Revisioned blob
- guid as range index
- listing using distinct to filter out revisions
- need static 'latest revision' property for CAS
    - not a feature in DynamoDB

# Supporting MediaWiki oldids
Use cases: 
- retrieval by oldid: /v1/en.wikipedia.org/pages/Foo/html/12345
- listing of revisions per page: /v1/en.wikipedia.org/pages/Foo/revisions/`

Other goals:
- separate revision concept from other properties (otherwise end up with a lot
  of duplicated indexes)
- allow efficient lookup of `page -> oldid, tid` and `oldid -> page, tid`
    - primary access implicit in all by-oldid accesses: `oldid -> page, tid`
    - sounds like a table with secondary index

## Caching considerations for by-oldid accesses
Want to minimize round-trips (redirects) while keeping the caching / purging
manageable. Focus on round-trips especially for new content, perhaps more on
cache fragmentation for old content.

- resolve to UUID-based version internally, `return it`
    - if latest revision: needs to be purged
    - if old revision: can be cached, won't change any more (apart from
      security / version upgrades)
    - some cache fragmentation, but can set fairly short TTL as cache miss is
      cheap
- need time range for oldid: timeuuid of *next* oldid
    - so look for 2 oldids >= taget-oldid
        - if only one returned: latest

## Implementation idea
- separate revision bucket: `/v1/en.wikipedia.org/pages.revision/
- check if MW revision exists when referenced: 
    - if not: fetch revision info from API
        - need tid for revision
        - but will need by-timestamp retrieval support in parsoid & PHP
          preprocessor for accurate old revisions
