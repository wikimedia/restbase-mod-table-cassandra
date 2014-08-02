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

## Implementing KV buckets on top of db tables
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

### Revisioned blob
- guid as range index
- listing using distinct to filter out revisions
- need static 'latest revision' property for CAS
    - not a feature in DynamoDB
