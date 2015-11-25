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

### Features
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

## Configuration
Configuration of this module takes place from within an `x-modules` stanza in the YAML-formatted
[RESTBase configuration file](https://github.com/wikimedia/restbase/blob/master/config.example.wikimedia.yaml).
While complete configuration of RESTBase is beyond the scope of this document, (see the
[RESTBase docs](https://github.com/wikimedia/restbase) for that), this section covers the
[restbase-mod-table-cassandra](https://github.com/wikimedia/restbase-mod-table-cassandra) specifics.

```yaml
    - name: restbase-mod-table-cassandra
      version: 1.0.0
      type: npm
      options: # Passed to the module constructor
        conf:
          version: 1
          hosts: [localhost]
          username: cassandra
          password: cassandra
          defaultConsistency: localOne
          localDc: datacenter1
          datacenters:
            - datacenter1
          storage_groups:
            - name: default.group.local
              domains: /./
```

### Version
The version of this configuration.  Each edit of the module configuration must
correpond to a new, unique version.

*Note: Versions must be monotonically increasing.*

```yaml
    version: 1
```

### Hosts
A list of Cassandra nodes to use as contact points.

```yaml
    hosts:
      - cassandra-01.sample.org
      - cassandra-02.sample.org
      - cassandra-03.sample.org
```

### Credentials
Password credentials to use in authenticating with Cassandra.

*Note: Optional; Leave unconfigured if Cassandra authentication is not enabled.*

```yaml
    username: someuser
    password: somepass
```

### Default Consistency
The Cassandra consistency level to use when not otherwise specified.  Valid
values are those from the [nodejs driver for Cassandra](http://docs.datastax.com/en/drivers/nodejs/2.0/module-types.html#~consistencies).
Defaults to `localOne`.

```yaml
    defaultConsistency: localOne
```

### TLS
Key and certificate information for use in TLS-encrypted environments.  See the
[nodejs documentation on `tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_port_host_options_callback)
for the meaning of these directives.

*Note: Optional; Leave unconfigured if Cassandra client encryption is not enabled.*

```yaml
    tls:
      cert: /etc/restbase/tls/cert.pem
      key: /etc/restbase/tls/key.pem
      ca:
        - /etc/restbase/tls/root.pem
```

### Local Datacenter
[restbase-mod-table-cassandra](https://github.com/wikimedia/restbase-mod-table-cassandra)
uses a datacenter-aware connection pool.  The `localDc` directive instructs the module
which datacenter to treat as 'local' to this instance.  Cassandra nodes in the local
datacenter will be used for queries, and any others serve as a fallback.  Defaults to
`datacenter1` (the Cassandra default).

*Note: the `localDc` must be in the list of configured datacenters (see below).*

```yaml
    localDc: datacenter1
```

### Datacenters
The list of datacenters this Cassandra cluster belongs to.  Data will be replicated
across these datacenters accordingly.  Defaults to `[ datacenter1 ]`.

*Note: Changing this list alters the underlying Cassandra keyspaces in order to add
or remove datacenter replicas accordingly, but replication is NOT made retroactive.
You MUST perform a
[Cassandra repair](http://wiki.apache.org/cassandra/Operations?#Repairing_missing_or_inconsistent_data)
after adding a new datacenter to realize the
added redundancy.  Likewise, you must perform a cleanup to reclaim space if a
datacenter is removed.*

```yaml
    datacenters:
      - datacenter1
```

### Storage Groups
Storage groups are used to map tables to one or more hosts/domains.

```yaml
    storage_groups:
      - name: default.group.local
        domains: /./
```
