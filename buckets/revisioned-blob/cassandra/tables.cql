CREATE KEYSPACE "testdb"   
    WITH REPLICATION = {'class' : 'SimpleStrategy',  'replication_factor' : 3 };

use testdb;


CREATE TABLE revisions (   
    name text,
    prop text,
    -- timestamp uuid for this part
    tid timeuuid,
    -- the revision timestamp uuid, so that we can list all properties or the
    -- latest value for a given property
    -- Example: select prop from revisions where name = 'Animal_Farm' and
    -- revtid = 6c745300-eb62-11e0-9234-0123456789ab;
    revtid timeuuid,
    value blob, -- prop value
    -- optional HTTP headers: used for format versioning etc
    -- Concent-type: application/json;schema=mediawiki.org..
    headers map<text, text>,
    PRIMARY KEY (name, prop, tid)
) WITH 
compression = { 
    'sstable_compression' : 'DeflateCompressor', 
    'chunk_length_kb': 256 
} and CLUSTERING ORDER BY (prop asc, tid desc);

-- Local secondary indexes, to be used with partition key
create index on revisions(tid);
create index on revisions(revtid);


-- Ordered indexes for range queries

-- page name and tid for a given revid
-- Updated on new revision. First insert if not exists revid / name, then
-- update tid if sucessful.
CREATE table idx_revisions_by_revid (
    revid int,
    name text,
    tid timeuuid,
    PRIMARY KEY (revid, name, tid)
) WITH 
compression = { 
    'sstable_compression' : 'LZ4Compressor', 
    'chunk_length_kb': 64 
} and CLUSTERING ORDER BY (name asc, tid desc);


-- Prop registry
-- Used for default content-types and other headers. Example: Make sure
-- Content-disposition is set explicitly on wikitext responses. See 
-- https://bugzilla.wikimedia.org/show_bug.cgi?id=28235#c6
CREATE TABLE props (
    for text,
    prop text,
    inherits boolean, -- whether a new revision inherits older values
    headers map<text, text>, -- Content-type etc
    PRIMARY KEY(for, prop)
);
