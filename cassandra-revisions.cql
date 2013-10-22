CREATE KEYSPACE "testdb"   
    WITH REPLICATION = {'class' : 'SimpleStrategy',  'replication_factor' : 3 };

use testdb;


CREATE TABLE revisions (   
    name text,
    prop text,
    tid timeuuid,
    revtid timeuuid,
    props set<text>,
    headers map<text, text>, -- optional HTTP headers: used for format versioning etc
    value blob, -- static prop value
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

-------------------------------------------------------------------------------
-- Atomic update / edit conflict detection support
--
-- Not needed as long as this is handled in MySQL.
-------------------------------------------------------------------------------
--
--  Staging table for new revisions.
--  
--  On WRITE:
--  1) Content is first inserted here. 
--  2) The pages table is atomically updated (IF latestrev = <base revision>)
--  3) the matching timestamps are copied to the regular revisions table. 
--  4) Finally, the staging entries are deleted.
--  On READ:
--  * Reads for older revisions (>5 minutes or so) go directly to revisions.
--    Low read consistency needed.
--  * Reads for recent revisions or latest go through pages. Missing revisions
--    entries are tried on revisions_staging as well.
-- 
CREATE TABLE revisions_staging (   
    tid timeuuid, -- optimize for timeuuid access
    name text,
    prop text,
    props set<text>,
    headers map<text, text>, -- optional headers: used for format versioning etc
    value blob, 
    PRIMARY KEY (tid, name, prop)
);

-- CAS entry point for writes
CREATE TABLE pages (
    name text,
    latestrev timeuuid,
    -- The last 500 (or so) revisions for this name only. Atomically updated
    -- with the latestrev. Anything in this set is committed, so should be in
    -- revisions. If it is not found in revisions, then it needs to be moved
    -- from the staging table.
    lastrevs set<timeuuid>,
    PRIMARY KEY (name)
);
