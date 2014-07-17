-- ###########################################
-- Global tables
-- ###########################################

CREATE TABLE domains (
    app text, -- normally fixed as 'storoid'
    -- The domain name.
    name text,
    tid timeuuid, -- synchronization point
    meta text,
    -- Buckets <name, JSON metadata>
    buckets map<text, text>,
    PRIMARY KEY (app, name)
);
