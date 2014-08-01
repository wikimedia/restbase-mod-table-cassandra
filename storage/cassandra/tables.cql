
CREATE TABLE IF NOT EXISTS %s.domains (
    -- Short domain id, max 8 chars [a-zA-Z][a-zA-Z0-9_]*
    id text,
    -- The full domain name
    domain text,
    tid timeuuid, -- synchronization point
    meta text,
    -- Buckets <name, JSON metadata>
    buckets map<text, text>,
    PRIMARY KEY (id, domain)
);
