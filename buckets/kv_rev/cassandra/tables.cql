CREATE TABLE IF NOT EXISTS %s.kv_rev (   
    key %s,
    tid timeuuid,
    headers map<text, text>,
    value %s,
    PRIMARY KEY (key, tid)
) WITH 
compression = { 
    'sstable_compression' : 'DeflateCompressor', 
    'chunk_length_kb': 256 
} and CLUSTERING ORDER BY (tid desc);

