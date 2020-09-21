  $ ceph-kvstore-tool --help
  Usage: ceph-kvstore-tool <leveldb|rocksdb|bluestore-kv> <store path> command [args...]
  
  Commands:
    list [prefix]
    list-crc [prefix]
    dump [prefix]
    exists <prefix> [key]
    get <prefix> <key> [out <file>]
    crc <prefix> <key>
    get-size [<prefix> <key>]
    set <prefix> <key> [ver <N>|in <file>]
    rm <prefix> <key>
    rm-prefix <prefix>
    store-copy <path> [num-keys-per-tx] [leveldb|rocksdb|...] 
    store-crc <path>
    compact
    compact-prefix <prefix>
    compact-range <prefix> <start> <end>
    destructive-repair  (use only as last resort! may corrupt healthy data)
    stats
  
