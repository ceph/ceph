  $ ceph-kvstore-tool --help
  Usage: ceph-kvstore-tool <leveldb|rocksdb|bluestore-kv> <store path> command [args...]
  
  Commands:
    list [column/][prefix]
    list-crc [column/][prefix]
    dump [column/][prefix]
    exists [column/]<prefix> [key]
    get [column/]<prefix> <key> [out <file>]
    crc [column/]<prefix> <key>
    get-size [[column/]<prefix> <key>]
    set [column/]<prefix> <key> [ver <N>|in <file>]
    rm [column/]<prefix> <key>
    rm-prefix [column/]<prefix>
    list-columns
    store-copy <path> [num-keys-per-tx] [leveldb|rocksdb|...]
    store-crc <path>
    compact
    compact-prefix [column/]<prefix>
    compact-range [column/]<prefix> <start> <end>
    destructive-repair  (use only as last resort! may corrupt healthy data)
    stats
  

