set(kv_srcs
  KeyValueDB.cc
  RocksDBStore.cc
  KeyValueHistogram.cc
  rocksdb_cache/ShardedCache.cc
  rocksdb_cache/BinnedLRUCache.cc)

add_library(kv STATIC ${kv_srcs}
  $<TARGET_OBJECTS:common_prioritycache_obj>)

target_link_libraries(kv
  RocksDB::RocksDB
  heap_profiler)
