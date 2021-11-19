set(kv_srcs
  KeyValueDB.cc
  RocksDBStore.cc
  RocksDBStore_stats.cc
  KeyValueHistogram.cc
  rocksdb_cache/ShardedCache.cc
  rocksdb_cache/BinnedLRUCache.cc)

add_library(kv STATIC ${kv_srcs}
  $<TARGET_OBJECTS:common_prioritycache_obj>)

target_include_directories(kv PRIVATE "${CMAKE_SOURCE_DIR}/src/rocksdb")

target_link_libraries(kv
  RocksDB::RocksDB
  heap_profiler)
