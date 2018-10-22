set(kv_srcs
  KeyValueDB.cc
  MemDB.cc
  RocksDBStore.cc
  rocksdb_cache/ShardedCache.cc
  rocksdb_cache/BinnedLRUCache.cc)

if (WITH_LEVELDB)
  list(APPEND kv_srcs LevelDBStore.cc)
endif (WITH_LEVELDB)

add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC $<TARGET_OBJECTS:kv_objs>)
target_include_directories(kv_objs SYSTEM BEFORE PUBLIC
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(kv SYSTEM BEFORE PUBLIC
  $<TARGET_PROPERTY:RocksDB::RocksDB,INTERFACE_INCLUDE_DIRECTORIES>)
target_link_libraries(kv ${LEVELDB_LIBRARIES} RocksDB::RocksDB)
