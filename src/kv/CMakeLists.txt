set(kv_srcs
  KeyValueDB.cc
  LevelDBStore.cc
  RocksDBStore.cc)
add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC $<TARGET_OBJECTS:kv_objs>)
target_include_directories(kv_objs PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_include_directories(kv PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(kv bz2 z leveldb snappy rocksdb)
