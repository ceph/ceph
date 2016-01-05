set(kv_srcs
  KeyValueDB.cc
  LevelDBStore.cc
  RocksDBStore.cc)
add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC ${kv_srcs})
target_include_directories(kv_objs PUBLIC ${CMAKE_SOURCE_DIR}/src/rocksdb/include)
target_link_libraries(kv bz2 z leveldb snappy rocksdb)
