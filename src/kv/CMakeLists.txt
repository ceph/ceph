set(kv_srcs
  KeyValueDB.cc
  LevelDBStore.cc
  RocksDBStore.cc)
add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC $<TARGET_OBJECTS:kv_objs>)
target_include_directories(kv_objs PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_include_directories(kv PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(kv leveldb rocksdb snappy z)

# rocksdb detects bzlib and lz4 in its Makefile, which forces us to do the same.
find_package(BZip2 QUIET)
if (BZIP2_FOUND)
  target_link_libraries(kv ${BZIP2_LIBRARIES})
endif (BZIP2_FOUND)
find_package(LZ4 QUIET)
if (LZ4_FOUND)
  target_link_libraries(kv ${LZ4_LIBRARY})
endif (LZ4_FOUND)
