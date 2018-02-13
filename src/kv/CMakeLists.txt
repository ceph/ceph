set(kv_srcs
  KeyValueDB.cc
  MemDB.cc
  RocksDBStore.cc)

if (WITH_LEVELDB)
  list(APPEND kv_srcs LevelDBStore.cc)
endif (WITH_LEVELDB)

add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC $<TARGET_OBJECTS:kv_objs>)
target_include_directories(kv_objs SYSTEM BEFORE PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_include_directories(kv SYSTEM BEFORE PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(kv ${LEVELDB_LIBRARIES} ${ROCKSDB_LIBRARIES} ${ALLOC_LIBS} ${SNAPPY_LIBRARIES} ${ZLIB_LIBRARIES})
