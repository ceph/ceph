set(kv_srcs
  KeyValueDB.cc
  MemDB.cc
  RocksDBStore.cc)

if (WITH_LEVELDB)
  list(APPEND kv_srcs LevelDBStore.cc)
endif (WITH_LEVELDB)

if (WITH_ZS)
	set(kv_srcs ${kv_srcs} ZetaScaleStore.cc)
endif (WITH_ZS)
add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC $<TARGET_OBJECTS:kv_objs>)
target_include_directories(kv_objs BEFORE PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_include_directories(kv BEFORE PUBLIC ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(kv ${LEVELDB_LIBRARIES} rocksdb ${ALLOC_LIBS} snappy z)

if (WITH_ZS)
  target_include_directories(kv_objs PUBLIC ${CMAKE_SOURCE_DIR}/src/zs/include
	  PUBLIC ${CMAKE_SOURCE_DIR}/src/zs)
  target_include_directories(kv PUBLIC ${CMAKE_SOURCE_DIR}/src/zs/include
	  PUBLIC ${CMAKE_SOURCE_DIR}/src/zs)
  add_definitions("-DWITH_ZS")
endif (WITH_ZS)

# rocksdb detects bzlib and lz4 in its Makefile, which forces us to do the same.
find_package(BZip2 QUIET)
if (BZIP2_FOUND)
  target_link_libraries(kv ${BZIP2_LIBRARIES})
endif (BZIP2_FOUND)
find_package(LZ4 QUIET)
if (LZ4_FOUND)
  target_link_libraries(kv ${LZ4_LIBRARY})
endif (LZ4_FOUND)
