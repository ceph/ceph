set(kv_srcs
  KeyValueDB.cc
  RocksDBStore.cc
  LevelDBStore.cc)
#if(${WITH_ROCKSDB})
  #list(APPEND kv_srcs RocksDBStore.cc)
  #endif(${WITH_ROCKSDB})
if(${WITH_KINETIC})
  list(APPEND kv_srcs KineticStore.cc)
endif(${WITH_KINETIC})
add_library(kv_objs OBJECT ${kv_srcs})
add_library(kv STATIC ${kv_srcs})
target_include_directories(kv_objs PUBLIC ${CMAKE_SOURCE_DIR}/src/rocksdb/include)
target_include_directories(kv PUBLIC ${CMAKE_SOURCE_DIR}/src/rocksdb/include)
target_link_libraries(kv bz2 z leveldb snappy)
if(${WITH_ROCKSDB})
  target_link_libraries(kv librocksdb)
endif(${WITH_ROCKSDB})
#will add kinetic search later
if(${WITH_KINETIC})
  target_link_libraries(kv kinetic_client protobuf glog gflags ${CRYPTO_LIBS})
endif(${WITH_KINETIC})

