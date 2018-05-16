#ceph_perf_objectstore
add_executable(ceph_perf_objectstore 
  ObjectStoreTransactionBenchmark.cc
  )
set_target_properties(ceph_perf_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_objectstore os osdc global ${UNITTEST_LIBS})
install(TARGETS ceph_perf_objectstore
  DESTINATION bin)

#ceph_test_objectstore
add_library(store_test_fixture OBJECT store_test_fixture.cc)
set_target_properties(store_test_fixture PROPERTIES
  COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_objectstore
  store_test.cc
  $<TARGET_OBJECTS:store_test_fixture>)
set_target_properties(ceph_test_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_objectstore
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_objectstore
  DESTINATION ${CMAKE_INSTALL_BINDIR})

#ceph_test_keyvaluedb
add_executable(ceph_test_keyvaluedb
  test_kv.cc
  )
set_target_properties(ceph_test_keyvaluedb PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb
  os
  ceph-common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_keyvaluedb
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# ceph_test_filestore_idempotent
add_executable(ceph_test_filestore_idempotent
  test_idempotent.cc
  FileStoreTracker.cc
  ${CMAKE_SOURCE_DIR}/src/test/common/ObjectContents.cc
  )
target_link_libraries(ceph_test_filestore_idempotent
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_filestore_idempotent_sequence
add_executable(ceph_test_filestore_idempotent_sequence
  test_idempotent_sequence.cc
  DeterministicOpSequence.cc
  TestObjectStoreState.cc
  FileStoreDiff.cc
  )
target_link_libraries(ceph_test_filestore_idempotent_sequence
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )
install(TARGETS ceph_test_filestore_idempotent_sequence
  DESTINATION ${CMAKE_INSTALL_BINDIR})

# unittest_chain_xattr
add_executable(unittest_chain_xattr
  chain_xattr.cc
  )
add_ceph_unittest(unittest_chain_xattr)
target_link_libraries(unittest_chain_xattr os global)

# unittest_rocksdb_option
add_executable(unittest_rocksdb_option
  TestRocksdbOptionParse.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option global os ${BLKID_LIBRARIES})

if(WITH_BLUESTORE)
  # unittest_bit_alloc
  add_executable(unittest_bit_alloc
    BitAllocator_test.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_bit_alloc)
  target_link_libraries(unittest_bit_alloc os global)

  add_executable(unittest_alloc
    Allocator_test.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_alloc)
  target_link_libraries(unittest_alloc os global)

  # unittest_bluefs
  add_executable(unittest_bluefs
    test_bluefs.cc
    )
  add_ceph_unittest(unittest_bluefs)
  target_link_libraries(unittest_bluefs os global)

  # unittest_bluestore_types
  add_executable(unittest_bluestore_types
    test_bluestore_types.cc
    )
  add_ceph_unittest(unittest_bluestore_types)
  target_link_libraries(unittest_bluestore_types os global)
endif(WITH_BLUESTORE)

# unittest_transaction
add_executable(unittest_transaction
  test_transaction.cc)
add_ceph_unittest(unittest_transaction)
target_link_libraries(unittest_transaction os ceph-common ${XIO_LIBRARY})

# unittest_memstore_clone
add_executable(unittest_memstore_clone
  test_memstore_clone.cc
  $<TARGET_OBJECTS:store_test_fixture>)
add_ceph_unittest(unittest_memstore_clone)
target_link_libraries(unittest_memstore_clone os global)
