#ceph_perf_objectstore
add_executable(ceph_perf_objectstore 
  ObjectStoreTransactionBenchmark.cc
  )
set_target_properties(ceph_perf_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_perf_objectstore os osdc global ${UNITTEST_LIBS})

#ceph_test_objectstore
add_executable(ceph_test_objectstore
  store_test.cc
  )
set_target_properties(ceph_test_objectstore PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_objectstore
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

#ceph_test_keyvaluedb
add_executable(ceph_test_keyvaluedb
  test_kv.cc
  )
set_target_properties(ceph_test_keyvaluedb PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_keyvaluedb
  os
  common
  ${UNITTEST_LIBS}
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

# ceph_test_objectstore_workloadgen
add_executable(ceph_test_objectstore_workloadgen
  workload_generator.cc
  TestObjectStoreState.cc
  )
target_link_libraries(ceph_test_objectstore_workloadgen
  os
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  )

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

# unittest_chain_xattr
add_executable(unittest_chain_xattr EXCLUDE_FROM_ALL
  chain_xattr.cc
  )
add_ceph_unittest(unittest_chain_xattr ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_chain_xattr)
target_link_libraries(unittest_chain_xattr os global)

# unittest_rocksdb_option
add_executable(unittest_rocksdb_option EXCLUDE_FROM_ALL
  TestRocksdbOptionParse.cc
  )
add_ceph_unittest(unittest_rocksdb_option ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option global os ${BLKID_LIBRARIES})

# unittest_bit_alloc
add_executable(unittest_bit_alloc EXCLUDE_FROM_ALL
  BitAllocator_test.cc
  )
add_ceph_unittest(unittes_bit_alloc ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bit_alloc)
target_link_libraries(unittest_bit_alloc os global)

# unittest_bluefs
add_executable(unittest_bluefs EXCLUDE_FROM_ALL 
  test_bluefs.cc
  )
add_ceph_unittest(unittest_bluefs ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bluefs)
target_link_libraries(unittest_bluefs os global)

# unittest_bluestore_types
add_executable(unittest_bluestore_types EXCLUDE_FROM_ALL 
  test_bluestore_types.cc
  )
add_ceph_unittest(unittest_bluestore_types ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_bluestore_types)
target_link_libraries(unittest_bluestore_types os global)
  
# unittest_transaction
add_executable(unittest_transaction EXCLUDE_FROM_ALL 
  test_transaction.cc)
add_ceph_unittest(unittest_transaction ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_transaction)
target_link_libraries(unittest_transaction os common)

