add_executable(ceph_perf_objectstore 
  ObjectStoreTransactionBenchmark.cc)
target_link_libraries(ceph_perf_objectstore os osdc global ${UNITTEST_LIBS})
install(TARGETS ceph_perf_objectstore
  DESTINATION bin)

add_library(store_test_fixture OBJECT store_test_fixture.cc)
target_include_directories(store_test_fixture PRIVATE
  $<TARGET_PROPERTY:GTest::GTest,INTERFACE_INCLUDE_DIRECTORIES>)

add_executable(ceph_test_objectstore
  store_test.cc
  $<TARGET_OBJECTS:store_test_fixture>)
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

add_executable(ceph_test_keyvaluedb
  test_kv.cc)
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

# unittest_rocksdb_option
add_executable(unittest_rocksdb_option
  TestRocksdbOptionParse.cc
  $<TARGET_OBJECTS:unit-main>
  )
add_ceph_unittest(unittest_rocksdb_option)
target_link_libraries(unittest_rocksdb_option global os ${BLKID_LIBRARIES})

if(WITH_EVENTTRACE)
  add_dependencies(os eventtrace_tp)
endif()

if(WITH_BLUESTORE)

  add_executable(unittest_alloc
    Allocator_test.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_alloc)
  target_link_libraries(unittest_alloc os global)

  add_executable(unittest_alloc_bench
    Allocator_bench.cc
    $<TARGET_OBJECTS:unit-main>
    )
  target_link_libraries(unittest_alloc_bench ${UNITTEST_LIBS} os global)

  add_executable(unittest_fastbmap_allocator
    fastbmap_allocator_test.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_fastbmap_allocator)
  target_link_libraries(unittest_fastbmap_allocator os global)

  set_target_properties(unittest_fastbmap_allocator PROPERTIES COMPILE_FLAGS
  "${UNITTEST_CXX_FLAGS}")

  add_executable(unittest_hybrid_allocator
    hybrid_allocator_test.cc
    $<TARGET_OBJECTS:unit-main>
    )
  add_ceph_unittest(unittest_hybrid_allocator)
  target_link_libraries(unittest_hybrid_allocator os global)

  set_target_properties(unittest_hybrid_allocator PROPERTIES COMPILE_FLAGS
  "${UNITTEST_CXX_FLAGS}")

  add_executable(unittest_alloc_aging EXCLUDE_FROM_ALL
    Allocator_aging_fragmentation.cc)
  target_link_libraries(unittest_alloc_aging os global GTest::Main)

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

  # unittest_bdev
  add_executable(unittest_bdev
    test_bdev.cc
    )
  add_ceph_unittest(unittest_bdev)
  target_link_libraries(unittest_bdev os global)

  # unittest_deferred
  add_executable(unittest_deferred
    test_deferred.cc
    )
  add_ceph_unittest(unittest_deferred)
  target_link_libraries(unittest_deferred os global)

endif(WITH_BLUESTORE)

# unittest_transaction
add_executable(unittest_transaction
  test_transaction.cc)
add_ceph_unittest(unittest_transaction)
target_link_libraries(unittest_transaction os ceph-common)

# unittest_memstore_clone
add_executable(unittest_memstore_clone
  test_memstore_clone.cc
  $<TARGET_OBJECTS:store_test_fixture>)
add_ceph_unittest(unittest_memstore_clone)
target_link_libraries(unittest_memstore_clone os global)

if(WITH_BLUESTORE)
  add_executable(ceph_test_alloc_replay
    allocator_replay_test.cc)
  target_link_libraries(ceph_test_alloc_replay os global ${UNITTEST_LIBS})
  install(TARGETS ceph_test_alloc_replay
    DESTINATION bin)
endif()

# fragmentation simulator
add_library(ObjectStoreImitator OBJECT ObjectStoreImitator.cc)

add_executable(ceph_test_fragmentation_sim
  Fragmentation_simulator.cc
  $<TARGET_OBJECTS:ObjectStoreImitator>)
add_ceph_unittest(ceph_test_fragmentation_sim)
target_link_libraries(ceph_test_fragmentation_sim os global)

set_target_properties(ceph_test_fragmentation_sim PROPERTIES COMPILE_FLAGS
"${UNITTEST_CXX_FLAGS}")
