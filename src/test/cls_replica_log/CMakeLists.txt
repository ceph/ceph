# ceph_test_cls_replica_log
add_executable(ceph_test_cls_replica_log
  test_cls_replica_log.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(ceph_test_cls_replica_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_replica_log
  librados
  cls_replica_log_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

