add_executable(ceph_test_cls_log
  test_cls_log.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(ceph_test_cls_log PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_log
  librados
  cls_log_client
  global
  radostest
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  )

