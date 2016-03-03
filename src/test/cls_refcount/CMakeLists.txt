# ceph_test_cls_refcount
add_executable(ceph_test_cls_refcount
  test_cls_refcount.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
set_target_properties(ceph_test_cls_refcount PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cls_refcount
  librados
  cls_refcount_client
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${ALLOC_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest
  )

