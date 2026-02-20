add_executable(ceph_test_cls_cmpomap test_cls_cmpomap.cc
  ../librados/test_pool_types.cc)
target_link_libraries(ceph_test_cls_cmpomap cls_cmpomap_client librados radostest-cxx ${UNITTEST_LIBS})
install(TARGETS ceph_test_cls_cmpomap DESTINATION ${CMAKE_INSTALL_BINDIR})
