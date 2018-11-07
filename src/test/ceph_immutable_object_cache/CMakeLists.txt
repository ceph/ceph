
add_executable(unittest_ceph_immutable_obj_cache
  test_main.cc
  test_SimplePolicy.cc
  test_sync_file.cc
  test_DomainSocket.cc
  )
add_ceph_unittest(unittest_ceph_immutable_obj_cache)


target_link_libraries(unittest_ceph_immutable_obj_cache
  ceph_immutable_object_cache_lib
  rados_test_stub
  librados
  global
  radostest
  stdc++fs
  GTest::GTest
  )


add_executable(ceph_test_immutable_obj_cache
  test_main.cc
  )

target_link_libraries(ceph_test_immutable_obj_cache
  librados
  radostest
  ${UNITTEST_LIBS}
  )


install(TARGETS
  ceph_test_immutable_obj_cache
  DESTINATION ${CMAKE_INSTALL_BINDIR})
