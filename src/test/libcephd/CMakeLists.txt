# cephdtest
set(cephdtest_srcs
  test.cc)
add_library(cephdtest STATIC ${cephdtest_srcs})
set_target_properties(cephdtest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# ceph_test_cephd_api_misc
add_executable(ceph_test_cephd_api_misc
  misc.cc
  )
set_target_properties(ceph_test_cephd_api_misc PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
target_link_libraries(ceph_test_cephd_api_misc
  cephd global ${UNITTEST_LIBS} cephdtest z snappy)

install(TARGETS
  ceph_test_cephd_api_misc
  DESTINATION ${CMAKE_INSTALL_BINDIR})
