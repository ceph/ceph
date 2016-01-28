add_library(rados_striper_test STATIC TestCase.cc)
target_link_libraries(rados_striper_test radostest)
set_target_properties(rados_striper_test PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_rados_api_striping
  striping.cc
  )
target_link_libraries(ceph_test_rados_api_striping librados libradosstriper 
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(ceph_test_rados_api_striping PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_ceph_test(rados-striper.sh ${CMAKE_SOURCE_DIR}/src/test/libradosstriper/rados-striper.sh)
