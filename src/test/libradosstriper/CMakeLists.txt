add_library(rados_striper_test STATIC TestCase.cc)
target_link_libraries(rados_striper_test radostest)
set_target_properties(rados_striper_test PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

add_executable(ceph_test_rados_striper_api_striping
  striping.cc
  )
target_link_libraries(ceph_test_rados_striper_api_striping librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(ceph_test_rados_striper_api_striping PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
install(TARGETS ceph_test_rados_striper_api_striping
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_rados_striper_api_io
  io.cc)
target_link_libraries(ceph_test_rados_striper_api_io librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(ceph_test_rados_striper_api_io PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
install(TARGETS ceph_test_rados_striper_api_io
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_rados_striper_api_aio
  aio.cc)
target_link_libraries(ceph_test_rados_striper_api_aio librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
set_target_properties(ceph_test_rados_striper_api_aio PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})
install(TARGETS ceph_test_rados_striper_api_aio
  DESTINATION ${CMAKE_INSTALL_BINDIR})
