add_library(rados_striper_test STATIC TestCase.cc)
target_link_libraries(rados_striper_test
  radostest
  GTest::GTest)

add_executable(ceph_test_rados_striper_api_striping
  striping.cc
  )
target_link_libraries(ceph_test_rados_striper_api_striping librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
install(TARGETS ceph_test_rados_striper_api_striping
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_rados_striper_api_io
  io.cc)
target_link_libraries(ceph_test_rados_striper_api_io librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
install(TARGETS ceph_test_rados_striper_api_io
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_rados_striper_api_aio
  aio.cc)
target_link_libraries(ceph_test_rados_striper_api_aio librados radosstriper
  ${UNITTEST_LIBS} rados_striper_test)
install(TARGETS ceph_test_rados_striper_api_aio
  DESTINATION ${CMAKE_INSTALL_BINDIR})
