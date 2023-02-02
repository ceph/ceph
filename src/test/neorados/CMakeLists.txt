add_executable(ceph_test_neorados test_neorados.cc)
target_link_libraries(ceph_test_neorados global libneorados
  ${unittest_libs}
  radostest
  radostest-cxx
  librados
  GTest::GTest)

add_executable(ceph_test_neorados_start_stop start_stop.cc)
target_link_libraries(ceph_test_neorados_start_stop global libneorados
  ${unittest_libs})

add_executable(ceph_test_neorados_completions completions.cc)
target_link_libraries(ceph_test_neorados_completions Boost::system pthread
  ${unittest_libs})

add_executable(ceph_test_neorados_op_speed op_speed.cc)
target_link_libraries(ceph_test_neorados_op_speed
  libneorados ${FMT_LIB} ${unittest_libs})

add_library(neoradostest-support STATIC common_tests.cc)
target_link_libraries(neoradostest-support
  libneorados ${FMT_LIB} GTest::GTest)

add_executable(ceph_test_neorados_list_pool list_pool.cc)
target_link_libraries(ceph_test_neorados_list_pool
  libneorados neoradostest-support global ${FMT_LIB} ${unittest_libs})

add_executable(ceph_test_neorados_handler_error
  handler_error.cc
  )
target_link_libraries(ceph_test_neorados_handler_error
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_neorados_handler_error
  DESTINATION ${CMAKE_INSTALL_BINDIR})
