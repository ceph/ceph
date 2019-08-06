add_executable(ceph_test_neorados_start_stop start_stop.cc)
target_link_libraries(ceph_test_neorados_start_stop global libneorados ${unittest_libs})

add_executable(ceph_test_neorados_completions completions.cc)
target_link_libraries(ceph_test_neorados_completions Boost::system pthread
  ${unittest_libs})

add_executable(ceph_test_neorados_op_speed op_speed.cc)
target_link_libraries(ceph_test_neorados_op_speed
  libneorados fmt::fmt ${unittest_libs})
