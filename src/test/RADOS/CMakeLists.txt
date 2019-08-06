add_executable(ceph_test_RADOS_start_stop start_stop.cc)
target_link_libraries(ceph_test_RADOS_start_stop
  global libRADOS ${unittest_libs})

add_executable(ceph_test_RADOS_completions completions.cc)
target_link_libraries(ceph_test_RADOS_completions Boost::system pthread
  ${unittest_libs})
