# unittest_slow_fast_codel
add_executable(unittest_slow_fast_codel
  TestBlueStoreSlowFastCoDel.cc
  )
add_ceph_unittest(unittest_slow_fast_codel)
target_link_libraries(unittest_slow_fast_codel
  rados_test_stub
  librados
  global
  radostest-cxx
  GTest::GTest
  ceph-common
  os
  global
  )
