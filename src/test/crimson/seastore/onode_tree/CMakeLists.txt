add_executable(unittest-staged-fltree
  test_staged_fltree.cc
  ../../gtest_seastar.cc)
add_ceph_unittest(unittest-staged-fltree)
target_link_libraries(unittest-staged-fltree
  crimson-seastore)
