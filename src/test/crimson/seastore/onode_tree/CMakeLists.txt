add_executable(unittest-staged-fltree
  test_staged_fltree.cc
  ../../gtest_seastar.cc)
add_ceph_unittest(unittest-staged-fltree)
target_link_libraries(unittest-staged-fltree
  crimson-seastore)

add_executable(unittest-fltree-onode-manager
  test_fltree_onode_manager.cc
  ../../gtest_seastar.cc)
add_ceph_unittest(unittest-fltree-onode-manager)
target_link_libraries(unittest-fltree-onode-manager
  crimson-seastore)
