add_executable(test-seastore-onode-tree-node
  test_node.cc)
add_ceph_unittest(test-seastore-onode-tree-node)
target_link_libraries(test-seastore-onode-tree-node
  crimson-seastore
  GTest::Main)

add_executable(unittest-staged-fltree
  test_staged_fltree.cc
  ../../gtest_seastar.cc)
add_ceph_unittest(unittest-staged-fltree)
target_link_libraries(unittest-staged-fltree
  crimson-seastore)
