add_executable(test-seastore-onode-tree-node
  test_node.cc)
add_ceph_unittest(test-seastore-onode-tree-node)
target_link_libraries(test-seastore-onode-tree-node
  crimson-seastore
  GTest::Main
  crimson-os
  crimson-common)

add_executable(unittest_staged_fltree
  test_staged_fltree.cc
  ../../gtest_seastar.cc)
add_ceph_unittest(unittest_staged_fltree)
target_link_libraries(unittest_staged_fltree
  crimson-seastore)
