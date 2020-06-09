add_executable(test-seastore-onode-tree-node
  test_node.cc)
add_ceph_unittest(test-seastore-onode-tree-node)
target_link_libraries(test-seastore-onode-tree-node
  crimson-seastore
  GTest::Main)
