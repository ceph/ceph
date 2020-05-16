add_executable(unittest_transaction_manager
  test_transaction_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest_transaction_manager)
target_link_libraries(
  unittest_transaction_manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest_btree_lba_manager
  test_btree_lba_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest_btree_lba_manager)
target_link_libraries(
  unittest_btree_lba_manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest_seastore_journal
  test_seastore_journal.cc)
add_ceph_test(unittest_seastore_journal
  unittest_seastore_cache)
target_link_libraries(
  unittest_seastore_journal
  crimson::gtest
  crimson-seastore)

add_executable(unittest_seastore_cache
  test_seastore_cache.cc)
add_ceph_test(unittest_seastore_cache
  unittest_seastore_cache)
target_link_libraries(
  unittest_seastore_cache
  crimson::gtest
  crimson-seastore)

add_subdirectory(onode_tree)
