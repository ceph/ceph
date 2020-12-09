add_executable(unittest-transaction-manager
  test_block.cc
  test_transaction_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-transaction-manager)
target_link_libraries(
  unittest-transaction-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-btree-lba-manager
  test_btree_lba_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-btree-lba-manager)
target_link_libraries(
  unittest-btree-lba-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-seastore-journal
  test_seastore_journal.cc)
add_ceph_test(unittest-seastore-journal
  unittest-seastore-journal)
target_link_libraries(
  unittest-seastore-journal
  crimson::gtest
  crimson-seastore)

add_executable(unittest-seastore-cache
  test_block.cc
  test_seastore_cache.cc)
add_ceph_test(unittest-seastore-cache
  unittest-seastore-cache)
target_link_libraries(
  unittest-seastore-cache
  crimson::gtest
  crimson-seastore)

add_executable(unittest-extmap-manager
  test_extmap_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-extmap-manager)
target_link_libraries(
  unittest-extmap-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_subdirectory(onode_tree)
