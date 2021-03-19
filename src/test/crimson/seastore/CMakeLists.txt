add_executable(unittest-transaction-manager
  test_block.cc
  test_transaction_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-transaction-manager
  unittest-seastar-socket --memory 256M --smp 1)
target_link_libraries(
  unittest-transaction-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-btree-lba-manager
  test_btree_lba_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-btree-lba-manager
  unittest-btree-lba-manager --memory 256M --smp 1)
target_link_libraries(
  unittest-btree-lba-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-seastore-journal
  test_seastore_journal.cc)
add_ceph_test(unittest-seastore-journal
  unittest-seastore-journal --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-journal
  crimson::gtest
  crimson-seastore)

add_executable(unittest-seastore-cache
  test_block.cc
  test_seastore_cache.cc)
add_ceph_test(unittest-seastore-cache
  unittest-seastore-cache --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-cache
  crimson::gtest
  crimson-seastore)

add_executable(unittest-extmap-manager
  test_extmap_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-extmap-manager
  unittest-extmap-manager --memory 256M --smp 1)
target_link_libraries(
  unittest-extmap-manager
  crimson::gtest
  crimson-seastore
  crimson-os
  crimson-common)

add_executable(unittest-collection-manager
  test_collection_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-collection-manager
  unittest-collection-manager --memory 256M --smp 1)
target_link_libraries(
  unittest-collection-manager
  crimson::gtest
  crimson-seastore
  crimson-os
  crimson-common)

add_executable(unittest-omap-manager
  test_omap_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-omap-manager
  unittest-omap-manager --memory 256M --smp 1)
target_link_libraries(
  unittest-omap-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-seastore
  test_seastore.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-seastore
  unittest-seastore --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore
  ${CMAKE_DL_LIBS}
  crimson-seastore
  crimson-common)

add_subdirectory(onode_tree)
