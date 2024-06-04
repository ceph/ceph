add_executable(unittest-transaction-manager
  test_block.cc
  test_transaction_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-transaction-manager
  --memory 256M --smp 1)
target_link_libraries(
  unittest-transaction-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-btree-lba-manager
  test_btree_lba_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-btree-lba-manager
  --memory 256M --smp 1)
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

add_executable(unittest-object-data-handler
  test_object_data_handler.cc
  ../gtest_seastar.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/lsan_suppressions.cc)
add_ceph_unittest(unittest-object-data-handler
  --memory 256M --smp 1)
target_link_libraries(
  unittest-object-data-handler
  crimson::gtest
  crimson-seastore
  crimson-os
  crimson-common)

add_executable(unittest-collection-manager
  test_collection_manager.cc
  ../gtest_seastar.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/lsan_suppressions.cc)
add_ceph_test(unittest-collection-manager
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
  --memory 256M --smp 1)
target_link_libraries(
  unittest-omap-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-seastore
  test_seastore.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-seastore
  --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore
  ${CMAKE_DL_LIBS}
  crimson-seastore
  crimson-common)

add_executable(unittest-seastore-randomblock-manager
  test_randomblock_manager.cc)
add_ceph_test(unittest-seastore-randomblock-manager
  unittest-seastore-randomblock-manager --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-randomblock-manager
  crimson::gtest
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-seastore-nvmedevice
  nvmedevice/test_nvmedevice.cc)
add_ceph_test(unittest-seastore-nvmedevice
  unittest-seastore-nvmedevice --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-nvmedevice
  crimson::gtest
  crimson-seastore
  aio)

add_executable(unittest-seastore-cbjournal
  test_cbjournal.cc)
add_ceph_test(unittest-seastore-cbjournal
  unittest-seastore-cbjournal --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-cbjournal
  crimson::gtest
  crimson-seastore
  aio)

add_executable(unittest-seastore-extent-allocator
  test_extent_allocator.cc)
add_ceph_test(unittest-seastore-extent-allocator
  unittest-seastore-extent-allocator --memory 256M --smp 1)
target_link_libraries(
  unittest-seastore-extent-allocator
  crimson::gtest
  crimson-seastore
  aio)

add_subdirectory(onode_tree)
