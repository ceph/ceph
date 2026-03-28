# Fixes: https://tracker.ceph.com/issues/71704
function(avoid_asan_uar_slowdown name)
  set_tests_properties(${name} PROPERTIES
    ENVIRONMENT_MODIFICATION
    ASAN_OPTIONS=string_append::max_uar_stack_size_log=16)
endfunction()

add_executable(ceph_test_transaction_manager
  test_block.cc
  test_transaction_manager.cc
  ../gtest_seastar.cc)
target_link_libraries(
  ceph_test_transaction_manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)

add_executable(unittest-omap-manager
  test_omap_manager.cc
  ../gtest_seastar.cc)
add_ceph_unittest(unittest-omap-manager
  --memory 256M --smp 1)
target_link_libraries(
  unittest-omap-manager
  ${CMAKE_DL_LIBS}
  crimson-seastore)
avoid_asan_uar_slowdown(unittest-omap-manager)

add_executable(ceph_test_btree_lba_manager
  test_btree_lba_manager.cc
  ../gtest_seastar.cc)
target_link_libraries(
  ceph_test_btree_lba_manager
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

add_executable(ceph_test_object_data_handler
  test_object_data_handler.cc
  ../gtest_seastar.cc
  ${PROJECT_SOURCE_DIR}/src/crimson/osd/lsan_suppressions.cc)
target_link_libraries(
  ceph_test_object_data_handler
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

add_executable(ceph_test_seastore
  test_seastore.cc
  ../gtest_seastar.cc)
target_link_libraries(
  ceph_test_seastore
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
