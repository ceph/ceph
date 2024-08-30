set(crimson_seastore_srcs
  cached_extent.cc
  seastore_types.cc
  segment_manager.cc
  segment_manager/ephemeral.cc
  segment_manager/block.cc
  transaction_manager.cc
  transaction.cc
  cache.cc
  root_block.cc
  lba_manager.cc
  async_cleaner.cc
  backref_manager.cc
  backref/backref_tree_node.cc
  backref/btree_backref_manager.cc
  lba_manager/btree/btree_lba_manager.cc
  lba_manager/btree/lba_btree_node.cc
  omap_manager.cc
  omap_manager/btree/btree_omap_manager.cc
  omap_manager/btree/omap_btree_node_impl.cc
  btree/btree_range_pin.cc
  btree/fixed_kv_node.cc
  onode.cc
  onode_manager/staged-fltree/node.cc
  onode_manager/staged-fltree/node_extent_manager.cc
  onode_manager/staged-fltree/node_extent_manager/seastore.cc
  onode_manager/staged-fltree/node_impl.cc
  onode_manager/staged-fltree/stages/item_iterator_stage.cc
  onode_manager/staged-fltree/stages/key_layout.cc
  onode_manager/staged-fltree/stages/node_stage_layout.cc
  onode_manager/staged-fltree/stages/node_stage.cc
  onode_manager/staged-fltree/stages/sub_items_stage.cc
  onode_manager/staged-fltree/super.cc
  onode_manager/staged-fltree/value.cc
  onode_manager/staged-fltree/fltree_onode_manager.cc
  collection_manager.cc
  collection_manager/flat_collection_manager.cc
  collection_manager/collection_flat_node.cc
  extent_placement_manager.cc
  object_data_handler.cc
  seastore.cc
  random_block_manager.cc
  random_block_manager/block_rb_manager.cc
  random_block_manager/rbm_device.cc
  random_block_manager/nvme_block_device.cc
  random_block_manager/avlallocator.cc
  journal/segmented_journal.cc
  journal/segment_allocator.cc
  journal/record_submitter.cc
  journal/circular_journal_space.cc
  journal.cc
  device.cc
  segment_manager_group.cc
  record_scanner.cc
  journal/circular_bounded_journal.cc
  ../../../test/crimson/seastore/test_block.cc
  ${PROJECT_SOURCE_DIR}/src/os/Transaction.cc
	)

CMAKE_DEPENDENT_OPTION(WITH_ZNS "enable Linux ZNS support" OFF
  "CMAKE_SYSTEM_NAME STREQUAL Linux" OFF)
if(WITH_ZNS)
  find_package(LinuxZNS REQUIRED)
  list(APPEND crimson_seastore_srcs
    segment_manager/zbd.cc)
endif()

add_library(crimson-seastore STATIC
  ${crimson_seastore_srcs})

target_link_libraries(crimson-seastore
  crimson)
if(WITH_ZNS)
  target_link_libraries(crimson-seastore
    Linux::ZNS)
endif()

set_target_properties(crimson-seastore PROPERTIES
  JOB_POOL_COMPILE heavy_compile_job_pool)
