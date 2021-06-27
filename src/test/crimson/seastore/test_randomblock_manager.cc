// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/random_block_manager/nvme_manager.h"
#include "crimson/os/seastore/random_block_manager/nvmedevice.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

constexpr uint64_t DEFAULT_TEST_SIZE = 1 << 20;
constexpr uint64_t DEFAULT_BLOCK_SIZE = 4096;

struct rbm_test_t : public  seastar_test_suite_t,
  TMTestState {
  segment_manager::EphemeralSegmentManagerRef segment_manager; // Need to be deleted, just for Cache
  Cache cache;
  std::unique_ptr<NVMeManager> rbm_manager;
  nvme_device::NVMeBlockDevice *device;

  struct rbm_transaction {
    void add_rbm_allocated_blocks(rbm_alloc_delta_t &d) {
      allocated_blocks.push_back(d);
    }
    void clear_rbm_allocated_blocks() {
      if (!allocated_blocks.empty()) {
	allocated_blocks.clear();
      }
    }
    const auto &get_rbm_allocated_blocks() {
      return allocated_blocks;
    }
    std::vector<rbm_alloc_delta_t> allocated_blocks;
  };

  std::default_random_engine generator;

  const uint64_t block_size = DEFAULT_BLOCK_SIZE;

  RandomBlockManager::mkfs_config_t config;
  paddr_t current;

  rbm_test_t() :
      segment_manager(segment_manager::create_test_ephemeral()),
      cache(*segment_manager)
  {
    device = new nvme_device::TestMemory(DEFAULT_TEST_SIZE);
    rbm_manager.reset(new NVMeManager(device, std::string()));
    config.start = 0;
    config.end = DEFAULT_TEST_SIZE;
    config.block_size = DEFAULT_BLOCK_SIZE;
    config.total_size = DEFAULT_TEST_SIZE;
  }

  seastar::future<> set_up_fut() final {
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    if (device) {
      delete device;
    }
    return tm_teardown();
  }

  auto mkfs() {
    return rbm_manager->mkfs(config).unsafe_get0();
  }

  auto read_rbm_header() {
    return rbm_manager->read_rbm_header(config.start).unsafe_get0();
  }

  auto open() {
    return rbm_manager->open("", config.start).unsafe_get0();
  }

  auto write(uint64_t addr, bufferptr &ptr) {
    return rbm_manager->write(addr, ptr).unsafe_get0();
  }

  auto read(uint64_t addr, bufferptr &ptr) {
    return rbm_manager->read(addr, ptr).unsafe_get0();
  }

  auto create_rbm_transaction() {
    return std::make_unique<rbm_transaction>();
  }

  auto alloc_extent(rbm_transaction &t, size_t size) {
    auto tt = tm->create_transaction(); // dummy transaction
    auto extent = rbm_manager->find_free_block(*tt, size).unsafe_get0();
    if (!extent.empty()) {
      rbm_alloc_delta_t alloc_info {
	extent_types_t::RBM_ALLOC_INFO,
	  extent,
	  rbm_alloc_delta_t::op_types_t::SET
      };
      t.add_rbm_allocated_blocks(alloc_info);
    }
  }

  auto free_extent(rbm_transaction &t, interval_set<blk_id_t> range) {
    for (auto r : range) {
      logger().debug("free_extent: start {} len {}", r.first * DEFAULT_BLOCK_SIZE,
		      r.second * DEFAULT_BLOCK_SIZE);
      rbm_manager->add_free_extent(t.allocated_blocks, r.first * DEFAULT_BLOCK_SIZE,
				    r.second * DEFAULT_BLOCK_SIZE);
    }
  }

  interval_set<blk_id_t> get_allocated_blk_ids(rbm_transaction &t) {
    auto allocated_blocks = t.get_rbm_allocated_blocks();
    interval_set<blk_id_t> alloc_ids;
    for (auto p : allocated_blocks) {
      alloc_ids.insert(p.alloc_blk_ids);
    }
    logger().debug(" get allocated blockid {}", alloc_ids);
    return alloc_ids;
  }

  bool check_ids_are_allocated(interval_set<blk_id_t> &ids, bool allocated = true) {
    bool ret = true;
    for (auto r : ids) {
      for (blk_id_t id = r.first; id < r.first + r.second; id++) {
	auto addr = rbm_manager->get_start_block_alloc_area() +
		     (id / rbm_manager->max_block_by_bitmap_block())
		     * DEFAULT_BLOCK_SIZE;
	logger().debug(" addr {} id {} ", addr, id);
	auto bp = bufferptr(ceph::buffer::create_page_aligned(DEFAULT_BLOCK_SIZE));
	rbm_manager->read(addr, bp).unsafe_get0();
	rbm_bitmap_block_t b_block(DEFAULT_BLOCK_SIZE);
	bufferlist bl;
	bl.append(bp);
	auto b_bl = bl.cbegin();
	decode(b_block, b_bl);
	if (!b_block.is_allocated(id % rbm_manager->max_block_by_bitmap_block())) {
	  logger().debug(" block id {} is not allocated", id);
	  if (allocated) {
	    ret = false;
	    return ret;
	  }
	} else {
	  logger().debug(" block id {} allocated", id);
	  if (!allocated) {
	    ret = false;
	    return ret;
	  }
	}
      }
    }
    return ret;
  }

  auto complete_allocation(rbm_transaction &t) {
    auto alloc_blocks = t.get_rbm_allocated_blocks();
    return rbm_manager->sync_allocation(alloc_blocks).unsafe_get0();
  }

  bufferptr generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    return buffer::ptr(buffer::create(blocks * block_size, contents));
  }

};

TEST_F(rbm_test_t, mkfs_test)
{
 run_async([this] {
   mkfs();
   open();
   auto super = read_rbm_header();
   ASSERT_TRUE(
       super.block_size == DEFAULT_BLOCK_SIZE &&
       super.end == DEFAULT_TEST_SIZE &&
       super.start_alloc_area == DEFAULT_BLOCK_SIZE &&
       super.free_block_count == DEFAULT_TEST_SIZE / DEFAULT_BLOCK_SIZE - 2  &&
       super.alloc_area_size == DEFAULT_BLOCK_SIZE
   );

 });
}

TEST_F(rbm_test_t, open_test)
{
 run_async([this] {
   mkfs();
   open();
   auto content = generate_extent(1);
   write(
       DEFAULT_BLOCK_SIZE,
       content
       );
   auto bp = bufferptr(ceph::buffer::create_page_aligned(DEFAULT_BLOCK_SIZE));
   read(
       DEFAULT_BLOCK_SIZE,
       bp
       );
   bufferlist bl;
   bufferlist block;
   bl.append(bp);
   block.append(content);
   ASSERT_EQ(
        bl.begin().crc32c(bl.length(), 1),
        block.begin().crc32c(block.length(), 1));

 });
}

TEST_F(rbm_test_t, block_alloc_test)
{
 run_async([this] {
   mkfs();
   open();
   auto t = create_rbm_transaction();
   alloc_extent(*t, DEFAULT_BLOCK_SIZE);
   auto alloc_ids = get_allocated_blk_ids(*t);
   complete_allocation(*t);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));

   auto t2 = create_rbm_transaction();
   alloc_extent(*t2, DEFAULT_BLOCK_SIZE * 3);
   alloc_ids = get_allocated_blk_ids(*t2);
   complete_allocation(*t2);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
 });
}

TEST_F(rbm_test_t, block_alloc_free_test)
{
 run_async([this] {
   mkfs();
   open();
   auto t = create_rbm_transaction();
   alloc_extent(*t, DEFAULT_BLOCK_SIZE);
   auto alloc_ids = get_allocated_blk_ids(*t);
   free_extent(*t, alloc_ids);
   complete_allocation(*t);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));

   auto t2 = create_rbm_transaction();
   alloc_extent(*t2, DEFAULT_BLOCK_SIZE * 4);
   alloc_ids = get_allocated_blk_ids(*t2);
   free_extent(*t2, alloc_ids);
   complete_allocation(*t2);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));

   auto t3 = create_rbm_transaction();
   alloc_extent(*t3, DEFAULT_BLOCK_SIZE * 8);
   alloc_ids = get_allocated_blk_ids(*t3);
   complete_allocation(*t3);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));

   auto t4 = create_rbm_transaction();
   free_extent(*t4, alloc_ids);
   complete_allocation(*t4);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));
 });
}

TEST_F(rbm_test_t, many_block_alloc)
{
 run_async([this] {
   config.start = 0;
   config.end = DEFAULT_TEST_SIZE * 1024;
   config.block_size = DEFAULT_BLOCK_SIZE;
   config.total_size = DEFAULT_TEST_SIZE * 1024;
   mkfs();
   open();
   auto max = rbm_manager->max_block_by_bitmap_block();
   rbm_manager->rbm_sync_block_bitmap_by_range(max + 10, max + 14, bitmap_op_types_t::ALL_SET).unsafe_get0();
   interval_set<blk_id_t> alloc_ids;
   alloc_ids.insert(max + 12, 2);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   alloc_ids.clear();
   alloc_ids.insert(max + 10, 4);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   rbm_manager->rbm_sync_block_bitmap_by_range(max + 10, max + 14, bitmap_op_types_t::ALL_CLEAR).unsafe_get0();
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));
   rbm_manager->rbm_sync_block_bitmap_by_range(max + 10, max + max + 10, bitmap_op_types_t::ALL_SET).unsafe_get0();
   alloc_ids.clear();
   alloc_ids.insert(max + 10000, 10);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   alloc_ids.clear();
   alloc_ids.insert(max + max, 10);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   rbm_manager->rbm_sync_block_bitmap_by_range(max, max * 3, bitmap_op_types_t::ALL_SET).unsafe_get0();
   alloc_ids.clear();
   alloc_ids.insert(max * 3 - 1, 1);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   alloc_ids.clear();
   alloc_ids.insert(max * 3, 1);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   alloc_ids.clear();
   alloc_ids.insert(max, 1);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   rbm_manager->rbm_sync_block_bitmap_by_range(max, max * 6, bitmap_op_types_t::ALL_SET).unsafe_get0();
   alloc_ids.clear();
   alloc_ids.insert(max * 5, 10);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   alloc_ids.clear();
   alloc_ids.insert(max * 6, 1);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));
   rbm_manager->rbm_sync_block_bitmap_by_range(max, max * 6, bitmap_op_types_t::ALL_CLEAR).unsafe_get0();
   alloc_ids.clear();
   alloc_ids.insert(max * 3, 10);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));
   alloc_ids.clear();
   alloc_ids.insert(max * 5, 10);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));
   alloc_ids.clear();
   alloc_ids.insert(max * 6, 1);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids, false));
 });
}

TEST_F(rbm_test_t, check_free_blocks)
{
 run_async([this] {
   mkfs();
   open();
   rbm_manager->rbm_sync_block_bitmap_by_range(10, 12, bitmap_op_types_t::ALL_SET).unsafe_get0();
   rbm_manager->check_bitmap_blocks().unsafe_get0();
   ASSERT_TRUE(rbm_manager->get_free_blocks() == DEFAULT_TEST_SIZE/DEFAULT_BLOCK_SIZE - 5);
   auto free = rbm_manager->get_free_blocks();
   interval_set<blk_id_t> alloc_ids;
   auto t = create_rbm_transaction();
   alloc_extent(*t, DEFAULT_BLOCK_SIZE * 4);
   alloc_ids = get_allocated_blk_ids(*t);
   complete_allocation(*t);
   ASSERT_TRUE(rbm_manager->get_free_blocks() == free - 4);

   free = rbm_manager->get_free_blocks();
   auto t2 = create_rbm_transaction();
   free_extent(*t2, alloc_ids);
   complete_allocation(*t2);
   ASSERT_TRUE(rbm_manager->get_free_blocks() == free + 4);
 });
}
