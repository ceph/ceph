// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/randomblock_manager.h"
#include "crimson/os/seastore/nvmedevice/nvmedevice.h"
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
  std::unique_ptr<RandomBlockManager> rbm_manager;
  nvme_device::NVMeBlockDevice *device;

  std::default_random_engine generator;

  const uint64_t block_size = DEFAULT_BLOCK_SIZE;

  RandomBlockManager::mkfs_config_t config;
  paddr_t current;

  rbm_test_t() :
      segment_manager(segment_manager::create_test_ephemeral()),
      cache(*segment_manager)
  {
    device = new nvme_device::TestMemory(DEFAULT_TEST_SIZE);
    rbm_manager.reset(new RandomBlockManager(device, std::string()));
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

  auto create_transaction() {
    return tm->create_transaction();
  }

  auto alloc_extent(Transaction &t, size_t size) {
    return rbm_manager->alloc_extent(t, size).unsafe_get0();
  }

  auto free_extent(Transaction &t, interval_set<blk_id_t> range) {
    for (auto r : range) {
      logger().debug("free_extent: start {} end {}", r.first * DEFAULT_BLOCK_SIZE,
		      (r.first + r.second - 1) * DEFAULT_BLOCK_SIZE);
      return rbm_manager->free_extent(t, r.first * DEFAULT_BLOCK_SIZE,
	      (r.first + r.second - 1) * DEFAULT_BLOCK_SIZE).unsafe_get0();
    }
  }

  auto submit_transaction(Transaction &t) {
    auto record = cache.try_construct_record(t);
    if (!record) {
      return false;
    }
    // TODO: write record on journal
    return true;
  }

  interval_set<blk_id_t> get_allocated_blk_ids(Transaction &t) {
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

  auto complete_allocation(Transaction &t) {
    return rbm_manager->complete_allocation(t).unsafe_get0();
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
   auto t = tm->create_transaction();
   alloc_extent(*t, DEFAULT_BLOCK_SIZE);
   auto alloc_ids = get_allocated_blk_ids(*t);
   ASSERT_TRUE(submit_transaction(*t));
   complete_allocation(*t);
   ASSERT_TRUE(check_ids_are_allocated(alloc_ids));

   auto t2 = tm->create_transaction();
   alloc_extent(*t2, DEFAULT_BLOCK_SIZE);
   alloc_ids = get_allocated_blk_ids(*t);
   alloc_ids = get_allocated_blk_ids(*t2);
   ASSERT_TRUE(submit_transaction(*t2));
   complete_allocation(*t2);
 });
}
