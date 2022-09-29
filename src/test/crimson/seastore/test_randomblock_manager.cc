// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"

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

struct rbm_test_t :
  public seastar_test_suite_t {
  std::unique_ptr<BlockRBManager> rbm_manager;
  std::unique_ptr<random_block_device::RBMDevice> device;

  struct rbm_transaction {
    void add_rbm_allocated_blocks(alloc_delta_t &d) {
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
    std::vector<alloc_delta_t> allocated_blocks;
  };

  std::default_random_engine generator;

  const uint64_t block_size = DEFAULT_BLOCK_SIZE;

  RandomBlockManager::mkfs_config_t config;
  paddr_t current;

  rbm_test_t() = default;

  seastar::future<> set_up_fut() final {
    device.reset(new random_block_device::TestMemory(DEFAULT_TEST_SIZE));
    device_id_t d_id = 1 << (std::numeric_limits<device_id_t>::digits - 1);
    device->set_device_id(d_id);
    rbm_manager.reset(new BlockRBManager(device.get(), std::string()));
    config.start = paddr_t::make_blk_paddr(d_id, 0);
    config.end = paddr_t::make_blk_paddr(d_id, DEFAULT_TEST_SIZE);
    config.block_size = DEFAULT_BLOCK_SIZE;
    config.total_size = DEFAULT_TEST_SIZE;
    config.device_id = d_id;
    return device->mount().handle_error(crimson::ct_error::assert_all{}
    ).then([this] {
      return rbm_manager->mkfs(config).handle_error(crimson::ct_error::assert_all{}
      ).then([this] {
	return rbm_manager->open().handle_error(crimson::ct_error::assert_all{});
      });
    });
  }

  seastar::future<> tear_down_fut() final {
    rbm_manager->close().unsafe_get0();
    device->close().unsafe_get0();
    rbm_manager.reset();
    device.reset();
    return seastar::now();
  }

  auto mkfs() {
    return rbm_manager->mkfs(config).unsafe_get0();
  }

  auto read_rbm_header() {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(config.start);
    return rbm_manager->read_rbm_header(addr).unsafe_get0();
  }

  auto open() {
    device->mount().unsafe_get0();
    return rbm_manager->open().unsafe_get0();
  }

  auto write(uint64_t addr, bufferptr &ptr) {
    paddr_t paddr = convert_abs_addr_to_paddr(
      addr,
      rbm_manager->get_device_id());
    return rbm_manager->write(paddr, ptr).unsafe_get0();
  }

  auto read(uint64_t addr, bufferptr &ptr) {
    paddr_t paddr = convert_abs_addr_to_paddr(
      addr,
      rbm_manager->get_device_id());
    return rbm_manager->read(paddr, ptr).unsafe_get0();
  }

  bufferptr generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    return buffer::ptr(buffer::create(blocks * block_size, contents));
  }

  void close() {
    rbm_manager->close().unsafe_get0();
    return;
  }

};

TEST_F(rbm_test_t, mkfs_test)
{
 run_async([this] {
   auto super = read_rbm_header();
   ASSERT_TRUE(
       super.block_size == DEFAULT_BLOCK_SIZE &&
       super.end == DEFAULT_TEST_SIZE 
   );
   config.block_size = 8196;
   mkfs();
   super = read_rbm_header();
   ASSERT_TRUE(
       super.block_size == 8196 &&
       super.end == DEFAULT_TEST_SIZE 
   );
 });
}

TEST_F(rbm_test_t, open_read_write_test)
{
 run_async([this] {
   auto content = generate_extent(1);
   {
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
   }
   close();
   open();
   {
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
   }
 });
}

