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

  uint64_t block_size = 0;
  uint64_t size = 0;

  device_config_t config;

  rbm_test_t() = default;

  seastar::future<> set_up_fut() final {
    device = random_block_device::create_test_ephemeral(
      random_block_device::DEFAULT_TEST_CBJOURNAL_SIZE, DEFAULT_TEST_SIZE);
    block_size = device->get_block_size();
    size = device->get_available_size();
    rbm_manager.reset(new BlockRBManager(device.get(), std::string(), false));
    config = get_rbm_ephemeral_device_config(0, 1);
    return device->mkfs(config).handle_error(crimson::ct_error::assert_all{}
    ).then([this] {
      return device->mount().handle_error(crimson::ct_error::assert_all{}
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
    return device->mkfs(config).unsafe_get0();
  }

  auto read_rbm_header() {
    return device->read_rbm_header(RBM_START_ADDRESS).unsafe_get0();
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
       super.block_size == block_size &&
       super.size == size
   );
   config.spec.id = DEVICE_ID_NULL;
   mkfs();
   super = read_rbm_header();
   ASSERT_TRUE(
       super.config.spec.id == DEVICE_ID_NULL &&
       super.size == size 
   );
 });
}

TEST_F(rbm_test_t, open_read_write_test)
{
 run_async([this] {
   auto content = generate_extent(1);
   {
     write(
	 block_size,
	 content
	 );
     auto bp = bufferptr(ceph::buffer::create_page_aligned(block_size));
     read(
	 block_size,
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
     auto bp = bufferptr(ceph::buffer::create_page_aligned(block_size));
     read(
	 block_size,
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

