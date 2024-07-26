// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/os/seastore/seastore_types.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/transaction.h"
#include "rbm_device.h"
#include "crimson/os/seastore/random_block_manager.h"

#include "crimson/common/layout.h"
#include "include/buffer.h"
#include "include/uuid.h"
#include "avlallocator.h"


namespace crimson::os::seastore {

using RBMDevice = random_block_device::RBMDevice;
using RBMDeviceRef = std::unique_ptr<RBMDevice>;

device_config_t get_rbm_ephemeral_device_config(
    std::size_t index, std::size_t num_devices);

class BlockRBManager final : public RandomBlockManager {
public:
  /*
   * Ondisk layout (TODO)
   *
   * ---------------------------------------------------------------------------
   * | rbm_metadata_header_t | metadatas |        ...      |    data blocks    |
   * ---------------------------------------------------------------------------
   */

  read_ertr::future<> read(paddr_t addr, bufferptr &buffer) final;
  write_ertr::future<> write(paddr_t addr, bufferptr &buf) final;
  open_ertr::future<> open() final;
  close_ertr::future<> close() final;

  /*
   * alloc_extent
   *
   * The role of this function is to find out free blocks the transaction requires.
   * To do so, alloc_extent() looks into both in-memory allocator
   * and freebitmap blocks.
   *
   */
  paddr_t alloc_extent(size_t size) final; // allocator, return blocks

  allocate_ret_bare alloc_extents(size_t size) final; // allocator, return blocks

  void complete_allocation(paddr_t addr, size_t size) final;

  size_t get_start_rbm_addr() const {
    return device->get_shard_journal_start() + device->get_journal_size();
  }
  size_t get_size() const final {
    return device->get_shard_end() - get_start_rbm_addr();
  };
  extent_len_t get_block_size() const final { return device->get_block_size(); }

  BlockRBManager(RBMDevice * device, std::string path, bool detailed)
    : device(device), path(path) {
    allocator.reset(new AvlAllocator(detailed));
  }

  write_ertr::future<> write(rbm_abs_addr addr, bufferlist &bl);

  device_id_t get_device_id() const final {
    assert(device);
    return device->get_device_id();
  }

  uint64_t get_free_blocks() const final { 
    // TODO: return correct free blocks after block allocator is introduced
    assert(device);
    return get_size() / get_block_size();
  }
  const seastore_meta_t &get_meta() const final {
    return device->get_meta();
  }
  RBMDevice* get_device() {
    return device;
  }

  void mark_space_used(paddr_t paddr, size_t len) final {
    assert(allocator);
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + len <= device->get_shard_end());
    allocator->mark_extent_used(addr, len);
  }

  void mark_space_free(paddr_t paddr, size_t len) final {
    assert(allocator);
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + len <= device->get_shard_end());
    allocator->free_extent(addr, len);
  }

  paddr_t get_start() final {
    return convert_abs_addr_to_paddr(
      get_start_rbm_addr(),
      device->get_device_id());
  }

  rbm_extent_state_t get_extent_state(paddr_t paddr, size_t size) final {
    assert(allocator);
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + size <= device->get_shard_end());
    return allocator->get_extent_state(addr, size);
  }

  size_t get_journal_size() const final {
    return device->get_journal_size();
  }

#ifdef UNIT_TESTS_BUILT
  void prefill_fragmented_device() final;
#endif

private:
  /*
   * this contains the number of bitmap blocks, free blocks and
   * rbm specific information
   */
  ExtentAllocatorRef allocator;
  RBMDevice * device;
  std::string path;
  int stream_id; // for multi-stream
};
using BlockRBManagerRef = std::unique_ptr<BlockRBManager>;

}
