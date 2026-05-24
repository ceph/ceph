// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
   * -------------------------------------------------------------------------------
   * | 23B magic | 37B null padding | DENC-encoded superblock header | data blocks |
   * |           |                  | header (device_superblock_t)   |             |
   * -------------------------------------------------------------------------------
   */

  read_ertr::future<> read(paddr_t addr, bufferptr &buffer) override;
  write_ertr::future<> write(paddr_t addr, bufferptr buf) override;
  open_ertr::future<> open() override;
  close_ertr::future<> close() override;

  /*
   * alloc_extent
   *
   * The role of this function is to find out free blocks the transaction requires.
   * To do so, alloc_extent() looks into both in-memory allocator
   * and freebitmap blocks.
   *
   * On v2 (pool-separated) devices, routes the request to the metadata or
   * data allocator based on `category`. On v1 single-pool devices (or v2
   * devices configured with metadata_size==0) the data allocator backs
   * both categories.
   */
  paddr_t alloc_extent(
      size_t size,
      data_category_t category = data_category_t::DATA) override;

  allocate_ret_bare alloc_extents(
      size_t size,
      data_category_t category = data_category_t::DATA) override;

  void complete_allocation(paddr_t addr, size_t size) override;

  // Address of the first byte past the journal — start of the metadata
  // pool on v2 devices, or start of the (single) data pool on v1.
  size_t get_start_rbm_addr() const {
    return device->get_shard_journal_start() + device->get_journal_size();
  }
  // Start of the data pool. On v1 (metadata_size==0) this equals
  // get_start_rbm_addr(); on v2 it's after the metadata region.
  size_t get_data_start_rbm_addr() const {
    return get_start_rbm_addr() + device->get_metadata_size();
  }
  // Total size of the data + metadata region (excluding the journal).
  size_t get_size() const override {
    return device->get_shard_end() - get_start_rbm_addr();
  };
  size_t get_data_size() const {
    return device->get_shard_end() - get_data_start_rbm_addr();
  }
  size_t get_metadata_size() const {
    return device->get_metadata_size();
  }
  extent_len_t get_block_size() const override { return device->get_block_size(); }

  BlockRBManager(RBMDevice * device, std::string path, bool detailed)
    : device(device), path(path), detailed(detailed) {
    data_allocator.reset(new AvlAllocator(detailed));
    // metadata_allocator is constructed lazily in open() iff the device
    // has a non-zero metadata region (v2 layout). On v1 it stays null.
  }

  write_ertr::future<> write(rbm_abs_addr addr, bufferlist &bl);

  device_id_t get_device_id() const override {
    assert(device);
    return device->get_device_id();
  }

  uint64_t get_free_blocks() const override { 
    // TODO: return correct free blocks after block allocator is introduced
    assert(device);
    return get_size() / get_block_size();
  }
  const seastore_meta_t &get_meta() const override {
    return device->get_meta();
  }
  RBMDevice* get_device() {
    return device;
  }

  // Route a paddr to the allocator that owns the underlying physical
  // range. On v2 devices, addresses below get_data_start_rbm_addr() belong
  // to the metadata allocator; the rest to the data allocator. On v1
  // (metadata_allocator == null) everything goes to the data allocator.
  ExtentAllocator *allocator_for(rbm_abs_addr addr) const {
    if (metadata_allocator && addr < get_data_start_rbm_addr()) {
      return metadata_allocator.get();
    }
    return data_allocator.get();
  }

  void mark_space_used(paddr_t paddr, size_t len) override {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + len <= device->get_shard_end());
    auto *a = allocator_for(addr);
    assert(a);
    a->mark_extent_used(addr, len);
  }

  void mark_space_free(paddr_t paddr, size_t len) override {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + len <= device->get_shard_end());
    auto *a = allocator_for(addr);
    assert(a);
    a->free_extent(addr, len);
  }

  paddr_t get_start() override {
    return convert_abs_addr_to_paddr(
      get_start_rbm_addr(),
      device->get_device_id());
  }

  rbm_extent_state_t get_extent_state(paddr_t paddr, size_t size) override {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
    assert(addr >= get_start_rbm_addr() &&
	   addr + size <= device->get_shard_end());
    auto *a = allocator_for(addr);
    assert(a);
    return a->get_extent_state(addr, size);
  }

  size_t get_journal_size() const override {
    return device->get_journal_size();
  }

  bool check_valid_range(rbm_abs_addr paddr, bufferptr &bptr);

#ifdef UNIT_TESTS_BUILT
  void prefill_fragmented_device() override;
#endif

private:
  /*
   * Allocators. data_allocator covers the data pool (the trailing portion
   * of the device past the journal and the metadata region). On v2 devices
   * with a non-zero metadata region, metadata_allocator covers
   * [get_start_rbm_addr(), get_data_start_rbm_addr()); on v1 devices it is
   * null and the data_allocator covers the entire post-journal range.
   */
  ExtentAllocatorRef data_allocator;
  ExtentAllocatorRef metadata_allocator;
  RBMDevice * device;
  std::string path;
  bool detailed;
  int stream_id; // for multi-stream
};
using BlockRBManagerRef = std::unique_ptr<BlockRBManager>;

}
