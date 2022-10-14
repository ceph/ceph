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
   * TODO: multiple allocation
   *
   */
  allocate_ret alloc_extent(
      Transaction &t, size_t size) final; // allocator, return blocks

  abort_allocation_ertr::future<> abort_allocation(Transaction &t) final;
  write_ertr::future<> complete_allocation(Transaction &t) final;

  size_t get_size() const final { return device->get_available_size(); };
  extent_len_t get_block_size() const final { return device->get_block_size(); }

  /*
   * We will have mulitple partitions (circularjournals and randbomblockmanagers)
   * on a device, so start and end location of the device are needed to
   * support such case.
   */
  BlockRBManager(RBMDevice * device, std::string path)
    : device(device), path(path) {}

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

private:
  /*
   * this contains the number of bitmap blocks, free blocks and
   * rbm specific information
   */
  //FreelistManager free_manager; // TODO: block management
  RBMDevice * device;
  std::string path;
  int stream_id; // for multi-stream
};
using BlockRBManagerRef = std::unique_ptr<BlockRBManager>;

}
