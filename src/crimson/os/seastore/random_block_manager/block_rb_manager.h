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

constexpr rbm_abs_addr RBM_START_ADDRESS = 0;
constexpr uint32_t RBM_SUPERBLOCK_SIZE = 4096;

using RBMDevice = random_block_device::RBMDevice;
using RBMDeviceRef = std::unique_ptr<RBMDevice>;

enum {
  // TODO: This allows the device to manage crc on a block by itself
  RBM_NVME_END_TO_END_PROTECTION = 1,
  RBM_BITMAP_BLOCK_CRC = 2,
};

struct rbm_metadata_header_t {
  size_t size = 0;
  size_t block_size = 0;
  uint64_t start; // start location of the device
  uint64_t end;   // end location of the device
  uint64_t magic; // to indicate randomblock_manager
  uuid_d uuid;
  uint32_t start_data_area;
  uint64_t flag; // reserved
  uint64_t feature;
  device_id_t device_id;
  checksum_t crc;

  DENC(rbm_metadata_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.block_size, p);
    denc(v.start, p);
    denc(v.end, p);
    denc(v.magic, p);
    denc(v.uuid, p);
    denc(v.start_data_area, p);
    denc(v.flag, p);
    denc(v.feature, p);
    denc(v.device_id, p);

    denc(v.crc, p);
    DENC_FINISH(p);
  }

};

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header);

}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_metadata_header_t
)

namespace crimson::os::seastore {

class BlockRBManager final : public RandomBlockManager {
public:
  /*
   * Ondisk layout (TODO)
   *
   * ---------------------------------------------------------------------------
   * | rbm_metadata_header_t | metadatas |        ...      |    data blocks    |
   * ---------------------------------------------------------------------------
   */

  mkfs_ertr::future<> mkfs(mkfs_config_t) final;
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

  read_ertr::future<rbm_metadata_header_t> read_rbm_header(rbm_abs_addr addr);
  write_ertr::future<> write_rbm_header();

  size_t get_size() const final { return super.size; };
  extent_len_t get_block_size() const final { return super.block_size; }

  /*
   * We will have mulitple partitions (circularjournals and randbomblockmanagers)
   * on a device, so start and end location of the device are needed to
   * support such case.
   */
  BlockRBManager(RBMDevice * device, std::string path)
    : device(device), path(path) {}

  write_ertr::future<> write(rbm_abs_addr addr, bufferlist &bl);

  device_id_t get_device_id() const final {
    return super.device_id;
  }

  uint64_t get_free_blocks() const final { 
    // TODO: return correct free blocks after block allocator is introduced
    return super.size / super.block_size;
  }

private:
  /*
   * this contains the number of bitmap blocks, free blocks and
   * rbm specific information
   */
  rbm_metadata_header_t super;
  //FreelistManager free_manager; // TODO: block management
  RBMDevice * device;
  std::string path;
  int stream_id; // for multi-stream
};
using BlockRBManagerRef = std::unique_ptr<BlockRBManager>;

}
