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
#include "nvmedevice.h"
#include "crimson/os/seastore/random_block_manager.h"

#include "crimson/common/layout.h"
#include "include/buffer.h"
#include "include/uuid.h"

namespace crimson::os::seastore {

constexpr uint32_t RBM_SUPERBLOCK_SIZE = 4096;

using NVMeBlockDevice = nvme_device::NVMeBlockDevice;
using NVMeBlockDeviceRef = std::unique_ptr<NVMeBlockDevice>;

enum {
  // TODO: This allows the device to manage crc on a block by itself
  RBM_NVME_END_TO_END_PROTECTION = 1,
  RBM_BITMAP_BLOCK_CRC = 2,
};

constexpr uint32_t BITS_PER_CHAR = 8;
inline char BIT_CHAR_MASK(uint64_t nr)
{
  return (char(1) << (nr % BITS_PER_CHAR));
}

struct rbm_metadata_header_t {
  size_t size = 0;
  size_t block_size = 0;
  uint64_t start; // start location of the device
  uint64_t end;   // end location of the device
  uint64_t magic; // to indicate randomblock_manager
  uuid_d uuid;
  uint64_t free_block_count;
  uint64_t alloc_area_size; // bitmap
  uint32_t start_alloc_area; // block number
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
    denc(v.free_block_count, p);
    denc(v.alloc_area_size, p);
    denc(v.start_alloc_area, p);
    denc(v.start_data_area, p);
    denc(v.flag, p);
    denc(v.feature, p);
    denc(v.device_id, p);

    denc(v.crc, p);
    DENC_FINISH(p);
  }

};

struct rbm_bitmap_block_header_t {
  uint32_t size;
  checksum_t checksum;
  DENC(rbm_bitmap_block_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.checksum, p);
    DENC_FINISH(p);
  }
};

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header);
std::ostream &operator<<(std::ostream &out, const rbm_bitmap_block_header_t &header);

enum class bitmap_op_types_t : uint8_t {
  ALL_CLEAR = 1,
  ALL_SET = 2
};

struct rbm_bitmap_block_t {
  rbm_bitmap_block_header_t header;
  bufferlist buf;

  uint64_t get_size() {
    return header.size;
  }
  void set_crc() {
    header.checksum = buf.crc32c(-1);
  }

  bool is_correct_crc() {
    ceph_assert(buf.length());
    return buf.crc32c(-1) == header.checksum;
  }

  void set_bit(uint64_t nr) {
    ceph_assert(buf.length());
    char mask = BIT_CHAR_MASK(nr);
    char *p = buf.c_str() + (nr / BITS_PER_CHAR);
    *p |= mask;
  }

  void set_all_bits() {
    ceph_assert(buf.length());
    ::memset(buf.c_str(), std::numeric_limits<unsigned char>::max(), buf.length());
  }

  void set_clear_bits() {
    ceph_assert(buf.length());
    ::memset(buf.c_str(), 0, buf.length());
  }

  void clear_bit(uint64_t nr) {
    ceph_assert(buf.length());
    char mask = ~BIT_CHAR_MASK(nr);
    char *p = buf.c_str() + (nr / BITS_PER_CHAR);
    *p &= mask;
  }

  bool is_allocated(uint64_t nr) {
    ceph_assert(buf.length());
    char mask = BIT_CHAR_MASK(nr);
    char *p = buf.c_str() + (nr / BITS_PER_CHAR);
    return *p & mask;
  }

  rbm_bitmap_block_t(size_t size) {
    header.size = size;
  }

  rbm_bitmap_block_t() = default;

  DENC(rbm_bitmap_block_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.header, p);
    denc(v.buf, p);
    DENC_FINISH(p);
  }
};

}

WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_metadata_header_t
)
WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_bitmap_block_t
)
WRITE_CLASS_DENC_BOUNDED(
  crimson::os::seastore::rbm_bitmap_block_header_t
)

namespace crimson::os::seastore {

class NVMeManager final : public RandomBlockManager {
public:
  /*
   * Ondisk layout
   *
   * ---------------------------------------------------------------------------
   * | rbm_metadata_header_t | rbm_bitmap_block_t 1 |  ... |    data blocks    |
   * ---------------------------------------------------------------------------
   */

  mkfs_ertr::future<> mkfs(mkfs_config_t) final;
  read_ertr::future<> read(paddr_t addr, bufferptr &buffer) final;
  write_ertr::future<> write(paddr_t addr, bufferptr &buf) final;
  open_ertr::future<> open(const std::string &path, paddr_t start) final;
  close_ertr::future<> close() final;

  /*
   * alloc_extent
   *
   * The role of this function is to find out free blocks the transaction requires.
   * To do so, alloc_extent() looks into both in-memory allocator
   * and freebitmap blocks.
   * But, in-memory allocator is the future work, and is not implemented yet,
   * we use freebitmap directly to allocate freeblocks for now.
   *
   * Each bit in freebitmap block represents whether a block is allocated or not.
   *
   * TODO: multiple allocation
   *
   */
  allocate_ret alloc_extent(
      Transaction &t, size_t size) final; // allocator, return blocks

  /*
   * free_extent
   *
   * add a range of free blocks to transaction
   *
   */
  abort_allocation_ertr::future<> abort_allocation(Transaction &t) final;
  write_ertr::future<> complete_allocation(Transaction &t) final;

  open_ertr::future<> _open_device(const std::string path);
  read_ertr::future<rbm_metadata_header_t> read_rbm_header(rbm_abs_addr addr);
  write_ertr::future<> write_rbm_header();

  size_t get_size() const final { return super.size; };
  size_t get_block_size() const final { return super.block_size; }

  // max block number a block can represent using bitmap
  uint64_t max_block_by_bitmap_block() {
    return (super.block_size - ceph::encoded_sizeof_bounded<rbm_bitmap_block_t>()) * 8;
  }

  uint64_t convert_block_no_to_bitmap_block(blk_no_t block_no)
  {
    ceph_assert(super.block_size);
    return block_no / max_block_by_bitmap_block();
  }

  /*
   * convert_bitmap_block_no_to_block_id
   *
   * return block id using address where freebitmap is stored and offset
   */
  blk_no_t convert_bitmap_block_no_to_block_id(uint64_t offset, rbm_abs_addr addr)
  {
    ceph_assert(super.block_size);
    // freebitmap begins at block 1
    return (addr / super.block_size - 1) * max_block_by_bitmap_block() + offset;
  }

  uint64_t get_alloc_area_size() {
    ceph_assert(super.size);
    ceph_assert(super.block_size);
    uint64_t total_block_num = super.size / super.block_size;
    uint64_t need_blocks = (total_block_num % max_block_by_bitmap_block()) ?
		  (total_block_num / max_block_by_bitmap_block() + 1) :
		  (total_block_num / max_block_by_bitmap_block());
    ceph_assert(need_blocks);
    return need_blocks * super.block_size;
  }

  using find_block_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::enoent>;
  using find_block_ret = find_block_ertr::future<interval_set<blk_no_t>>;
  /*
   * find_free_block
   *
   * Try to find free blocks by reading bitmap blocks on the disk sequentially
   * The free blocks will be added to allocated_blocks in Transaction.
   * This needs to be improved after in-memory block allocation is introduced.
   *
   */
  find_block_ret find_free_block(Transaction &t, size_t size);

  /*
   * rbm_sync_block_bitmap
   *
   * Write rbm_bitmap_block_t to the device
   *
   * @param rbm_bitmap_block_t
   * @param uint64_t the block number the rbm_bitmap_block_t will be stored
   *
   */
  write_ertr::future<> rbm_sync_block_bitmap(
      rbm_bitmap_block_t &block, blk_no_t block_no);

  using check_bitmap_blocks_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg>;
  check_bitmap_blocks_ertr::future<> check_bitmap_blocks();
  uint64_t get_free_blocks() const {
    return super.free_block_count;
  }
  /*
   * We will have mulitple partitions (circularjournals and randbomblockmanagers)
   * on a device, so start and end location of the device are needed to
   * support such case.
   */
  NVMeManager(NVMeBlockDevice * device, std::string path)
    : device(device), path(path) {}

  /*
   * bitmap block area (freebitmap) layout
   *
   * -----------------------------------------------------------
   * | header   1 |   bitmap  1   | header  2 |    bitmap  2   |
   * -----------------------------------------------------------
   *  <--       1 block        --> <--     1 block          -->
   *
   * 1 block contains both bitmap header and bitmap.
   * We use this layout as a default layout here.
   * But, we'll consider to exploit end to end data protection.
   * If we use the end to end data protection, which is a feature specified in NVMe,
   * we can avoid any calculation for checksum. The checksum regarding the block
   * will be managed by the NVMe device.
   *
   */
  mkfs_ertr::future<> initialize_blk_alloc_area();
  uint64_t get_start_block_alloc_area() {
    return super.start_alloc_area;
  }

  void alloc_rbm_bitmap_block_buf(rbm_bitmap_block_t &b_block) {
    auto bitmap_blk = ceph::bufferptr(buffer::create_page_aligned(
			super.block_size -
			ceph::encoded_sizeof_bounded<rbm_bitmap_block_t>()));
    bitmap_blk.zero();
    b_block.buf.append(bitmap_blk);
  }

  rbm_abs_addr get_blk_paddr_by_block_no(blk_no_t id) {
    return (id * super.block_size) + super.start;
  }

  int num_block_between_blk_ids(blk_no_t start, blk_no_t end) {
    auto max = max_block_by_bitmap_block();
    auto block_start = start / max;
    auto block_end = end / max;
    return block_end - block_start + 1;
  }

  write_ertr::future<> rbm_sync_block_bitmap_by_range(
      blk_no_t start, blk_no_t end, bitmap_op_types_t op);
  void add_cont_bitmap_blocks_to_buf(
      bufferlist& buf, int num_block, bitmap_op_types_t op) {
    rbm_bitmap_block_t b_block(super.block_size);
    alloc_rbm_bitmap_block_buf(b_block);
    if (op == bitmap_op_types_t::ALL_SET) {
      b_block.set_all_bits();
    } else {
      b_block.set_clear_bits();
    }
    for (int i = 0; i < num_block; i++) {
      encode(b_block, buf);
    }
  }

  write_ertr::future<> write(rbm_abs_addr addr, bufferlist &bl);
  write_ertr::future<> sync_allocation(
      std::vector<rbm_alloc_delta_t>& alloc_blocks);
  void add_free_extent(
      std::vector<rbm_alloc_delta_t>& v, rbm_abs_addr from, size_t len);

  device_id_t get_device_id() const final {
    return super.device_id;
  }

private:
  /*
   * this contains the number of bitmap blocks, free blocks and
   * rbm specific information
   */
  rbm_metadata_header_t super;
  //FreelistManager free_manager; // TODO: block management
  NVMeBlockDevice * device;
  std::string path;
  int stream_id; // for multi-stream
};
using NVMeManagerRef = std::unique_ptr<NVMeManager>;

}
