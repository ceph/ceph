// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/os/seastore/logging.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "include/interval_set.h"
#include "include/intarith.h"
#include "block_rb_manager.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

BlockRBManager::mkfs_ertr::future<> BlockRBManager::mkfs(mkfs_config_t config)
{
  LOG_PREFIX(BlockRBManager::mkfs);
  super.uuid = uuid_d(); // TODO
  super.magic = 0xFF; // TODO
  super.start = convert_paddr_to_abs_addr(
    config.start);
  super.end = convert_paddr_to_abs_addr(
    config.end);
  super.block_size = config.block_size;
  super.size = config.total_size;
  super.start_data_area = 0;
  super.crc = 0;
  super.feature |= RBM_BITMAP_BLOCK_CRC;
  super.device_id = config.device_id;

  DEBUG("super {} ", super);
  // write super block
  return write_rbm_header(
  ).safe_then([] {
    return mkfs_ertr::now();
  }).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error write_rbm_header in BlockRBManager::mkfs"
  });
}


/* TODO : block allocator */
BlockRBManager::allocate_ret BlockRBManager::alloc_extent(
    Transaction &t, size_t size)
{

  /*
   * 1. find free blocks using block allocator
   * 2. add free blocks to transaction
   *    (the free block is reserved state, not stored)
   * 3. link free blocks to onode
   * Due to in-memory block allocator is the next work to do,
   * just read the block bitmap directly to find free blocks.
   *
   */
  // TODO: block allocation using in-memory block allocator
  return allocate_ret(
    allocate_ertr::ready_future_marker{},
    paddr_t{});
}


BlockRBManager::abort_allocation_ertr::future<> BlockRBManager::abort_allocation(
    Transaction &t)
{
  /*
   * TODO: clear all allocation infos associated with transaction in in-memory allocator
   */
  return abort_allocation_ertr::now();
}

BlockRBManager::write_ertr::future<> BlockRBManager::complete_allocation(
    Transaction &t)
{
  return write_ertr::now();
}

BlockRBManager::open_ertr::future<> BlockRBManager::open()
{
  return read_rbm_header(RBM_START_ADDRESS
  ).safe_then([&](auto s)
    -> open_ertr::future<> {
    if (s.magic != 0xFF) {
      return crimson::ct_error::enoent::make();
    }
    super = s;
    return open_ertr::now();
  }).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error read_rbm_header in BlockRBManager::open"
    }
  );
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  paddr_t paddr,
  bufferptr &bptr)
{
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (addr > super.end || addr < super.start ||
      bptr.length() > super.end - super.start) {
    return crimson::ct_error::erange::make();
  }
  return device->write(
    addr,
    bptr);
}

BlockRBManager::read_ertr::future<> BlockRBManager::read(
  paddr_t paddr,
  bufferptr &bptr)
{
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (addr > super.end || addr < super.start ||
      bptr.length() > super.end - super.start) {
    return crimson::ct_error::erange::make();
  }
  return device->read(
    addr,
    bptr);
}

BlockRBManager::close_ertr::future<> BlockRBManager::close()
{
  ceph_assert(device);
  return device->close();
}


BlockRBManager::write_ertr::future<> BlockRBManager::write_rbm_header()
{
  bufferlist meta_b_header;
  super.crc = 0;
  encode(super, meta_b_header);
  // If NVMeDevice supports data protection, CRC for checksum is not required
  // NVMeDevice is expected to generate and store checksum internally.
  // CPU overhead for CRC might be saved.
  if (device->is_data_protection_enabled()) {
    super.crc = -1;
  }
  else {
    super.crc = meta_b_header.crc32c(-1);
  }

  bufferlist bl;
  encode(super, bl);
  auto iter = bl.begin();
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  assert(bl.length() < super.block_size);
  iter.copy(bl.length(), bp.c_str());

  return device->write(super.start, bp);
}

BlockRBManager::read_ertr::future<rbm_metadata_header_t> BlockRBManager::read_rbm_header(
    rbm_abs_addr addr)
{
  LOG_PREFIX(BlockRBManager::read_rbm_header);
  ceph_assert(device);
  bufferptr bptr =
    bufferptr(ceph::buffer::create_page_aligned(RBM_SUPERBLOCK_SIZE));
  bptr.zero();
  return device->read(
    addr,
    bptr
  ).safe_then([length=bptr.length(), this, bptr, FNAME]()
    -> read_ertr::future<rbm_metadata_header_t> {
    bufferlist bl;
    bl.append(bptr);
    auto p = bl.cbegin();
    rbm_metadata_header_t super_block;
    try {
      decode(super_block, p);
    }
    catch (ceph::buffer::error& e) {
      DEBUG("read_rbm_header: unable to decode rbm super block {}",
	    e.what());
      return crimson::ct_error::enoent::make();
    }
    checksum_t crc = super_block.crc;
    bufferlist meta_b_header;
    super_block.crc = 0;
    encode(super_block, meta_b_header);

    // Do CRC verification only if data protection is not supported.
    if (device->is_data_protection_enabled() == false) {
      if (meta_b_header.crc32c(-1) != crc) {
        DEBUG("bad crc on super block, expected {} != actual {} ",
              meta_b_header.crc32c(-1), crc);
        return crimson::ct_error::input_output_error::make();
      }
    }
    DEBUG("got {} ", super);
    return read_ertr::future<rbm_metadata_header_t>(
      read_ertr::ready_future_marker{},
      super_block
    );
  }).handle_error(
    read_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BlockRBManager::read_rbm_header"
    }
  );
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  rbm_abs_addr addr,
  bufferlist &bl)
{
  LOG_PREFIX(BlockRBManager::write);
  ceph_assert(device);
  bufferptr bptr;
  try {
    bptr = bufferptr(ceph::buffer::create_page_aligned(bl.length()));
    auto iter = bl.cbegin();
    iter.copy(bl.length(), bptr.c_str());
  } catch (const std::exception &e) {
    DEBUG("write: exception creating aligned buffer {}", e);
    ceph_assert(0 == "unhandled exception");
  }
  return device->write(
    addr,
    bptr);
}

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header)
{
  out << " rbm_metadata_header_t(size=" << header.size
       << ", block_size=" << header.block_size
       << ", start=" << header.start
       << ", end=" << header.end
       << ", magic=" << header.magic
       << ", uuid=" << header.uuid
       << ", start_data_area=" << header.start_data_area
       << ", flag=" << header.flag
       << ", feature=" << header.feature
       << ", crc=" << header.crc;
  return out << ")";
}

}
