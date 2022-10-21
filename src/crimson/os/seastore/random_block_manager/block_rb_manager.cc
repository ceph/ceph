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

device_config_t get_rbm_ephemeral_device_config(
    std::size_t index, std::size_t num_devices)
{
  assert(num_devices > index);
  magic_t magic = 0xfffa;
  auto type = device_type_t::RANDOM_BLOCK_EPHEMERAL;
  bool is_major_device;
  secondary_device_set_t secondary_devices;
  if (index == 0) {
    is_major_device = true;
    for (std::size_t secondary_index = index + 1;
         secondary_index < num_devices;
         ++secondary_index) {
      device_id_t secondary_id = static_cast<device_id_t>(secondary_index);
      secondary_devices.insert({
        secondary_index, device_spec_t{magic, type, secondary_id}
      });
    }
  } else { // index > 0
    is_major_device = false;
  }

  device_id_t id = static_cast<device_id_t>(DEVICE_ID_RANDOM_BLOCK_MIN + index);
  seastore_meta_t meta = {};
  return {is_major_device,
          device_spec_t{magic, type, id},
          meta,
          secondary_devices};
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
  return device->read_rbm_header(RBM_START_ADDRESS
  ).safe_then([](auto s)
    -> open_ertr::future<> {
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
  LOG_PREFIX(BlockRBManager::write);
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  rbm_abs_addr start = 0;
  rbm_abs_addr end = device->get_available_size();
  if (addr < start || addr + bptr.length() > end) {
    ERROR("out of range: start {}, end {}, addr {}, length {}",
      start, end, addr, bptr.length());
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
  LOG_PREFIX(BlockRBManager::read);
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  rbm_abs_addr start = 0;
  rbm_abs_addr end = device->get_available_size();
  if (addr < start || addr + bptr.length() > end) {
    ERROR("out of range: start {}, end {}, addr {}, length {}",
      start, end, addr, bptr.length());
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
       << ", feature=" << header.feature
       << ", journal_size=" << header.journal_size
       << ", crc=" << header.crc
       << ", config=" << header.config;
  return out << ")";
}

}
