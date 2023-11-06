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

paddr_t BlockRBManager::alloc_extent(size_t size)
{
  LOG_PREFIX(BlockRBManager::alloc_extent);
  assert(allocator);
  auto alloc = allocator->alloc_extent(size);
  if (!alloc) {
    return P_ADDR_NULL;
  }
  ceph_assert((*alloc).num_intervals() == 1);
  auto extent = (*alloc).begin();
  ceph_assert(size == extent.get_len());
  paddr_t paddr = convert_abs_addr_to_paddr(
    extent.get_start(),
    device->get_device_id());
  DEBUG("allocated addr: {}, size: {}, requested size: {}",
	paddr, extent.get_len(), size);
  return paddr;
}

BlockRBManager::allocate_ret_bare
BlockRBManager::alloc_extents(size_t size)
{
  LOG_PREFIX(BlockRBManager::alloc_extents);
  assert(allocator);
  auto alloc = allocator->alloc_extents(size);
  if (!alloc) {
    return {};
  }
  allocate_ret_bare ret;
  size_t len = 0;
  for (auto extent = (*alloc).begin();
       extent != (*alloc).end();
       extent++) {
    len += extent.get_len();
    paddr_t paddr = convert_abs_addr_to_paddr(
      extent.get_start(),
      device->get_device_id());
    DEBUG("allocated addr: {}, size: {}, requested size: {}",
         paddr, extent.get_len(), size);
    ret.push_back(
      {std::move(paddr),
      static_cast<extent_len_t>(extent.get_len())});
  }
  ceph_assert(size == len);
  return ret;
}

void BlockRBManager::complete_allocation(
    paddr_t paddr, size_t size)
{
  assert(allocator);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  allocator->complete_allocation(addr, size);
}

BlockRBManager::open_ertr::future<> BlockRBManager::open()
{
  assert(device);
  assert(device->get_available_size() > 0);
  assert(device->get_block_size() > 0);
  auto ool_start = get_start_rbm_addr();
  allocator->init(
    ool_start,
    device->get_shard_end() -
    ool_start,
    device->get_block_size());
  return open_ertr::now();
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  paddr_t paddr,
  bufferptr &bptr)
{
  LOG_PREFIX(BlockRBManager::write);
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  rbm_abs_addr start = device->get_shard_start();
  rbm_abs_addr end = device->get_shard_end();
  if (addr < start || addr + bptr.length() > end) {
    ERROR("out of range: start {}, end {}, addr {}, length {}",
      start, end, addr, bptr.length());
    return crimson::ct_error::erange::make();
  }
  bufferptr bp = bufferptr(ceph::buffer::create_page_aligned(bptr.length()));
  bp.copy_in(0, bptr.length(), bptr.c_str());
  return device->write(
    addr,
    std::move(bp));
}

BlockRBManager::read_ertr::future<> BlockRBManager::read(
  paddr_t paddr,
  bufferptr &bptr)
{
  LOG_PREFIX(BlockRBManager::read);
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  rbm_abs_addr start = device->get_shard_start();
  rbm_abs_addr end = device->get_shard_end();
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
  allocator->close();
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
    std::move(bptr));
}

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header)
{
  out << " rbm_metadata_header_t(size=" << header.size
       << ", block_size=" << header.block_size
       << ", feature=" << header.feature
       << ", journal_size=" << header.journal_size
       << ", crc=" << header.crc
       << ", config=" << header.config
       << ", shard_num=" << header.shard_num;
  for (auto p : header.shard_infos) {
    out << p;
  }
  return out << ")";
}

std::ostream &operator<<(std::ostream &out, const rbm_shard_info_t &shard)
{
  out << " rbm_shard_info_t(size=" << shard.size
      << ", start_offset=" << shard.start_offset;
  return out << ")";
}

}
