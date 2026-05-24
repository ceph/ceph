// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

paddr_t BlockRBManager::alloc_extent(size_t size, data_category_t category)
{
  LOG_PREFIX(BlockRBManager::alloc_extent);
  // Route to metadata pool only when it exists and the caller is asking
  // for metadata. Everything else (including data on v1 single-pool
  // devices) goes to data_allocator.
  ExtentAllocator *a =
      (metadata_allocator && category == data_category_t::METADATA)
          ? metadata_allocator.get()
          : data_allocator.get();
  auto alloc = a->alloc_extent(size);
  if (!alloc) {
    return P_ADDR_NULL;
  }
  ceph_assert((*alloc).num_intervals() == 1);
  auto extent = (*alloc).begin();
  ceph_assert(size == extent.get_len());
  paddr_t paddr = convert_abs_addr_to_paddr(
    extent.get_start(),
    device->get_device_id());
  DEBUG("allocated addr: {}, size: {}, requested size: {}, pool: {}",
	paddr, extent.get_len(), size,
	(a == metadata_allocator.get()) ? "metadata" : "data");
  return paddr;
}

BlockRBManager::allocate_ret_bare
BlockRBManager::alloc_extents(size_t size, data_category_t category)
{
  LOG_PREFIX(BlockRBManager::alloc_extents);
  ExtentAllocator *a =
      (metadata_allocator && category == data_category_t::METADATA)
          ? metadata_allocator.get()
          : data_allocator.get();
  auto alloc = a->alloc_extents(size);
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
    DEBUG("allocated addr: {}, size: {}, requested size: {}, pool: {}",
         paddr, extent.get_len(), size,
         (a == metadata_allocator.get()) ? "metadata" : "data");
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
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  auto *a = allocator_for(addr);
  assert(a);
  a->complete_allocation(addr, size);
}

BlockRBManager::open_ertr::future<> BlockRBManager::open()
{
  LOG_PREFIX(BlockRBManager::open);
  assert(device);
  assert(device->get_available_size() > 0);
  assert(device->get_block_size() > 0);
  auto pool_start = get_start_rbm_addr();
  auto data_start = get_data_start_rbm_addr();
  auto device_end = device->get_shard_end();
  auto meta_size = device->get_metadata_size();
  auto block_size = device->get_block_size();

  if (meta_size > 0) {
    // v2 pool-separated layout: carve [pool_start, data_start) for metadata
    // and [data_start, device_end) for data.
    ceph_assert(data_start > pool_start);
    ceph_assert(device_end > data_start);
    metadata_allocator.reset(new AvlAllocator(detailed));
    metadata_allocator->init(pool_start, data_start - pool_start, block_size);
    data_allocator->init(data_start, device_end - data_start, block_size);
    INFO("v2 pool-separated: metadata [0x{:x},0x{:x}) data [0x{:x},0x{:x})",
         pool_start, data_start, data_start, device_end);
  } else {
    // v1 single-pool layout: data_allocator covers everything.
    data_allocator->init(pool_start, device_end - pool_start, block_size);
    INFO("v1 single-pool: data [0x{:x},0x{:x})", pool_start, device_end);
  }
  return open_ertr::now();
}

bool BlockRBManager::check_valid_range(rbm_abs_addr addr, bufferptr &bptr) {
  LOG_PREFIX(BlockRBManager::check_valid_range);
  rbm_abs_addr start = device->get_shard_start();
  rbm_abs_addr end = device->get_shard_end();
  if (addr < start || addr + bptr.length() > end) {
    ERROR("out of range: start {}, end {}, addr {}, length {}",
      start, end, addr, bptr.length());
    return false;
  }
  return true;
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  paddr_t paddr,
  bufferptr bptr)
{
  ceph_assert(device);
  ceph_assert(bptr.is_page_aligned());
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (!check_valid_range(addr, bptr)) {
    co_return co_await write_ertr::future<>(
      crimson::ct_error::erange::make());
  }
  co_return co_await device->write(
    addr,
    bptr);
}

BlockRBManager::read_ertr::future<> BlockRBManager::read(
  paddr_t paddr,
  bufferptr &bptr)
{
  ceph_assert(device);
  ceph_assert(bptr.is_page_aligned());
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (!check_valid_range(addr, bptr)) {
    co_return co_await read_ertr::future<>(
      crimson::ct_error::erange::make());
  }
  co_return co_await device->read(
    addr,
    bptr);
}

BlockRBManager::close_ertr::future<> BlockRBManager::close()
{
  ceph_assert(device);
  data_allocator->close();
  if (metadata_allocator) {
    metadata_allocator->close();
  }
  co_return co_await device->close();
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
  co_return co_await device->write(
    addr,
    std::move(bptr));
}

#ifdef UNIT_TESTS_BUILT
void BlockRBManager::prefill_fragmented_device()
{
  LOG_PREFIX(BlockRBManager::prefill_fragmented_device);
  // the first 3 blocks must be allocated to lba root
  // and backref root during mkfs.
  // This test helper only fragments the data allocator (the metadata
  // allocator, if present, is not affected) since callers exercising the
  // fragmented-device assertions don't currently use pool separation.
  for (size_t block = get_block_size() * 3;
      block <= get_size() - get_block_size() * 3;
      block += get_block_size() * 2) {
    DEBUG("marking {}~{} used",
      get_data_start_rbm_addr() + block,
      get_block_size());
    data_allocator->mark_extent_used(
      get_data_start_rbm_addr() + block,
      get_block_size());
  }
}
#endif

}
