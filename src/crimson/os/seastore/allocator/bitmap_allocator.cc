// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/allocator/bitmap_allocator.h"

namespace crimson::os::seastore {

BitmapAllocator::BitmapAllocator(allocator_spec_t spec)
  : spec(spec), hint(0) {
  _init(spec.capacity, spec.block_size);
}

void BitmapAllocator::mark_free(paddr_t paddr, extent_len_t length) {
  assert(paddr.get_device_id() == spec.device_id);
  auto offset = paddr.as_blk_paddr().get_block_off();
  assert(offset % spec.block_size == 0);
  assert(length % spec.block_size == 0);
  _mark_free(offset, length);
}

void BitmapAllocator::mark_used(paddr_t paddr, extent_len_t length) {
  assert(paddr.get_device_id() == spec.device_id);
  auto offset = paddr.as_blk_paddr().get_block_off();
  assert(offset % spec.block_size == 0);
  assert(length % spec.block_size == 0);
  _mark_allocated(offset, length);
}

Allocator::pextent_vec_t BitmapAllocator::allocate(size_t length) {
  uint64_t allocated = 0;
  port::interval_vector_t extents;

  _allocate_l2(
    length,
    spec.min_alloc_size,
    spec.max_alloc_size,
    hint,
    &allocated,
    &extents);

  if (!allocated)
    return {};

  hint = extents.back().offset + extents.back().length;

  pextent_vec_t res;
  res.reserve(extents.size());

  for (auto &e : extents) {
    res.push_back(pextent_t{paddr_t::make_blk_paddr(spec.device_id, e.offset), (extent_len_t)e.length});
  }

  return res;
}

void BitmapAllocator::release(paddr_t paddr, extent_len_t length) {
  assert(paddr.get_device_id() == spec.device_id);
  interval_set<uint64_t> release_set;
  release_set.insert(paddr.as_blk_paddr().get_block_off(), length);
  _free_l2(release_set);
}

}
