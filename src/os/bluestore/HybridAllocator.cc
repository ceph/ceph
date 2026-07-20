// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "HybridAllocator.h"

#define dout_context (T::get_context())
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << (std::string(this->get_type()) + "::").c_str()

/*
 * class HybridAvlAllocator
 *
 *
 */
const char* HybridAvlAllocator::get_type() const
{
  return "hybrid";
}

/*
 * class HybridBtree2Allocator
 *
 *
 */
const char* HybridBtree2Allocator::get_type() const
{
  return "hybrid_btree2";
}

int64_t HybridBtree2Allocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused and likely unneeded for btree2 allocator
  PExtentVector* extents)
{
  ldout(get_context(), 10) << __func__ << std::hex
    << " want 0x" << want
    << " unit 0x" << unit
    << " max_alloc_size 0x" << max_alloc_size
    << " hint 0x" << hint
    << std::dec << dendl;
  ceph_assert(std::has_single_bit(unit));
  ceph_assert(want % unit == 0);

  uint64_t cached_chunk_offs = 0;
  if (try_get_from_cache(&cached_chunk_offs, want)) {
    extents->emplace_back(cached_chunk_offs, want);
    return want;
  }
  return HybridAllocatorBase<Btree2Allocator>::allocate(want,
    unit, max_alloc_size, hint, extents);
}
void HybridBtree2Allocator::release(const release_set_t& release_set)
{
  if (!has_cache() || release_set.num_intervals() >= pextent_array_size) {
    HybridAllocatorBase<Btree2Allocator>::release(release_set);
    return;
  }
  PExtentArray to_release;
  size_t count = 0;
  auto p = release_set.begin();
  while (p != release_set.end()) {
    if (!try_put_cache(p.get_start(), p.get_len())) {
      to_release[count++] = &*p;// bluestore_pextent_t(p.get_start(), p.get_len());
    }
    ++p;
  }
  if (count > 0) {
    std::lock_guard l(get_lock());
    _release(count, &to_release.at(0));
  }
}

#include "HybridAllocator_impl.h"
