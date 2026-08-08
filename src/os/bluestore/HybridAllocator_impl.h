// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "HybridAllocator.h"

#define dout_context (T::get_context())
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << (std::string(this->get_type()) + "::")

template <typename T>
int64_t HybridAllocatorBase<T>::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint,
  PExtentVector* extents)
{
  dout(10) << __func__ << std::hex
    << " 0x" << want
    << "/" << unit
    << "," << max_alloc_size
    << "," << hint
    << std::dec << dendl;
  ceph_assert(std::has_single_bit(unit));
  ceph_assert(want % unit == 0);

  if (max_alloc_size == 0) {
    max_alloc_size = want;
  }
  if (constexpr auto cap = std::numeric_limits<uint32_t>::max();
    max_alloc_size >= cap) {
    max_alloc_size = p2align(uint64_t(cap), (uint64_t)T::get_block_size());
  }

  auto lock_wait_start = mono_clock::now();

  std::lock_guard l(T::get_lock());

  auto lock_acquired = mono_clock::now();

  // try bitmap first to avoid unneeded contiguous extents split if
  // desired amount is less than shortes range in AVL or Btree2
  bool primary_first = !(bmap_alloc &&
                         bmap_alloc->get_free() &&
                         want < T::_lowest_size_available());

  int64_t res = _allocate_or_rollback(primary_first,
    want, unit, max_alloc_size, hint, extents);
  ceph_assert(res >= 0);
  if ((uint64_t)res < want) {
    auto orig_size = extents->size();
    int64_t res2 = 0;
    // try alternate allocator
    if (!primary_first) {
      res2 = T::_allocate(want - res, unit, max_alloc_size, hint, extents);
    } else if (bmap_alloc) {
      res2 =
        bmap_alloc->allocate(want - res, unit, max_alloc_size, hint, extents);
    }
    if (res2 >= 0) {
      res += res2;
    } else {
      // allocator shouldn't return new extents on error
      ceph_assert(orig_size == extents->size());
    }
  }
  this->logger->tinc_with_max(
      l_bluestore_allocator_alloc_process_lat,
      mono_clock::now() - lock_acquired);
  this->logger->tinc_with_max(
      l_bluestore_allocator_lock_wait_lat,
      lock_acquired - lock_wait_start);
  return res ? res : -ENOSPC;
}

template <typename T>
void HybridAllocatorBase<T>::dump()
{
  std::lock_guard l(T::get_lock());
  T::_dump();
  if (bmap_alloc) {
    bmap_alloc->dump();
  }
  dout(0) << __func__
    << " avl_free: " << T::_get_free()
    << " bmap_free: " << (bmap_alloc ? bmap_alloc->get_free() : 0)
    << dendl;
}

template <typename T>
void HybridAllocatorBase<T>::init_rm_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(T::get_lock());
  dout(10) << __func__ << std::hex
    << " offset 0x" << offset
    << " length 0x" << length
    << std::dec << dendl;
  T::_try_remove_from_tree(offset, length,
    [&](uint64_t o, uint64_t l, bool found) {
      if (!found) {
        if (bmap_alloc) {
          bmap_alloc->init_rm_free(o, l);
        } else {
          derr << __func__ << " lambda " << std::hex
            << "Uexpected extent: "
            << " 0x" << o << "~" << l
            << std::dec << dendl;
          ceph_assert(false);
        }
      }
    });
}

template <typename T>
void HybridAllocatorBase<T>::_spillover_range(uint64_t start, uint64_t end)
{
  auto size = end - start;
  dout(20) << __func__
    << std::hex << " "
    << start << "~" << size
    << std::dec
    << dendl;
  ceph_assert(size);
  if (!bmap_alloc) {
    dout(1) << __func__
      << " constructing fallback allocator"
      << dendl;
    bmap_alloc = std::make_unique<BitmapAllocator>(T::get_context(),
      T::get_capacity(),
      T::get_block_size(),
      T::get_name() + ".fallback");
  }
  bmap_alloc->init_add_free(start, size);
}

template <typename T>
uint64_t HybridAllocatorBase<T>::_spillover_allocate(uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint,
  PExtentVector* extents)
{
  return _allocate_or_rollback(false,
    want,
    unit,
    max_alloc_size,
    hint,
    extents);
}

template <typename T>
uint64_t HybridAllocatorBase<T>::get_free_extents(
  uint64_t range_begin,
  uint64_t range_end,
  size_t max_count,
  free_extent_vector_t* out)
{
  if (!bmap_alloc) {
    return T::get_free_extents(range_begin, range_end, max_count, out);
  }

  // Fetch up to max_count from each sub-allocator independently.
  // Both return extents sorted by offset; we merge them in O(n+m).
  free_extent_vector_t primary_out, bmap_out;
  uint64_t primary_cursor = T::get_free_extents(
    range_begin, range_end, max_count, &primary_out);
  uint64_t bmap_cursor = bmap_alloc->get_free_extents(
    range_begin, range_end, max_count, &bmap_out);

  auto pi = primary_out.begin();
  auto bi = bmap_out.begin();
  const bool unbounded = (max_count == 0);
  size_t n = 0;

  while ((pi != primary_out.end() || bi != bmap_out.end()) &&
         (unbounded || n < max_count)) {
    bool take_primary;
    if (pi == primary_out.end()) {
      take_primary = false;
    } else if (bi == bmap_out.end()) {
      take_primary = true;
    } else {
      take_primary = (pi->offset <= bi->offset);
    }
    out->push_back(take_primary ? *pi++ : *bi++);
    ++n;
  }

  // Resume cursor: the minimum next-unprocessed offset from either stream.
  // Prefer batch leftovers (exact extent offsets) over sub-cursors (used only
  // when a batch was exhausted but the sub-allocator signalled more remains).
  uint64_t cursor = range_end;
  if (pi != primary_out.end()) {
    cursor = std::min(cursor, pi->offset);
  } else if (primary_cursor < range_end) {
    cursor = std::min(cursor, primary_cursor);
  }
  if (bi != bmap_out.end()) {
    cursor = std::min(cursor, bi->offset);
  } else if (bmap_cursor < range_end) {
    cursor = std::min(cursor, bmap_cursor);
  }
  return cursor;
}

template <typename PrimaryAllocator>
uint64_t HybridAllocatorBase<PrimaryAllocator>::_allocate_or_rollback(
  bool primary,
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint,
  PExtentVector* extents)
{
  int64_t res = 0;
  ceph_assert(extents);
  // preserve original 'extents' vector state
  auto orig_size = extents->size();
  if (primary) {
    res = PrimaryAllocator::_allocate(want, unit, max_alloc_size, hint, extents);
  } else if (bmap_alloc) {
    res = bmap_alloc->allocate(want, unit, max_alloc_size, hint, extents);
  }
  if (res < 0) {
    // got a failure, release already allocated
    PExtentVector local_extents;
    PExtentVector* e = extents;
    if (orig_size) {
      local_extents.insert(
        local_extents.end(), extents->begin() + orig_size, extents->end());
      e = &local_extents;
    }

    if (e->size()) {
      if(primary) {
        PrimaryAllocator::_release(*e);
      } else if (bmap_alloc) {
        bmap_alloc->release(*e);
      }
    }
    extents->resize(orig_size);
    res = 0;
  }
  return (uint64_t)res;
}
