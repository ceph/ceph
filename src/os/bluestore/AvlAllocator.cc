// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AvlAllocator.h"

#include <bit>
#include <limits>

#include "common/config_proxy.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << "AvlAllocator "

MEMPOOL_DEFINE_OBJECT_FACTORY(range_seg_t, range_seg_t, bluestore_alloc);

namespace {
  // a light-weight "range_seg_t", which only used as the key when searching in
  // range_tree and range_size_tree
  struct range_t {
    uint64_t start;
    uint64_t end;
  };
}

/*
 * This is a helper function that can be used by the allocator to find
 * a suitable block to allocate. This will search the specified AVL
 * tree looking for a block that matches the specified criteria.
 */
uint64_t AvlAllocator::_pick_block_after(uint64_t *cursor,
					 uint64_t size,
					 uint64_t align)
{
  const auto compare = range_tree.key_comp();
  uint32_t search_count = 0;
  uint64_t search_bytes = 0;
  auto rs_start = range_tree.lower_bound(range_t{*cursor, size}, compare);
  for (auto rs = rs_start; rs != range_tree.end(); ++rs) {
    uint64_t offset = rs->start;
    *cursor = offset + size;
    if (offset + size <= rs->end) {
      return offset;
    }
    if (max_search_count > 0 && ++search_count > max_search_count) {
      return -1ULL;
    }
    if (search_bytes = rs->start - rs_start->start;
	max_search_bytes > 0 && search_bytes > max_search_bytes) {
      return -1ULL;
    }
  }

  if (*cursor == 0) {
    // If we already started from beginning, don't bother with searching from beginning
    return -1ULL;
  }
  // If we reached end, start from beginning till cursor.
  for (auto rs = range_tree.begin(); rs != rs_start; ++rs) {
    uint64_t offset = rs->start;
    *cursor = offset + size;
    if (offset + size <= rs->end) {
      return offset;
    }
    if (max_search_count > 0 && ++search_count > max_search_count) {
      return -1ULL;
    }
    if (max_search_bytes > 0 && search_bytes + rs->start > max_search_bytes) {
      return -1ULL;
    }
  }
  return -1ULL;
}

uint64_t AvlAllocator::_pick_block_fits(uint64_t size,
					uint64_t align)
{
  // instead of searching from cursor, just pick the smallest range which fits
  // the needs
  const auto compare = range_size_tree.key_comp();
  auto rs_start = range_size_tree.lower_bound(range_t{0, size}, compare);
  for (auto rs = rs_start; rs != range_size_tree.end(); ++rs) {
    uint64_t offset = rs->start;
    if (offset + size <= rs->end) {
      return offset;
    }
  }
  return -1ULL;
}

void AvlAllocator::_add_to_tree(uint64_t start, uint64_t size)
{
  ceph_assert(size != 0);

  uint64_t end = start + size;

  auto rs_after = range_tree.upper_bound(range_t{start, end},
					 range_tree.key_comp());

  /* Make sure we don't overlap with either of our neighbors */
  auto rs_before = range_tree.end();
  if (rs_after != range_tree.begin()) {
    rs_before = std::prev(rs_after);
  }

  if ((rs_before != range_tree.end() && rs_before->end > start) ||
      (rs_after != range_tree.end() && rs_after->start < end)) {
    derr << __func__ << " inconsistent tree state " << std::hex
         << " 0x" << start << "~" << end - start
         << std::dec << dendl;
    _dump();
  }
  ceph_assert(rs_before == range_tree.end() || rs_before->end <= start);
  ceph_assert(rs_after == range_tree.end() || rs_after->start >= end);

  bool merge_before = (rs_before != range_tree.end() && rs_before->end == start);
  bool merge_after = (rs_after != range_tree.end() && rs_after->start == end);

  if (merge_before && merge_after) {
    _range_size_tree_rm(*rs_before);
    _range_size_tree_rm(*rs_after);
    rs_after->start = rs_before->start;
    range_tree.erase_and_dispose(rs_before, dispose_rs{});
    _range_size_tree_try_insert(*rs_after);
  } else if (merge_before) {
    _range_size_tree_rm(*rs_before);
    rs_before->end = end;
    _range_size_tree_try_insert(*rs_before);
  } else if (merge_after) {
    _range_size_tree_rm(*rs_after);
    rs_after->start = start;
    _range_size_tree_try_insert(*rs_after);
  } else {
    _try_insert_range(start, end, &rs_after);
  }
}

void AvlAllocator::_process_range_removal(uint64_t start, uint64_t end,
  AvlAllocator::range_tree_t::iterator& rs)
{
  bool left_over = (rs->start != start);
  bool right_over = (rs->end != end);

  _range_size_tree_rm(*rs);

  if (left_over && right_over) {
    auto old_right_end = rs->end;
    auto insert_pos = rs;
    ceph_assert(insert_pos != range_tree.end());
    ++insert_pos;
    rs->end = start;

    // Insert tail first to be sure insert_pos hasn't been disposed.
    // This woulnd't dispose rs though since it's out of range_size_tree.
    // Don't care about a small chance of 'not-the-best-choice-for-removal' case
    // which might happen if rs has the lowest size.
    _try_insert_range(end, old_right_end, &insert_pos);
    _range_size_tree_try_insert(*rs);

  } else if (left_over) {
    rs->end = start;
    _range_size_tree_try_insert(*rs);
  } else if (right_over) {
    rs->start = end;
    _range_size_tree_try_insert(*rs);
  } else {
    range_tree.erase_and_dispose(rs, dispose_rs{});
  }
}

void AvlAllocator::_remove_from_tree(uint64_t start, uint64_t size)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);
  ceph_assert(size <= num_free);

  auto rs = range_tree.find(range_t{start, end}, range_tree.key_comp());
  /* Make sure we completely overlap with someone */
  if (rs == range_tree.end() ||
      rs->start > start ||
      rs->end < end) {
    derr << __func__ << " inconsistent tree state " << std::hex
         << " 0x" << rs->start << "~" << rs->end - rs->start
         << std::dec << dendl;
    _dump();
  }
  ceph_assert(rs != range_tree.end());
  ceph_assert(rs->start <= start);
  ceph_assert(rs->end >= end);

  _process_range_removal(start, end, rs);
}

void AvlAllocator::_try_remove_from_tree(uint64_t start, uint64_t size,
  std::function<void(uint64_t, uint64_t, bool)> cb)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);

  auto rs = range_tree.find(range_t{ start, end },
    range_tree.key_comp());

  if (rs == range_tree.end() || rs->start >= end) {
    cb(start, size, false);
    return;
  }

  do {

    auto next_rs = rs;
    ++next_rs;

    if (start < rs->start) {
      cb(start, rs->start - start, false);
      start = rs->start;
    }
    auto range_end = std::min(rs->end, end);
    _process_range_removal(start, range_end, rs);
    cb(start, range_end - start, true);
    start = range_end;

    rs = next_rs;
  } while (rs != range_tree.end() && rs->start < end && start < end);
  if (start < end) {
    cb(start, end - start, false);
  }
}

int64_t AvlAllocator::_allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  uint64_t allocated = 0;
  while (allocated < want) {
    uint64_t offset, length;
    int r = _allocate(std::min(max_alloc_size, want - allocated),
      unit, &offset, &length);
    if (r < 0) {
      // Allocation failed.
      break;
    }
    extents->emplace_back(offset, length);
    allocated += length;
  }
  return allocated ? allocated : -ENOSPC;
}

int AvlAllocator::_allocate(
  uint64_t size,
  uint64_t unit,
  uint64_t *offset,
  uint64_t *length)
{
  uint64_t max_size = 0;
  if (auto p = range_size_tree.rbegin(); p != range_size_tree.rend()) {
    max_size = p->end - p->start;
  }

  bool force_range_size_alloc = false;
  if (max_size < size) {
    if (max_size < unit) {
      return -ENOSPC;
    }
    size = p2align(max_size, unit);
    ceph_assert(size > 0);
    force_range_size_alloc = true;
  }

  const int free_pct = num_free * 100 / device_size;
  uint64_t start = 0;
  // If we're running low on space, find a range by size by looking up in the size
  // sorted tree (best-fit), instead of searching in the area pointed by cursor
  if (force_range_size_alloc ||
      max_size < range_size_alloc_threshold ||
      free_pct < range_size_alloc_free_pct) {
    start = -1ULL;
  } else {
    /*
     * Find the largest power of 2 block size that evenly divides the
     * requested size. This is used to try to allocate blocks with similar
     * alignment from the same area (i.e. same cursor bucket) but it does
     * not guarantee that other allocations sizes may exist in the same
     * region.
     */
    uint64_t align = size & -size;
    ceph_assert(align != 0);
    uint64_t* cursor = &lbas[cbits(align) - 1];
    start = _pick_block_after(cursor, size, unit);
    dout(20) << __func__
             << std::hex << " first fit params: 0x" << start << "~" << size
             << std::dec << dendl;
  }
  if (start == -1ULL) {
    do {
      start = _pick_block_fits(size, unit);
      dout(20) << __func__
               << std::hex << " best fit params: 0x" << start << "~" << size
               << std::dec << dendl;
      if (start != uint64_t(-1ULL)) {
        break;
      }
      // try to collect smaller extents as we could fail to retrieve
      // that large block due to misaligned extents
      size = p2align(size >> 1, unit);
    } while (size >= unit);
  }
  if (start == -1ULL) {
    return -ENOSPC;
  }
  dout(20) << __func__ << " allocated 0x" << std::hex
           << start << "~" << size
           << std::dec << dendl;
  _remove_from_tree(start, size);

  *offset = start;
  *length = size;
  return 0;
}

void AvlAllocator::_release(const interval_set<uint64_t>& release_set)
{
  for (auto p = release_set.begin(); p != release_set.end(); ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ceph_assert(offset + length <= uint64_t(device_size));
    ldout(cct, 20) << __func__ << std::hex
      << " 0x" << offset
      << "~" << length
      << std::dec << dendl;
    _add_to_tree(offset, length);
  }
}

void AvlAllocator::_release(const PExtentVector& release_set) {
  for (auto& e : release_set) {
    ldout(cct, 20) << __func__ << std::hex
      << " 0x" << e.offset
      << "~" << e.length
      << std::dec << dendl;
    _add_to_tree(e.offset, e.length);
  }
}

void AvlAllocator::_shutdown()
{
  range_size_tree.clear();
  range_tree.clear_and_dispose(dispose_rs{});
}

AvlAllocator::AvlAllocator(CephContext* cct,
                           int64_t device_size,
                           int64_t block_size,
                           uint64_t max_mem,
                           std::string_view name) :
  Allocator(name, device_size, block_size),
  range_size_alloc_threshold(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_threshold")),
  range_size_alloc_free_pct(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_free_pct")),
  max_search_count(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_ff_max_search_count")),
  max_search_bytes(
    cct->_conf.get_val<Option::size_t>("bluestore_avl_alloc_ff_max_search_bytes")),
  range_count_cap(max_mem / sizeof(range_seg_t)),
  cct(cct)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << get_capacity() << "/"
                 << get_block_size() << std::dec << dendl;
}

AvlAllocator::AvlAllocator(CephContext* cct,
			   int64_t device_size,
			   int64_t block_size,
			   std::string_view name) :
  AvlAllocator(cct, device_size, block_size, 0 /* max_mem */, name)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << get_capacity() << "/"
                 << get_block_size() << std::dec << dendl;
}

AvlAllocator::~AvlAllocator()
{
  shutdown();
}

int64_t AvlAllocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  ldout(cct, 10) << __func__ << std::hex
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
  if (constexpr auto cap = std::numeric_limits<decltype(bluestore_pextent_t::length)>::max();
      max_alloc_size >= cap) {
    max_alloc_size = p2align(uint64_t(cap), (uint64_t)block_size);
  }
  std::lock_guard l(lock);
  return _allocate(want, unit, max_alloc_size, hint, extents);
}

void AvlAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
  _release(release_set);
}

uint64_t AvlAllocator::get_free()
{
  std::lock_guard l(lock);
  return num_free;
}

double AvlAllocator::get_fragmentation()
{
  std::lock_guard l(lock);
  return _get_fragmentation();
}

void AvlAllocator::dump()
{
  std::lock_guard l(lock);
  _dump();
}

void AvlAllocator::_dump() const
{
  ldout(cct, 0) << __func__ << " range_tree: " << dendl;
  for (auto& rs : range_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.start << "~" << rs.end
      << std::dec
      << dendl;
  }
  ldout(cct, 0) << __func__ << " range_size_tree: " << dendl;
  for (auto& rs : range_size_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.start << "~" << rs.end
      << std::dec
      << dendl;
  }
}

void AvlAllocator::foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  _foreach(notify);
}

void AvlAllocator::_foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify) const
{
  for (auto& rs : range_tree) {
    notify(rs.start, rs.end - rs.start);
  }
}

void AvlAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " 0x" << offset
                 << "~" << length
                 << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _add_to_tree(offset, length);
}

void AvlAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " 0x" << offset
                 << "~" << length
                 << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _remove_from_tree(offset, length);
}

void AvlAllocator::shutdown()
{
  std::lock_guard l(lock);
  _shutdown();
}
