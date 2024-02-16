// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "BtreeAllocator.h"

#include <bit>
#include <limits>

#include "common/config_proxy.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << "BtreeAllocator "

/*
 * This is a helper function that can be used by the allocator to find
 * a suitable block to allocate. This will search the specified B-tree
 * looking for a block that matches the specified criteria.
 */
uint64_t BtreeAllocator::_pick_block_after(uint64_t *cursor,
					   uint64_t size,
					   uint64_t align)
{
  auto rs_start = range_tree.lower_bound(*cursor);
  for (auto rs = rs_start; rs != range_tree.end(); ++rs) {
    uint64_t offset = rs->first;
    if (offset + size <= rs->second) {
      *cursor = offset + size;
      return offset;
    }
  }
  if (*cursor == 0) {
    // If we already started from beginning, don't bother with searching from beginning
    return -1ULL;
  }
  // If we reached end, start from beginning till cursor.
  for (auto rs = range_tree.begin(); rs != rs_start; ++rs) {
    uint64_t offset = rs->first;
    if (offset + size <= rs->second) {
      *cursor = offset + size;
      return offset;
    }
  }
  return -1ULL;
}

uint64_t BtreeAllocator::_pick_block_fits(uint64_t size,
                                        uint64_t align)
{
  // instead of searching from cursor, just pick the smallest range which fits
  // the needs
  auto rs_start = range_size_tree.lower_bound(range_value_t{0,size});
  for (auto rs = rs_start; rs != range_size_tree.end(); ++rs) {
    uint64_t offset = rs->start;
    if (offset + size <= rs->start + rs->size) {
      return offset;
    }
  }
  return -1ULL;
}

void BtreeAllocator::_add_to_tree(uint64_t start, uint64_t size)
{
  ceph_assert(size != 0);

  uint64_t end = start + size;

  auto rs_after = range_tree.upper_bound(start);

  /* Make sure we don't overlap with either of our neighbors */
  auto rs_before = range_tree.end();
  if (rs_after != range_tree.begin()) {
    rs_before = std::prev(rs_after);
  }

  bool merge_before = (rs_before != range_tree.end() && rs_before->second == start);
  bool merge_after = (rs_after != range_tree.end() && rs_after->first == end);

  if (merge_before && merge_after) {
    // | before   |//////| after |
    // | before >>>>>>>>>>>>>>>  |
    range_seg_t seg_before{rs_before->first, rs_before->second};
    range_seg_t seg_after{rs_after->first, rs_after->second};
    // expand the head seg before rs_{before,after} are invalidated
    rs_before->second = seg_after.end;
    // remove the tail seg from offset tree
    range_tree.erase(rs_after);
    // remove the head and tail seg from size tree
    range_size_tree.erase(seg_before);
    range_size_tree.erase(seg_after);
    // insert the merged seg into size tree
    range_size_tree.emplace(seg_before.start, seg_after.end);
  } else if (merge_before) {
    // | before   |//////|
    // | before >>>>>>>> |
    // remove the head seg from the size tree
    range_seg_t seg_before{rs_before->first, rs_before->second};
    range_size_tree.erase(seg_before);
    // expand the head seg in the offset tree
    rs_before->second = end;
    // insert the merged seg into size tree
    range_size_tree.emplace(seg_before.start, end);
  } else if (merge_after) {
    // |//////| after |
    // | merge after  |
    // remove the tail seg from size tree
    range_seg_t seg_after{rs_after->first, rs_after->second};
    range_size_tree.erase(seg_after);
    // remove the tail seg from offset tree
    range_tree.erase(rs_after);
    // insert the merged seg
    range_tree.emplace(start, seg_after.end);
    range_size_tree.emplace(start, seg_after.end);
  } else {
    // no neighbours
    range_tree.emplace_hint(rs_after, start, end);
    range_size_tree.emplace(start, end);
  }
  num_free += size;
}

void BtreeAllocator::_process_range_removal(uint64_t start, uint64_t end,
  BtreeAllocator::range_tree_t::iterator& rs)
{
  bool left_over = (rs->first != start);
  bool right_over = (rs->second != end);

  range_seg_t seg_whole{rs->first, rs->second};
  range_size_tree.erase(seg_whole);

  // | left <|////|  right |
  if (left_over && right_over) {
    // shink the left seg in offset tree
    // this should be done before calling any emplace/emplace_hint
    // on range_tree since they invalidate rs iterator.
    rs->second = start;
    // insert the shrinked left seg back into size tree
    range_size_tree.emplace(seg_whole.start, start);

    // add the spin-off right seg
    range_seg_t seg_after{end, seg_whole.end};
    range_tree.emplace_hint(rs, seg_after.start, seg_after.end);
    range_size_tree.emplace(seg_after);
  } else if (left_over) {
    // | left <|///////////|
    // shrink the left seg in the offset tree
    rs->second = start;
    // insert the shrinked left seg back into size tree
    range_size_tree.emplace(seg_whole.start, start);
  } else if (right_over) {
    // |//////////| right |
    // remove the whole seg from offset tree
    range_tree.erase(rs);
    // add the spin-off right seg
    range_seg_t seg_after{end, seg_whole.end};
    range_tree.emplace(seg_after.start, seg_after.end);
    range_size_tree.emplace(seg_after);
  } else {
    range_tree.erase(rs);
  }
  num_free -= (end - start);
}

void BtreeAllocator::_remove_from_tree(uint64_t start, uint64_t size)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);
  ceph_assert(size <= num_free);

  // Make sure we completely overlap with someone
  auto rs = range_tree.lower_bound(start);
  if ((rs == range_tree.end() || rs->first > start) && rs != range_tree.begin()) {
    --rs;
  }
  ceph_assert(rs != range_tree.end());
  ceph_assert(rs->first <= start);
  ceph_assert(rs->second >= end);

  _process_range_removal(start, end, rs);
}

void BtreeAllocator::_try_remove_from_tree(uint64_t start, uint64_t size,
  std::function<void(uint64_t, uint64_t, bool)> cb)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);

  auto rs = range_tree.find(start);

  if (rs == range_tree.end() || rs->first >= end) {
    cb(start, size, false);
    return;
  }

  do {

    //FIXME: this is apparently wrong since _process_range_removal might
    // invalidate existing iterators.
    // Not a big deal so far since this method is not in use - it's called
    // when making Hybrid allocator from a regular one. Which isn't an option
    // for BtreeAllocator for now.
    auto next_rs = rs;
    ++next_rs;

    if (start < rs->first) {
      cb(start, rs->first - start, false);
      start = rs->first;
    }
    auto range_end = std::min(rs->second, end);
    _process_range_removal(start, range_end, rs);
    cb(start, range_end - start, true);
    start = range_end;

    rs = next_rs;
  } while (rs != range_tree.end() && rs->first < end && start < end);
  if (start < end) {
    cb(start, end - start, false);
  }
}

int64_t BtreeAllocator::_allocate(
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
  assert(range_size_tree.size() == range_tree.size());
  return allocated ? allocated : -ENOSPC;
}

int BtreeAllocator::_allocate(
  uint64_t size,
  uint64_t unit,
  uint64_t *offset,
  uint64_t *length)
{
  uint64_t max_size = 0;
  if (auto p = range_size_tree.rbegin(); p != range_size_tree.rend()) {
    max_size = p->size;
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
  /*
   * If we're running low on space switch to using the size
   * sorted B-tree (best-fit).
   */
  if (force_range_size_alloc ||
      max_size < range_size_alloc_threshold ||
      free_pct < range_size_alloc_free_pct) {
    do {
      start = _pick_block_fits(size, unit);
      dout(20) << __func__ << " best fit=" << start << " size=" << size << dendl;
      if (start != uint64_t(-1ULL)) {
        break;
      }
      // try to collect smaller extents as we could fail to retrieve
      // that large block due to misaligned extents
      size = p2align(size >> 1, unit);
    } while (size >= unit);
  } else {
    do {
      /*
       * Find the largest power of 2 block size that evenly divides the
       * requested size. This is used to try to allocate blocks with similar
       * alignment from the same area (i.e. same cursor bucket) but it does
       * not guarantee that other allocations sizes may exist in the same
       * region.
       */
      uint64_t* cursor = &lbas[cbits(size) - 1];
      start = _pick_block_after(cursor, size, unit);
      dout(20) << __func__ << " first fit=" << start << " size=" << size << dendl;
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

  _remove_from_tree(start, size);

  *offset = start;
  *length = size;
  return 0;
}

void BtreeAllocator::_release(const interval_set<uint64_t>& release_set)
{
  for (auto p = release_set.begin(); p != release_set.end(); ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ceph_assert(offset + length <= uint64_t(device_size));
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << offset
      << " length 0x" << length
      << std::dec << dendl;
    _add_to_tree(offset, length);
  }
}

void BtreeAllocator::_release(const PExtentVector& release_set) {
  for (auto& e : release_set) {
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << e.offset
      << " length 0x" << e.length
      << std::dec << dendl;
    _add_to_tree(e.offset, e.length);
  }
}

void BtreeAllocator::_shutdown()
{
  range_size_tree.clear();
  range_tree.clear();
}

BtreeAllocator::BtreeAllocator(CephContext* cct,
			       int64_t device_size,
			       int64_t block_size,
			       uint64_t max_mem,
			       std::string_view name) :
  Allocator(name, device_size, block_size),
  range_size_alloc_threshold(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_threshold")),
  range_size_alloc_free_pct(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_free_pct")),
  range_count_cap(max_mem / sizeof(range_seg_t)),
  cct(cct)
{}

BtreeAllocator::BtreeAllocator(CephContext* cct,
			       int64_t device_size,
			       int64_t block_size,
			       std::string_view name) :
  BtreeAllocator(cct, device_size, block_size, 0 /* max_mem */, name)
{}

BtreeAllocator::~BtreeAllocator()
{
  shutdown();
}

int64_t BtreeAllocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " want 0x" << want
                 << " unit 0x" << unit
                 << " max_alloc_size 0x" << max_alloc_size
                 << " hint 0x" << hint
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

void BtreeAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
  _release(release_set);
}

uint64_t BtreeAllocator::get_free()
{
  std::lock_guard l(lock);
  return num_free;
}

double BtreeAllocator::get_fragmentation()
{
  std::lock_guard l(lock);
  return _get_fragmentation();
}

void BtreeAllocator::dump()
{
  std::lock_guard l(lock);
  _dump();
}

void BtreeAllocator::_dump() const
{
  ldout(cct, 0) << __func__ << " range_tree: " << dendl;
  for (auto& rs : range_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.first << "~" << rs.second
      << std::dec
      << dendl;
  }

  ldout(cct, 0) << __func__ << " range_size_tree: " << dendl;
  for (auto& rs : range_size_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.size << "@" << rs.start
      << std::dec
      << dendl;
  }
}

void BtreeAllocator::foreach(std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  for (auto& rs : range_tree) {
    notify(rs.first, rs.second - rs.first);
  }
}

void BtreeAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  _add_to_tree(offset, length);
}

void BtreeAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  _remove_from_tree(offset, length);
}

void BtreeAllocator::shutdown()
{
  std::lock_guard l(lock);
  _shutdown();
}
