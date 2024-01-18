// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//
#include "avlallocator.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

void AvlAllocator::mark_extent_used(rbm_abs_addr addr, size_t size) 
{
  LOG_PREFIX(AvlAllocator::mark_extent_used);
  DEBUG("addr: {}, size: {}, avail: {}", addr, size, available_size);
  _remove_from_tree(addr, size);
}

void AvlAllocator::init(rbm_abs_addr addr, size_t size, size_t b_size) 
{
  LOG_PREFIX(AvlAllocator::init);
  DEBUG("addr: {}, size: {}", addr, size);
  auto r = new extent_range_t{ addr, addr + size };
  extent_tree.insert(*r);
  extent_size_tree.insert(*r);
  available_size = size;
  block_size = b_size;
  total_size = size;
  base_addr = addr;
}

void AvlAllocator::_remove_from_tree(rbm_abs_addr start, rbm_abs_addr size)
{
  LOG_PREFIX(AvlAllocator::_remove_from_tree);
  rbm_abs_addr end = start + size;

  ceph_assert(size != 0);
  ceph_assert(size <= available_size);

  auto rs = extent_tree.find(extent_range_t{start, end}, extent_tree.key_comp());
  DEBUG("rs start: {}, rs end: {}", rs->start, rs->end);
  ceph_assert(rs != extent_tree.end());
  ceph_assert(rs->start <= start);
  ceph_assert(rs->end >= end);

  bool left_over = (rs->start != start);
  bool right_over = (rs->end != end);

  _extent_size_tree_rm(*rs);

  if (left_over && right_over) {
    auto old_right_end = rs->end;
    auto insert_pos = rs;
    ceph_assert(insert_pos != extent_tree.end());
    ++insert_pos;
    rs->end = start;

    auto r = new extent_range_t{end, old_right_end};
    extent_tree.insert_before(insert_pos, *r);
    extent_size_tree.insert(*r);
    available_size += r->length();
    _extent_size_tree_try_insert(*rs);
  } else if (left_over) {
    assert(is_aligned(start, block_size));
    rs->end = start;
    _extent_size_tree_try_insert(*rs);
  } else if (right_over) {
    assert(is_aligned(end, block_size));
    rs->start = end;
    _extent_size_tree_try_insert(*rs);
  } else {
    extent_tree.erase_and_dispose(rs, dispose_rs{});
  }
}

rbm_abs_addr AvlAllocator::find_block(size_t size)
{
  const auto comp = extent_size_tree.key_comp();
  auto iter = extent_size_tree.lower_bound(
    extent_range_t{base_addr, base_addr + size}, comp);
  for (; iter != extent_size_tree.end(); ++iter) {
    assert(is_aligned(iter->start, block_size));
    rbm_abs_addr off = iter->start;
    if (off + size <= iter->end) {
      return off;
    } 
  }
  return total_size;
}

extent_len_t AvlAllocator::find_block(
  size_t size,
  rbm_abs_addr &start)
{
  uint64_t max_size = 0;
  auto p = extent_size_tree.rbegin();
  if (p != extent_size_tree.rend()) {
    max_size = p->end - p->start;
  }

  assert(max_size);
  if (max_size <= size) {
    start = p->start;
    return max_size;
  }

  const auto comp = extent_size_tree.key_comp();
  auto iter = extent_size_tree.lower_bound(
    extent_range_t{base_addr, base_addr + size}, comp);
  ceph_assert(iter != extent_size_tree.end());
  ceph_assert(is_aligned(iter->start, block_size));
  ceph_assert(size <= iter->length());
  start = iter->start;
  return size;
}


void AvlAllocator::_add_to_tree(rbm_abs_addr start, rbm_abs_addr size)
{
  LOG_PREFIX(AvlAllocator::_add_to_tree);
  ceph_assert(size != 0);
  DEBUG("addr: {}, size: {}", start, size);

  rbm_abs_addr end = start + size;

  auto rs_after = extent_tree.upper_bound(extent_range_t{start, end},
					 extent_tree.key_comp());

  auto rs_before = extent_tree.end();
  if (rs_after != extent_tree.begin()) {
    rs_before = std::prev(rs_after);
  }

  bool merge_before = (rs_before != extent_tree.end() && rs_before->end == start);
  bool merge_after = (rs_after != extent_tree.end() && rs_after->start == end);

  if (merge_before && merge_after) {
    _extent_size_tree_rm(*rs_before);
    _extent_size_tree_rm(*rs_after);
    rs_after->start = rs_before->start;
    extent_tree.erase_and_dispose(rs_before, dispose_rs{});
    _extent_size_tree_try_insert(*rs_after);
  } else if (merge_before) {
    _extent_size_tree_rm(*rs_before);
    rs_before->end = end;
    _extent_size_tree_try_insert(*rs_before);
  } else if (merge_after) {
    _extent_size_tree_rm(*rs_after);
    rs_after->start = start;
    _extent_size_tree_try_insert(*rs_after);
  } else {
    auto r = new extent_range_t{start, end};
    extent_tree.insert(*r);
    extent_size_tree.insert(*r);
    available_size += r->length();
  }
}

std::optional<interval_set<rbm_abs_addr>> AvlAllocator::alloc_extent(
  size_t size)
{
  LOG_PREFIX(AvlAllocator::alloc_extent);
  if (available_size < size) {
    return std::nullopt;
  }
  if (extent_size_tree.empty()) {
    return std::nullopt;
  }
  ceph_assert(size > 0);
  ceph_assert(is_aligned(size, block_size));

  interval_set<rbm_abs_addr> result;

  auto try_to_alloc_block = [this, &result, FNAME] (uint64_t alloc_size) -> uint64_t
  {
    rbm_abs_addr start = find_block(alloc_size);
    if (start != base_addr + total_size) {
      _remove_from_tree(start, alloc_size);
      DEBUG("allocate addr: {}, allocate size: {}, available size: {}",
	start, alloc_size, available_size);
      result.insert(start, alloc_size);
      return alloc_size;
    }
    return 0;
  };
  
  auto alloc = std::min(max_alloc_size, size);
  rbm_abs_addr ret = try_to_alloc_block(alloc);
  if (ret == 0) {
    return std::nullopt;
  }

  assert(!result.empty());
  assert(result.num_intervals() == 1);
  for (auto p : result) {
    INFO("result start: {}, end: {}", p.first, p.first + p.second);
    if (detailed) {
      assert(!reserved_extent_tracker.contains(p.first, p.second));
      reserved_extent_tracker.insert(p.first, p.second);
    }
  }
  return result;
}

std::optional<interval_set<rbm_abs_addr>> AvlAllocator::alloc_extents(
  size_t size)
{
  LOG_PREFIX(AvlAllocator::alloc_extents);
  if (available_size < size) {
    return std::nullopt;
  }
  if (extent_size_tree.empty()) {
    return std::nullopt;
  }
  ceph_assert(size > 0);
  ceph_assert(is_aligned(size, block_size));

  interval_set<rbm_abs_addr> result;

  auto try_to_alloc_block = [this, &result, FNAME] (uint64_t alloc_size)
  {
    while (alloc_size) {
      rbm_abs_addr start = 0;
      extent_len_t len = find_block(alloc_size, start);
      ceph_assert(len);
      _remove_from_tree(start, len);
      DEBUG("allocate addr: {}, allocate size: {}, available size: {}",
	start, len, available_size);
      result.insert(start, len);
      alloc_size -= len;
    }
    return 0;
  };
  
  auto alloc = std::min(max_alloc_size, size);
  try_to_alloc_block(alloc);

  assert(!result.empty());
  for (auto p : result) {
    INFO("result start: {}, end: {}", p.first, p.first + p.second);
    if (detailed) {
      assert(!reserved_extent_tracker.contains(p.first, p.second));
      reserved_extent_tracker.insert(p.first, p.second);
    }
  }
  return result;
}

void AvlAllocator::free_extent(rbm_abs_addr addr, size_t size)
{
  assert(total_size);
  assert(total_size > available_size);
  _add_to_tree(addr, size);
  if (detailed && reserved_extent_tracker.contains(addr, size)) {
    reserved_extent_tracker.erase(addr, size);
  }
}

bool AvlAllocator::is_free_extent(rbm_abs_addr start, size_t size)
{
  rbm_abs_addr end = start + size;
  ceph_assert(size != 0);
  if (start < base_addr || base_addr + total_size < end) {
    return false;
  }

  auto rs = extent_tree.find(extent_range_t{start, end}, extent_tree.key_comp());
  if (rs != extent_tree.end() && rs->start <= start && rs->end >= end) {
    return true;
  }
  return false;
}
}
