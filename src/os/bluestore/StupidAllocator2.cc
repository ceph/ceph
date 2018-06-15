// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator2.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc 0x" << this << " "

StupidAllocator2::StupidAllocator2(CephContext* cct)
  : cct(cct), num_free(0),
    bins(bins_count),
    last_alloc(0)
{
  static_assert ((bins_count % 8) == 1);
  static_assert (bins_count > 0);
}

StupidAllocator2::~StupidAllocator2()
{
}

size_t StupidAllocator2::_choose_bin(uint64_t orig_len)
{
  uint64_t len = orig_len / cct->_conf->bdev_block_size;
  int p2 = cbits(len);
  uint32_t bin;
  uint32_t rest = (orig_len * 16 >> p2) / cct->_conf->bdev_block_size - 8;
  bin = p2 * 8 + rest;
  if (bin >= bins_count)
    bin = bins_count - 1;
  assert(bin < bins_count);
  ldout(cct, 30) << __func__ << " len 0x" << std::hex << orig_len
      << std::dec << " -> " << bin << dendl;
  return bin;
}

void StupidAllocator2::_insert_free(uint64_t off, uint64_t len)
{
  ldout(cct, 30) << __func__ << std::hex << off << ":" << len
                   << std::dec << dendl;
  // new region can merge with prev and next regions, if connects
  if (all.size() == 0) {
    //initial insert
    unsigned bin = _choose_bin(len);
    auto it_all = all.emplace(off, region_map_t::iterator{});
    assert(it_all.second);
    auto it_bin = bins[bin].emplace(off, region{len, it_all.first});
    it_all.first->second = it_bin.first;
    return;
  }

  auto it_next = lower_bound(all, off);
  auto it_prev = it_next;
  it_prev--; //this is always valid, because we have at least one element in map.
  if (it_next != all.end()) {
    assert(off + len <= it_next->first); //newly added free region may not intersect with next free region
    if (off + len == it_next->first) {
      //touches next region - connect
      ldout(cct, 30) << __func__ << ":" << __LINE__ << dendl;
      auto it_next_bin = it_next->second;
      unsigned next_bin = _choose_bin(it_next_bin->second.length);
      assert(it_next->first == it_next_bin->first);
      len += it_next_bin->second.length;
      bins[next_bin].erase(it_next_bin);
      it_next = all.erase(it_next);
      ldout(cct, 30) << __func__ << ":" << __LINE__ << "   len=" << std::hex << len << std::dec << dendl;
    }
  }
  if ((it_prev != all.end()) &&
      (it_prev->first + it_prev->second->second.length == off)) {
    //previous region connects with current area
    ldout(cct, 30) << __func__ << ":" << __LINE__ << dendl;
    auto it_prev_bin = it_prev->second;
    len += it_prev_bin->second.length;
    off -= it_prev_bin->second.length;
    assert(it_prev->first == off);
    unsigned prev_bin = _choose_bin(it_prev_bin->second.length);
    unsigned new_bin = _choose_bin(len);
    if (prev_bin == new_bin) {
      //we can reuse existing regions
      it_prev_bin->second.length = len; //fix region length
    } else {
      auto new_bin_it = bins[new_bin].emplace(off, region{len, it_prev});
      bins[prev_bin].erase(it_prev_bin);
      assert(new_bin_it.second);
      it_prev->second = new_bin_it.first;
      assert(it_prev->first == off);
    }
  } else {
    //previous region not connected to current area
    ldout(cct, 30) << __func__ << ":" << __LINE__ << dendl;
    unsigned new_bin = _choose_bin(len);
    auto new_all_it = all.emplace(off, region_map_t::iterator{});
    assert(new_all_it.second);
    auto new_bin_it = bins[new_bin].emplace(off, region{len, new_all_it.first});
    assert(new_bin_it.second);
    new_all_it.first->second = new_bin_it.first;
  }
}

void StupidAllocator2::_remove_free(uint64_t offset, uint64_t length)
{
  auto all_it = lower_bound(all, offset);
  assert(all_it != all.end());
  auto bin_it = all_it->second;
  uint64_t len = bin_it->second.length;
  unsigned bin = _choose_bin(len);
  all_it++; //change iterator while it is still valid
  len = std::min(len, length);
  remove(bin_it, bin, offset, len);
  num_free -= len;
  offset += len;
  length -= len;
  while (length > 0) {
    assert(all_it != all.end());
    bin_it = all_it->second;
    assert(bin_it->first == offset);
    len = bin_it->second.length;
    bin = _choose_bin(bin_it->second.length);
    len = std::min(len, length);
    all_it++;
    remove(bin_it, bin, offset, len);
    num_free -= len;
    offset += len;
    length -= len;
  }
  assert(length == 0);
}

/*
 * If point offset belongs to some region, return it.
 * It point offset does not belong to any region return first region after it.
 * Obviously, if offset is higher then any region return end().
 */
StupidAllocator2::region_map_t::iterator
StupidAllocator2::lower_bound(region_map_t& map, uint64_t offset)
{
  //find equal of higher ->first
  auto it = map.lower_bound(offset);
  if (it != map.end()) {
    if (it->first == offset) {
      return it;
    }
  }
  //definately not exact match
  if (it != map.begin()) {
    //check if previous region contains offset
    auto ita = it;
    ita--;
    assert(ita->first < offset);
    if (ita->first + ita->second.length > offset) {
      return ita;
    }
  }
  //not exact and not in previous
  //offset is between regions
  return it;
}

/*
 * If point offset belongs to some region, return it.
 * It point offset does not belong to any region return first region after it.
 * Obviously, if offset is higher then any region return end().
 */
StupidAllocator2::free_map_t::iterator
StupidAllocator2::lower_bound(free_map_t& map, uint64_t offset)
{
  //find equal of higher ->first
  auto it = map.lower_bound(offset);
  if (it != map.end()) {
    if (it->first == offset) {
      return it;
    }
  }
  //definately not exact match
  if (it != map.begin()) {
    //check if previous region contains offset
    auto ita = it;
    ita--;
    assert(ita->first < offset);
    if (ita->first + ita->second->second.length > offset) {
      return ita;
    }
  }
  //not exact and not in previous
  //offset is between regions
  return it;
}

void
StupidAllocator2::remove(region_map_t::iterator& reg, unsigned bin, uint64_t offset, uint64_t length)
{
  ldout(cct, 30) << __func__ << std::hex << offset << ":" << length
                   << std::dec << dendl;
  assert(reg->first <= offset);
  assert(reg->first + reg->second.length >= offset + length);

  if (offset == reg->first) {
    if (length == reg->second.length) {
      // exact match
      all.erase(reg->second.to_all);
      bins[bin].erase(reg);
    } else {
      // some rest remains
      uint64_t rest_offset = offset + length;
      uint64_t rest_length = reg->second.length - length;
      unsigned rest_bin = _choose_bin(rest_length);
      auto rest_it = bins[rest_bin].emplace(rest_offset, region{rest_length, reg->second.to_all});
      assert(rest_it.second);
      auto all_it = all.emplace_hint(reg->second.to_all, rest_offset, rest_it.first);
      rest_it.first->second.to_all = all_it;
      all.erase(reg->second.to_all);
      bins[bin].erase(reg);
    }
  } else {
    // some area in front of region remains
    if (offset + length == reg->first + reg->second.length) {
      // no area in rest of region
      uint64_t front_length = offset - reg->first;
      unsigned front_bin = _choose_bin(front_length);
      if (front_bin == bin) {
        //remainder size not degraded dramatically
        reg->second.length = front_length;
      } else {
        //bin changed
        auto front_it = bins[front_bin].emplace(reg->first, region{front_length, reg->second.to_all});
        assert(front_it.second);
        reg->second.to_all->second = front_it.first;
        bins[bin].erase(reg);
      }
    } else {
      //areas both in front and rest of region
      uint64_t front_offset = reg->first;
      uint64_t front_length = offset - reg->first;
      uint64_t rest_offset = offset + length;
      uint64_t rest_length = reg->second.length - front_length - length;
      unsigned front_bin = _choose_bin(front_length);
      unsigned rest_bin = _choose_bin(rest_length);
      auto after = all.erase(reg->second.to_all);
      bins[bin].erase(reg);

      auto front_it = bins[front_bin].emplace(front_offset, region{front_length});
      assert(front_it.second);
      auto all_it = all.emplace_hint(after, front_offset, front_it.first);
      front_it.first->second.to_all = all_it;

      auto rest_it = bins[rest_bin].emplace(rest_offset, region{rest_length});
      assert(rest_it.second);
      all_it = all.emplace_hint(after, rest_offset, rest_it.first);
      rest_it.first->second.to_all = all_it;
    }
  }
}

int64_t StupidAllocator2::allocate_int(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " want_size 0x" << std::hex << want_size
	   	 << " alloc_unit 0x" << alloc_unit
	   	 << " hint 0x" << hint << std::dec
	   	 << dendl;
  uint64_t want = std::max(alloc_unit, want_size);
  int bin = _choose_bin(want);
  int orig_bin = bin;

  auto p = bins[0].begin();
  assert(isp2(alloc_unit));
  ceph::math::p2_t<uint64_t> alloc_unit_p2(alloc_unit);

  if (!hint)
    hint = last_alloc;

  // search up (from hint)
  if (hint) {
    for (bin = orig_bin; bin < (int)bins.size(); ++bin) {
      p = /*free[bin].*/lower_bound(bins[bin], hint);
      while (p != bins[bin].end()) {
	if (_aligned_len(p, alloc_unit_p2) >= want_size) {
	  goto found;
	}
	++p;
      }
    }
  }

  // search up (from origin, and skip searched extents by hint)
  for (bin = orig_bin; bin < (int)bins.size(); ++bin) {
    p = bins[bin].begin();
    auto end = hint ? lower_bound(bins[bin], hint) : bins[bin].end();
    while (p != end) {
      if (_aligned_len(p, alloc_unit_p2) >= want_size) {
	goto found;
      }
      ++p;
    }
  }

  // search down (hint)
  if (hint) {
    for (bin = orig_bin; bin >= 0; --bin) {
      p = lower_bound(bins[bin], hint);
      while (p != bins[bin].end()) {
	if (_aligned_len(p, alloc_unit_p2) >= alloc_unit) {
	  goto found;
	}
	++p;
      }
    }
  }

  // search down (from origin, and skip searched extents by hint)
  for (bin = orig_bin; bin >= 0; --bin) {
    p = bins[bin].begin();
    auto end = hint ? lower_bound(bins[bin], hint) : bins[bin].end();
    while (p != end) {
      if (_aligned_len(p, alloc_unit_p2) >= alloc_unit) {
	goto found;
      }
      ++p;
    }
  }

  return -ENOSPC;

  found:
  uint64_t off = p->first;
  uint64_t len = want;
  uint64_t unalign = p->first % alloc_unit;
  if (unalign != 0) {
    off = off + (alloc_unit - unalign);
  }
  len = std::min(p->first + p->second.length - off, want);
  remove(p, bin, off, len);
  *length = len;
  *offset = off;

  num_free -= len;
  assert(num_free >= 0);
  last_alloc = off + len;
  return 0;
}

int64_t StupidAllocator2::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  PExtentVector *extents)
{
  uint64_t allocated_size = 0;
  uint64_t offset = 0;
  uint32_t length = 0;
  int res = 0;

  if (max_alloc_size == 0) {
    max_alloc_size = want_size;
  }

  while (allocated_size < want_size) {
    res = allocate_int(std::min(max_alloc_size, (want_size - allocated_size)),
       alloc_unit, hint, &offset, &length);
    if (res != 0) {
      /*
       * Allocation failed.
       */
      break;
    }
    bool can_append = true;
    if (!extents->empty()) {
      bluestore_pextent_t &last_extent  = extents->back();
      if ((last_extent.end() == offset) &&
	  ((last_extent.length + length) <= max_alloc_size)) {
	can_append = false;
	last_extent.length += length;
      }
    }
    if (can_append) {
      extents->emplace_back(bluestore_pextent_t(offset, length));
    }

    allocated_size += length;
    hint = offset + length;
  }

  if (allocated_size == 0) {
    return -ENOSPC;
  }
  return allocated_size;
}

void StupidAllocator2::release(
  const interval_set<uint64_t>& release_set)
{
  std::lock_guard<std::mutex> l(lock);
  for (interval_set<uint64_t>::const_iterator p = release_set.begin();
       p != release_set.end();
       ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		   << std::dec << dendl;
    _insert_free(offset, length);
    num_free += length;
  }
}

uint64_t StupidAllocator2::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void StupidAllocator2::dump()
{
  std::lock_guard<std::mutex> l(lock);
  for (unsigned bin = 0; bin < bins.size(); ++bin) {
    ldout(cct, 0) << __func__ << " free bin " << bin << ": "
	    	  << bins[bin].size() << " extents" << dendl;
    for (auto p = bins[bin].begin();
	 p != bins[bin].end();
	 ++p) {
      ldout(cct, 0) << __func__ << "  0x" << std::hex << p->first << "~"
	      	    << p->second.length << std::dec << dendl;
      assert(p->second.to_all->first == p->first);
    }
  }
  ldout(cct, 0) << __func__ << " all "
                << all.size() << " extents" << dendl;
  for (auto p = all.begin();
       p != all.end();
       ++p) {
    ldout(cct, 0) << __func__ << "  0x" << std::hex << p->first << "~"
                  << p->second->second.length << std::dec << dendl;
    assert(p->second->first == p->first);
  }

}

void StupidAllocator2::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

void StupidAllocator2::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
      << std::dec << dendl;

  _remove_free(offset, length);
  assert(num_free >= 0);
}


void StupidAllocator2::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
}

