// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc 0x" << this << " "

StupidAllocator::StupidAllocator(CephContext* cct,
                                 int64_t capacity,
                                 int64_t _block_size,
                                 std::string_view name)
  : Allocator(name, capacity, _block_size),
    cct(cct), num_free(0),
    free(10)
{
  ceph_assert(cct != nullptr);
  ceph_assert(block_size > 0);
}

StupidAllocator::~StupidAllocator()
{
}

unsigned StupidAllocator::_choose_bin(uint64_t orig_len)
{
  uint64_t len = orig_len / block_size;
  int bin = std::min((int)cbits(len), (int)free.size() - 1);
  ldout(cct, 30) << __func__ << " len 0x" << std::hex << orig_len
		 << std::dec << " -> " << bin << dendl;
  return bin;
}

void StupidAllocator::_insert_free(uint64_t off, uint64_t len)
{
  unsigned bin = _choose_bin(len);
  ldout(cct, 30) << __func__ << " 0x" << std::hex << off << "~" << len
		 << std::dec << " in bin " << bin << dendl;
  while (true) {
    free[bin].insert(off, len, &off, &len);
    unsigned newbin = _choose_bin(len);
    if (newbin == bin)
      break;
    ldout(cct, 30) << __func__ << " promoting 0x" << std::hex << off << "~" << len
		   << std::dec << " to bin " << newbin << dendl;
    free[bin].erase(off, len);
    bin = newbin;
  }
}

int64_t StupidAllocator::allocate_int(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " want_size 0x" << std::hex << want_size
	   	 << " alloc_unit 0x" << alloc_unit
	   	 << " hint 0x" << hint << std::dec
	   	 << dendl;
  uint64_t want = std::max(alloc_unit, want_size);
  int bin = _choose_bin(want);
  int orig_bin = bin;

  auto p = free[0].begin();

  if (!hint)
    hint = last_alloc;

  // search up (from hint)
  if (hint) {
    for (bin = orig_bin; bin < (int)free.size(); ++bin) {
      p = free[bin].lower_bound(hint);
      while (p != free[bin].end()) {
	if (p.get_len() >= want_size) {
	  goto found;
	}
	++p;
      }
    }
  }

  // search up (from origin, and skip searched extents by hint)
  for (bin = orig_bin; bin < (int)free.size(); ++bin) {
    p = free[bin].begin();
    auto end = hint ? free[bin].lower_bound(hint) : free[bin].end();
    while (p != end) {
      if (p.get_len() >= want_size) {
	goto found;
      }
      ++p;
    }
  }

  // search down (hint)
  if (hint) {
    for (bin = orig_bin; bin >= 0; --bin) {
      p = free[bin].lower_bound(hint);
      while (p != free[bin].end()) {
	if (p.get_len() >= alloc_unit) {
	  goto found;
	}
	++p;
      }
    }
  }

  // search down (from origin, and skip searched extents by hint)
  for (bin = orig_bin; bin >= 0; --bin) {
    p = free[bin].begin();
    auto end = hint ? free[bin].lower_bound(hint) : free[bin].end();
    while (p != end) {
      if (p.get_len() >= alloc_unit) {
	goto found;
      }
      ++p;
    }
  }

  return -ENOSPC;

 found:
  *offset = p.get_start();
  *length = std::min(std::max(alloc_unit, want_size), p2align(p.get_len(), alloc_unit));

  if (cct->_conf->bluestore_debug_small_allocations) {
    uint64_t max =
      alloc_unit * (rand() % cct->_conf->bluestore_debug_small_allocations);
    if (max && *length > max) {
      ldout(cct, 10) << __func__ << " shortening allocation of 0x" << std::hex
	       	     << *length << " -> 0x"
	       	     << max << " due to debug_small_allocations" << std::dec
		     << dendl;
      *length = max;
    }
  }
  ldout(cct, 30) << __func__ << " got 0x" << std::hex << *offset << "~" << *length
	   	 << " from bin " << std::dec << bin << dendl;

  free[bin].erase(*offset, *length);
  uint64_t off, len;
  if (*offset && free[bin].contains(*offset - 1, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      ldout(cct, 30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       	     << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }
  if (free[bin].contains(*offset + *length, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      ldout(cct, 30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       	     << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }

  num_free -= *length;
  ceph_assert(num_free >= 0);
  last_alloc = *offset + *length;
  return 0;
}

int64_t StupidAllocator::allocate(
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
  // cap with 32-bit val
  max_alloc_size = std::min(max_alloc_size, 0x10000000 - alloc_unit);

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
      if (last_extent.end() == offset) {
        uint64_t l64 = last_extent.length;
        l64 += length;
        if (l64 < 0x100000000 && l64 <= max_alloc_size) {
	  can_append = false;
	  last_extent.length += length;
        }
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

void StupidAllocator::release(
  const interval_set<uint64_t>& release_set)
{
  std::lock_guard l(lock);
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

uint64_t StupidAllocator::get_free()
{
  std::lock_guard l(lock);
  return num_free;
}

double StupidAllocator::get_fragmentation()
{
  ceph_assert(get_block_size());
  double res;
  uint64_t max_intervals = 0;
  uint64_t intervals = 0;
  {
    std::lock_guard l(lock);
    max_intervals = p2roundup<uint64_t>(num_free,
                                        get_block_size()) / get_block_size();
    for (unsigned bin = 0; bin < free.size(); ++bin) {
      intervals += free[bin].num_intervals();
    }
  }
  ldout(cct, 30) << __func__ << " " << intervals << "/" << max_intervals 
                 << dendl;
  ceph_assert(intervals <= max_intervals);
  if (!intervals || max_intervals <= 1) {
    return 0.0;
  }
  intervals--;
  max_intervals--;
  res = (double)intervals / max_intervals;
  return res;
}

void StupidAllocator::dump()
{
  std::lock_guard l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    ldout(cct, 0) << __func__ << " free bin " << bin << ": "
	    	  << free[bin].num_intervals() << " extents" << dendl;
    for (auto p = free[bin].begin();
	 p != free[bin].end();
	 ++p) {
      ldout(cct, 0) << __func__ << "  0x" << std::hex << p.get_start() << "~"
	      	    << p.get_len() << std::dec << dendl;
    }
  }
}

void StupidAllocator::foreach(std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    for (auto p = free[bin].begin(); p != free[bin].end(); ++p) {
      notify(p.get_start(), p.get_len());
    }
  }
}

void StupidAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

void StupidAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   	 << std::dec << dendl;
  interval_set_t rm;
  rm.insert(offset, length);
  for (unsigned i = 0; i < free.size() && !rm.empty(); ++i) {
    interval_set_t overlap;
    overlap.intersection_of(rm, free[i]);
    if (!overlap.empty()) {
      ldout(cct, 20) << __func__ << " bin " << i << " rm 0x" << std::hex << overlap
		     << std::dec << dendl;
      auto it = overlap.begin();
      auto it_end = overlap.end();
      while (it != it_end) {
        auto o = it.get_start();
        auto l = it.get_len();

        free[i].erase(o, l,
          [&](uint64_t off, uint64_t len) {
            unsigned newbin = _choose_bin(len);
            if (newbin != i) {
              ldout(cct, 30) << __func__ << " demoting1 0x" << std::hex << off << "~" << len
                             << std::dec << " to bin " << newbin << dendl;
              _insert_free(off, len);
              return true;
            }
            return false;
          });
        ++it;
      }

      rm.subtract(overlap);
    }
  }
  ceph_assert(rm.empty());
  num_free -= length;
  ceph_assert(num_free >= 0);
}


void StupidAllocator::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
}

