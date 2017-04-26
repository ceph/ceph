// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc "

StupidAllocator::StupidAllocator(CephContext* cct)
  : cct(cct), num_free(0),
    num_reserved(0),
    free(10),
    last_alloc(0)
{
}

StupidAllocator::~StupidAllocator()
{
}

unsigned StupidAllocator::_choose_bin(uint64_t orig_len)
{
  uint64_t len = orig_len / cct->_conf->bdev_block_size;
  int bin = std::min((int)cbits(len), (int)free.size() - 1);
  dout(30) << __func__ << " len 0x" << std::hex << orig_len << std::dec
	   << " -> " << bin << dendl;
  return bin;
}

void StupidAllocator::_insert_free(uint64_t off, uint64_t len)
{
  unsigned bin = _choose_bin(len);
  dout(30) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << " in bin " << bin << dendl;
  while (true) {
    free[bin].insert(off, len, &off, &len);
    unsigned newbin = _choose_bin(len);
    if (newbin == bin)
      break;
    dout(30) << __func__ << " promoting 0x" << std::hex << off << "~" << len
	     << std::dec << " to bin " << newbin << dendl;
    free[bin].erase(off, len);
    bin = newbin;
  }
}

int StupidAllocator::reserve(uint64_t need)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " need 0x" << std::hex << need
	   << " num_free 0x" << num_free
	   << " num_reserved 0x" << num_reserved << std::dec << dendl;
  if ((int64_t)need > num_free - num_reserved)
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

void StupidAllocator::unreserve(uint64_t unused)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " unused 0x" << std::hex << unused
	   << " num_free 0x" << num_free
	   << " num_reserved 0x" << num_reserved << std::dec << dendl;
  assert(num_reserved >= (int64_t)unused);
  num_reserved -= unused;
}

/// return the effective length of the extent if we align to alloc_unit
static uint64_t aligned_len(btree_interval_set<uint64_t>::iterator p,
			    uint64_t alloc_unit)
{
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  if (skew > p.get_len())
    return 0;
  else
    return p.get_len() - skew;
}

int64_t StupidAllocator::allocate_int(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " want_size 0x" << std::hex << want_size
	   << " alloc_unit 0x" << alloc_unit
	   << " hint 0x" << hint << std::dec
	   << dendl;
  uint64_t want = MAX(alloc_unit, want_size);
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
	if (aligned_len(p, alloc_unit) >= want_size) {
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
      if (aligned_len(p, alloc_unit) >= want_size) {
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
	if (aligned_len(p, alloc_unit) >= alloc_unit) {
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
      if (aligned_len(p, alloc_unit) >= alloc_unit) {
	goto found;
      }
      ++p;
    }
  }

  return -ENOSPC;

 found:
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  *offset = p.get_start() + skew;
  *length = MIN(MAX(alloc_unit, want_size), p.get_len() - skew);
  if (cct->_conf->bluestore_debug_small_allocations) {
    uint64_t max =
      alloc_unit * (rand() % cct->_conf->bluestore_debug_small_allocations);
    if (max && *length > max) {
      dout(10) << __func__ << " shortening allocation of 0x" << std::hex
	       << *length << " -> 0x"
	       << max << " due to debug_small_allocations" << std::dec << dendl;
      *length = max;
    }
  }
  dout(30) << __func__ << " got 0x" << std::hex << *offset << "~" << *length
	   << " from bin " << std::dec << bin << dendl;

  free[bin].erase(*offset, *length);
  uint64_t off, len;
  if (*offset && free[bin].contains(*offset - skew - 1, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }
  if (free[bin].contains(*offset + *length, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }

  num_free -= *length;
  num_reserved -= *length;
  assert(num_free >= 0);
  assert(num_reserved >= 0);
  last_alloc = *offset + *length;
  return 0;
}

int64_t StupidAllocator::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  mempool::bluestore_alloc::vector<AllocExtent> *extents)
{
  uint64_t allocated_size = 0;
  uint64_t offset = 0;
  uint32_t length = 0;
  int res = 0;

  if (max_alloc_size == 0) {
    max_alloc_size = want_size;
  }

  ExtentList block_list = ExtentList(extents, 1, max_alloc_size);

  while (allocated_size < want_size) {
    res = allocate_int(MIN(max_alloc_size, (want_size - allocated_size)),
       alloc_unit, hint, &offset, &length);
    if (res != 0) {
      /*
       * Allocation failed.
       */
      break;
    }
    block_list.add_extents(offset, length);
    allocated_size += length;
    hint = offset + length;
  }

  if (allocated_size == 0) {
    return -ENOSPC;
  }
  return allocated_size;
}

void StupidAllocator::release(
  uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

uint64_t StupidAllocator::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void StupidAllocator::dump()
{
  std::lock_guard<std::mutex> l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    dout(0) << __func__ << " free bin " << bin << ": "
	    << free[bin].num_intervals() << " extents" << dendl;
    for (auto p = free[bin].begin();
	 p != free[bin].end();
	 ++p) {
      dout(0) << __func__ << "  0x" << std::hex << p.get_start() << "~"
	      << p.get_len() << std::dec << dendl;
    }
  }
}

void StupidAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

void StupidAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  btree_interval_set<uint64_t> rm;
  rm.insert(offset, length);
  for (unsigned i = 0; i < free.size() && !rm.empty(); ++i) {
    btree_interval_set<uint64_t> overlap;
    overlap.intersection_of(rm, free[i]);
    if (!overlap.empty()) {
      dout(20) << __func__ << " bin " << i << " rm 0x" << std::hex << overlap
	       << std::dec << dendl;
      free[i].subtract(overlap);
      rm.subtract(overlap);
    }
  }
  assert(rm.empty());
  num_free -= length;
  assert(num_free >= 0);
}


void StupidAllocator::shutdown()
{
  dout(1) << __func__ << dendl;
}

