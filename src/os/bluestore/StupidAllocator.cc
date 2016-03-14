// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator.h"
#include "bluestore_types.h"
#include "BlueStore.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc "

StupidAllocator::StupidAllocator()
  : num_free(0),
    num_uncommitted(0),
    num_committing(0),
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
  uint64_t len = orig_len / g_conf->bluestore_min_alloc_size;
  int bin = 0;
  while (len && bin + 1 < (int)free.size()) {
    len >>= 1;
    bin++;
  }
  dout(30) << __func__ << " len " << orig_len << " -> " << bin << dendl;
  return bin;
}

void StupidAllocator::_insert_free(uint64_t off, uint64_t len)
{
  unsigned bin = _choose_bin(len);
  dout(30) << __func__ << " " << off << "~" << len << " in bin " << bin << dendl;
  while (true) {
    free[bin].insert(off, len, &off, &len);
    unsigned newbin = _choose_bin(len);
    if (newbin == bin)
      break;
    dout(30) << __func__ << " promoting " << off << "~" << len
	     << " to bin " << newbin << dendl;
    free[bin].erase(off, len);
    bin = newbin;
  }
}

int StupidAllocator::reserve(uint64_t need)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " need " << need << " num_free " << num_free
	   << " num_reserved " << num_reserved << dendl;
  if ((int64_t)need > num_free - num_reserved)
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

void StupidAllocator::unreserve(uint64_t unused)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " unused " << unused << " num_free " << num_free
	   << " num_reserved " << num_reserved << dendl;
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

int StupidAllocator::allocate(
  uint64_t need_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " need_size " << need_size
	   << " alloc_unit " << alloc_unit
	   << " hint " << hint
	   << dendl;
  uint64_t want = MAX(alloc_unit, need_size);
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
	if (aligned_len(p, alloc_unit) >= need_size) {
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
      if (aligned_len(p, alloc_unit) >= need_size) {
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

  assert(0 == "caller didn't reserve?");
  return -ENOSPC;

 found:
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  *offset = p.get_start() + skew;
  *length = MIN(MAX(alloc_unit, need_size), p.get_len() - skew);
  if (g_conf->bluestore_debug_small_allocations) {
    uint64_t max =
      alloc_unit * (rand() % g_conf->bluestore_debug_small_allocations);
    if (max && *length > max) {
      dout(10) << __func__ << " shortening allocation of " << *length << " -> "
	       << max << " due to debug_small_allocations" << dendl;
      *length = max;
    }
  }
  dout(30) << __func__ << " got " << *offset << "~" << *length << " from bin "
	   << bin << dendl;

  free[bin].erase(*offset, *length);
  uint64_t off, len;
  if (*offset && free[bin].contains(*offset - skew - 1, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting " << off << "~" << len
	       << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }
  if (free[bin].contains(*offset + *length, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting " << off << "~" << len
	       << " to bin " << newbin << dendl;
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

int StupidAllocator::release(
  uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  uncommitted.insert(offset, length);
  num_uncommitted += length;
  return 0;
}

uint64_t StupidAllocator::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void StupidAllocator::dump(ostream& out)
{
  std::lock_guard<std::mutex> l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    dout(30) << __func__ << " free bin " << bin << ": "
	     << free[bin].num_intervals() << " extents" << dendl;
    for (auto p = free[bin].begin();
	 p != free[bin].end();
	 ++p) {
      dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
    }
  }
  dout(30) << __func__ << " committing: "
	   << committing.num_intervals() << " extents" << dendl;
  for (auto p = committing.begin();
       p != committing.end();
       ++p) {
    dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
  }
  dout(30) << __func__ << " uncommitted: "
	   << uncommitted.num_intervals() << " extents" << dendl;
  for (auto p = uncommitted.begin();
       p != uncommitted.end();
       ++p) {
    dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
  }
}

void StupidAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  _insert_free(offset, length);
  num_free += length;
}

void StupidAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  btree_interval_set<uint64_t> rm;
  rm.insert(offset, length);
  for (unsigned i = 0; i < free.size() && !rm.empty(); ++i) {
    btree_interval_set<uint64_t> overlap;
    overlap.intersection_of(rm, free[i]);
    if (!overlap.empty()) {
      dout(20) << __func__ << " bin " << i << " rm " << overlap << dendl;
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

void StupidAllocator::commit_start()
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " releasing " << num_uncommitted
	   << " in extents " << uncommitted.num_intervals() << dendl;
  assert(committing.empty());
  committing.swap(uncommitted);
  num_committing = num_uncommitted;
  num_uncommitted = 0;
}

void StupidAllocator::commit_finish()
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " released " << num_committing
	   << " in extents " << committing.num_intervals() << dendl;
  for (auto p = committing.begin();
       p != committing.end();
       ++p) {
    _insert_free(p.get_start(), p.get_len());
  }
  committing.clear();
  num_free += num_committing;
  num_committing = 0;
}
