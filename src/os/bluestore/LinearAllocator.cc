// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LinearAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "linearalloc "

LinearAllocator::LinearAllocator(CephContext* cct)
  : cct(cct), num_free(0),
    num_reserved(0),
    free(10),
    last_alloc(0)
{
}

LinearAllocator::~LinearAllocator()
{
}

void LinearAllocator::_insert_free(uint64_t off, uint64_t len)
{
  ldout(cct, 30) << __func__ << " 0x" << std::hex << off << "~" << len
		 << std::dec << dendl;
  free.insert(off, len, &off, &len);
}

int LinearAllocator::reserve(uint64_t need)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " need 0x" << std::hex << need
		 << " num_free 0x" << num_free
		 << " num_reserved 0x" << num_reserved << std::dec << dendl;
  if ((int64_t)need > num_free - num_reserved)
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

void LinearAllocator::unreserve(uint64_t unused)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " unused 0x" << std::hex << unused
		 << " num_free 0x" << num_free
		 << " num_reserved 0x" << num_reserved << std::dec << dendl;
  assert(num_reserved >= (int64_t)unused);
  num_reserved -= unused;
}

/// return the effective length of the extent if we align to alloc_unit
uint64_t LinearAllocator::_aligned_len(
  btree_interval_set<uint64_t,allocator>::iterator p,
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

int64_t LinearAllocator::allocate_int(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " want_size 0x" << std::hex << want_size
		 << " alloc_unit 0x" << alloc_unit
		 << " hint 0x" << hint << std::dec
		 << dendl;
  uint64_t want = MAX(alloc_unit, want_size);

  auto p = free.begin();

  if (!hint) {
    hint = last_alloc;
  }

  // search up (from hint)
  if (hint) {
    p = free.lower_bound(hint);
    while (p != free.end()) {
      if (_aligned_len(p, alloc_unit) >= want_size) {
	goto found;
      }
      ++p;
    }
  }

  // search up (from origin, and skip searched extents by hint)
  p = free.begin();
  auto end = hint ? free.lower_bound(hint) : free.end();
  while (p != end) {
    if (_aligned_len(p, alloc_unit) >= want_size) {
      goto found;
    }
    ++p;
  }

  return -ENOSPC;

 found:
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  *offset = p.get_start() + skew;
  *length = MIN(MAX(alloc_unit, want_size), P2ALIGN((p.get_len() - skew), alloc_unit));
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
		 << std::dec << dendl;

  free.erase(*offset, *length);
  uint64_t off, len;

  num_free -= *length;
  num_reserved -= *length;
  assert(num_free >= 0);
  assert(num_reserved >= 0);
  last_alloc = *offset + *length;
  return 0;
}

int64_t LinearAllocator::allocate(
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

void LinearAllocator::release(
  uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

uint64_t LinearAllocator::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void LinearAllocator::dump()
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 0) << __func__ << " free: "
		<< free.num_intervals() << " extents" << dendl;
  for (auto p = free.begin();
       p != free.end();
       ++p) {
    ldout(cct, 0) << __func__ << "  0x" << std::hex << p.get_start() << "~"
		  << p.get_len() << std::dec << dendl;
  }
}

void LinearAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

void LinearAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  btree_interval_set<uint64_t,allocator> rm;
  rm.insert(offset, length);
  btree_interval_set<uint64_t,allocator> overlap;
  overlap.intersection_of(rm, free);
  if (!overlap.empty()) {
    ldout(cct, 20) << __func__ << " rm 0x" << std::hex << overlap
		   << std::dec << dendl;
    free.subtract(overlap);
    rm.subtract(overlap);
  }
  assert(rm.empty());
  num_free -= length;
  assert(num_free >= 0);
}


void LinearAllocator::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
}
