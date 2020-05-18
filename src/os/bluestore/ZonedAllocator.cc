// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// 
// A simple allocator that just hands out space from the next empty zone.  This
// is temporary, just to get the simplest append-only write workload to work.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#include "ZonedAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "ZonedAllocator " << this << " "

ZonedAllocator::ZonedAllocator(CephContext* cct,
			       int64_t size,
			       int64_t block_size,
			       const std::string& name)
    : Allocator(name),
      cct(cct),
      num_free(0),
      size(size),
      block_size((block_size & 0x00000000ffffffff)),
      zone_size(((block_size & 0x0000ffff00000000) >> 32) * 1024 * 1024),
      starting_zone((block_size & 0xffff000000000000) >> 48),
      nr_zones(size / zone_size),
      write_pointers(nr_zones) {
  ldout(cct, 10) << __func__ << " size 0x" << std::hex << size
		 << " zone size 0x" << zone_size << std::dec
		 << " number of zones " << nr_zones
                 << " first sequential zone " << starting_zone
		 << dendl;
  ceph_assert(size % zone_size == 0);
}

ZonedAllocator::~ZonedAllocator() {}

int64_t ZonedAllocator::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  PExtentVector *extents) {
  std::lock_guard l(lock);

  ceph_assert(want_size % 4096 == 0);

  ldout(cct, 10) << __func__ << " trying to allocate "
		 << std::hex << want_size << dendl;

  uint64_t zone = starting_zone;
  for ( ; zone < nr_zones; ++zone) {
    if (fits(want_size, zone))
      break;
    ldout(cct, 10) << __func__ << " skipping zone " << zone
		   << " because there is not enough space: "
		   << " want_size = " << want_size
		   << " available = " << zone_free_space(zone) << dendl;
  }

  if (zone == nr_zones) {
    ldout(cct, 10) << __func__ << " failed to allocate" << dendl;
    return -ENOSPC;
  }

  uint64_t offset = zone_offset(zone);
  ldout(cct, 10) << __func__ << " advancing zone " << zone
		 << " write pointer from " << std::hex << offset
		 << " to " << offset + want_size << dendl;
  advance_wp(zone, want_size);

  if (zone_free_space(zone) == 0) {
    starting_zone = zone + 1;
  }

  ldout(cct, 10) << __func__ << " zone " << zone << " offset is now "
		 << std::hex << zone_wp(zone) << dendl;

  ldout(cct, 10) << __func__ << " allocated " << std::hex << want_size
		 << " bytes at offset " << offset
		 << " located at zone " << zone
		 << " and zone offset " << offset % zone_size << dendl;

  extents->emplace_back(bluestore_pextent_t(offset, want_size));
  return want_size;
}

void ZonedAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
}

uint64_t ZonedAllocator::get_free() {
  std::lock_guard l(lock);
  return num_free;
}

void ZonedAllocator::dump() {
  std::lock_guard l(lock);
}

void ZonedAllocator::dump(std::function<void(uint64_t offset,
					     uint64_t length)> notify) {
  std::lock_guard l(lock);
}

void ZonedAllocator::init_add_free(uint64_t offset, uint64_t length) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " " << std::hex
		 << offset << "~" << length << dendl;

  num_free += length;
  uint64_t zone = offset / zone_size;
  offset %= zone_size;
  write_pointers[zone] = offset;
  ldout(cct, 10) << __func__ << " set zone " << std::hex
		 << zone << " write pointer to 0x" << offset << dendl;

  length -= zone_size - offset;
  ceph_assert(length % zone_size == 0);

  for ( ; length; length -= zone_size) {
    write_pointers[++zone] = 0;
    ldout(cct, 30) << __func__ << " set zone 0x" << std::hex
		   << zone << " write pointer to 0x" << 0 << dendl;
  }
}

void ZonedAllocator::init_rm_free(uint64_t offset, uint64_t length) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex
		 << offset << "~" << length << dendl;

  num_free -= length;
  ceph_assert(num_free >= 0);

  uint64_t zone = offset / zone_size;
  offset %= zone_size;
  ceph_assert(write_pointers[zone] == offset);
  write_pointers[zone] = zone_size;
  ldout(cct, 10) << __func__ << " set zone 0x" << std::hex
		 << zone << " write pointer to 0x" << zone_size << dendl;

  length -= zone_size - offset;
  ceph_assert(length % zone_size == 0);

  for ( ; length; length -= zone_size) {
    write_pointers[++zone] = zone_size;
    ldout(cct, 10) << __func__ << " set zone 0x" << std::hex
		   << zone << " write pointer to 0x" << zone_size << dendl;
  }
}


void ZonedAllocator::shutdown() {
  ldout(cct, 1) << __func__ << dendl;
}
