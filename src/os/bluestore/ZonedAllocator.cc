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
#include "zoned_types.h"
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
      // To avoid interface changes, we piggyback zone size and the first
      // sequential zone number onto the first 32 bits of 64-bit |block_size|.
      // The last 32 bits of |block_size| is holding the actual block size.
      block_size((block_size & 0x00000000ffffffff)),
      zone_size(((block_size & 0x0000ffff00000000) >> 32) * 1024 * 1024),
      starting_zone_num((block_size & 0xffff000000000000) >> 48),
      num_zones(size / zone_size) {
  ldout(cct, 10) << __func__ << " size 0x" << std::hex << size
		 << " zone size 0x" << zone_size << std::dec
		 << " number of zones " << num_zones
		 << " first sequential zone " << starting_zone_num
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

  uint64_t zone_num = starting_zone_num;
  for ( ; zone_num < num_zones; ++zone_num) {
    if (fits(want_size, zone_num)) {
      break;
    }
    ldout(cct, 10) << __func__ << " skipping zone " << zone_num
		   << " because there is not enough space: "
		   << " want_size = " << want_size
		   << " available = " << get_remaining_space(zone_num)
		   << dendl;
  }

  if (zone_num == num_zones) {
    ldout(cct, 10) << __func__ << " failed to allocate" << dendl;
    return -ENOSPC;
  }

  uint64_t offset = get_offset(zone_num);

  ldout(cct, 10) << __func__ << " advancing zone " << std::hex
		 << zone_num << " write pointer from " << offset
		 << " to " << offset + want_size << dendl;

  advance_write_pointer(zone_num, want_size);
  if (get_remaining_space(zone_num) == 0) {
    starting_zone_num = zone_num + 1;
  }

  ldout(cct, 10) << __func__ << std::hex << " zone " << zone_num
		 << " offset is now " << get_write_pointer(zone_num) << dendl;

  ldout(cct, 10) << __func__ << " allocated " << std::hex << want_size
		 << " bytes at offset " << offset
		 << " located at zone " << zone_num
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

// This just increments |num_free|.  The actual free space is added by
// set_zone_states, as it updates the write pointer for each zone.
void ZonedAllocator::init_add_free(uint64_t offset, uint64_t length) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " " << std::hex
		 << offset << "~" << length << dendl;

  num_free += length;
}

void ZonedAllocator::init_rm_free(uint64_t offset, uint64_t length) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << " 0x" << std::hex
		 << offset << "~" << length << dendl;

  num_free -= length;
  ceph_assert(num_free >= 0);

  uint64_t zone_num = offset / zone_size;
  uint64_t write_pointer = offset % zone_size;
  uint64_t remaining_space = get_remaining_space(zone_num);

  ceph_assert(get_write_pointer(zone_num) == write_pointer);
  ceph_assert(remaining_space <= length);
  advance_write_pointer(zone_num, remaining_space);

  ldout(cct, 10) << __func__ << " set zone 0x" << std::hex
		 << zone_num << " write pointer to 0x" << zone_size << dendl;

  length -= remaining_space;
  ceph_assert(length % zone_size == 0);

  for ( ; length; length -= zone_size) {
    advance_write_pointer(++zone_num, zone_size);
    ldout(cct, 10) << __func__ << " set zone 0x" << std::hex
		   << zone_num << " write pointer to 0x" << zone_size << dendl;
  }
}

void ZonedAllocator::set_zone_states(std::vector<zone_state_t> &&_zone_states) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << dendl;
  zone_states = std::move(_zone_states);
}

void ZonedAllocator::shutdown() {
  ldout(cct, 1) << __func__ << dendl;
}
