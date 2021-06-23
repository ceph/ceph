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
			       int64_t blk_size,
			       std::string_view name)
    : Allocator(name, size, blk_size & 0x00000000ffffffff),
      cct(cct),
      num_free(0),
      size(size),
      // To avoid interface changes, we piggyback zone size and the first
      // sequential zone number onto the first 32 bits of 64-bit |blk_size|.
      // The last 32 bits of |blk_size| is holding the actual block size.
      block_size((blk_size & 0x00000000ffffffff)),
      zone_size(((blk_size & 0x0000ffff00000000) >> 32) * 1024 * 1024),
      first_seq_zone_num((blk_size >> 48) & 0xffff),
      starting_zone_num(first_seq_zone_num),
      num_zones(size / zone_size),
      num_zones_to_clean(0) {
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
  auto p = zones_to_clean.lower_bound(zone_num);
  for ( ; zone_num < num_zones; ++zone_num) {
    if (p != zones_to_clean.cend() && *p == zone_num) {
      ldout(cct, 10) << __func__ << " skipping zone " << zone_num
		     << " because it is being cleaned" << dendl;
      ++p;
      continue;
    }
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

  ldout(cct, 10) << __func__ << " incrementing zone " << std::hex
		 << zone_num << " write pointer from " << offset
		 << " to " << offset + want_size << dendl;

  increment_write_pointer(zone_num, want_size);
  num_free -= want_size;
  if (get_remaining_space(zone_num) == 0) {
    starting_zone_num = zone_num + 1;
  }

  ldout(cct, 10) << __func__ << std::hex << " zone " << zone_num
		 << " offset is now " << get_write_pointer(zone_num) << dendl;

  ldout(cct, 10) << __func__ << " allocated " << std::hex << want_size
		 << " bytes at offset " << offset
		 << " located at zone " << zone_num
		 << " and zone offset " << offset % zone_size << dendl;

  find_zones_to_clean();

  extents->emplace_back(bluestore_pextent_t(offset, want_size));
  return want_size;
}

void ZonedAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
  for (auto p = cbegin(release_set); p != cend(release_set); ++p) {
    auto offset = p.get_start();
    auto length = p.get_len();
    ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length << dendl;
    uint64_t zone_num = offset / zone_size;
    uint64_t num_dead = std::min(zone_size - offset % zone_size, length);
    for ( ; length; ++zone_num) {
      increment_num_dead_bytes(zone_num, num_dead);
      length -= num_dead;
      num_dead = std::min(zone_size, length);
    }
  }
}

uint64_t ZonedAllocator::get_free() {
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
// init_alloc, as it updates the write pointer for each zone.
void ZonedAllocator::init_add_free(uint64_t offset, uint64_t length) {
  ldout(cct, 40) << __func__ << " " << std::hex
		 << offset << "~" << length << dendl;

  num_free += length;
}

void ZonedAllocator::init_rm_free(uint64_t offset, uint64_t length) {
  std::lock_guard l(lock);
  ldout(cct, 40) << __func__ << " 0x" << std::hex
		 << offset << "~" << length << dendl;

  num_free -= length;
  ceph_assert(num_free >= 0);

  uint64_t zone_num = offset / zone_size;
  uint64_t write_pointer = offset % zone_size;
  uint64_t remaining_space = get_remaining_space(zone_num);

  ceph_assert(get_write_pointer(zone_num) == write_pointer);
  ceph_assert(remaining_space <= length);
  increment_write_pointer(zone_num, remaining_space);

  ldout(cct, 40) << __func__ << " set zone 0x" << std::hex
		 << zone_num << " write pointer to 0x" << zone_size << dendl;

  length -= remaining_space;
  ceph_assert(length % zone_size == 0);

  for ( ; length; length -= zone_size) {
    increment_write_pointer(++zone_num, zone_size);
    ldout(cct, 40) << __func__ << " set zone 0x" << std::hex
		   << zone_num << " write pointer to 0x" << zone_size << dendl;
  }
}

const std::set<uint64_t> *ZonedAllocator::get_zones_to_clean(void) {
  ldout(cct, 10) << __func__ << dendl;
  return num_zones_to_clean ? &zones_to_clean : nullptr;
}

bool ZonedAllocator::low_on_space(void) {
  ceph_assert(zones_to_clean.empty());

  uint64_t conventional_size = first_seq_zone_num * zone_size;
  uint64_t sequential_size = size - conventional_size;
  uint64_t sequential_num_free = num_free - conventional_size;
  double free_ratio = static_cast<double>(sequential_num_free) / sequential_size;

  ldout(cct, 10) << __func__ << " free size " << sequential_num_free
		 << " total size " << sequential_size
		 << " free ratio is " << free_ratio << dendl;

   // TODO: make 0.25 tunable
  return free_ratio <= 0.25;
}

void ZonedAllocator::find_zones_to_clean(void) {
  ldout(cct, 40) << __func__ << dendl;

  if (num_zones_to_clean || !low_on_space())
    return;

  ceph_assert(zones_to_clean.empty());
  
  // TODO: make this tunable; handle the case when there aren't this many zones
  // to clean.
  const int64_t num_zones_to_clean_at_once = 1;

  std::vector<uint64_t> idx(num_zones);
  std::iota(idx.begin(), idx.end(), 0);
  
  if (cct->_conf->subsys.should_gather<ceph_subsys_bluestore, 40>()) {
    for (size_t i = 0; i < zone_states.size(); ++i) {
      dout(40) << __func__ << " zone " << i << zone_states[i] << dendl;
    }
  }

  std::partial_sort(idx.begin(), idx.begin() + num_zones_to_clean_at_once, idx.end(),
		    [this](uint64_t i1, uint64_t i2) {
		      return zone_states[i1].num_dead_bytes > zone_states[i2].num_dead_bytes;
		    });

  ldout(cct, 10) << __func__ << " the zone that needs cleaning is "
		 << *idx.begin() << " num_dead_bytes = " << zone_states[*idx.begin()].num_dead_bytes
		 << dendl;

  zones_to_clean = {idx.begin(), idx.begin() + num_zones_to_clean_at_once};
  num_zones_to_clean = num_zones_to_clean_at_once;

  // TODO: handle the case of disk being full.
  ceph_assert(!zones_to_clean.empty());
  ceph_assert(num_zones_to_clean != 0);

  cleaner_lock->lock();
  cleaner_cond->notify_one();
  cleaner_lock->unlock();
}
 
void ZonedAllocator::init_alloc(std::vector<zone_state_t> &&_zone_states,
				ceph::mutex *_cleaner_lock,
				ceph::condition_variable *_cleaner_cond) {
  std::lock_guard l(lock);
  cleaner_lock = _cleaner_lock;
  cleaner_cond = _cleaner_cond;
  ldout(cct, 10) << __func__ << dendl;
  zone_states = std::move(_zone_states);
}

void ZonedAllocator::mark_zones_to_clean_free(void) {
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << dendl;
  for (auto zone_num : zones_to_clean) {
    ldout(cct, 10) << __func__ << " zone " << zone_num << " is now clean" << dendl;
    num_free += zone_states[zone_num].write_pointer;
    zone_states[zone_num].num_dead_bytes = 0;
    zone_states[zone_num].write_pointer = 0;
  }
  zones_to_clean.clear();
  num_zones_to_clean = 0;
}

void ZonedAllocator::shutdown() {
  ldout(cct, 1) << __func__ << dendl;
}
