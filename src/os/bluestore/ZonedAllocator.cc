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
#define dout_prefix *_dout << "ZonedAllocator(" << this << ") " << __func__ << " "

ZonedAllocator::ZonedAllocator(CephContext* cct,
			       int64_t size,
			       int64_t blk_size,
			       int64_t _zone_size,
			       int64_t _first_sequential_zone,
			       std::string_view name)
    : Allocator(name, size, blk_size),
      cct(cct),
      num_free(0),
      size(size),
      conventional_size(_first_sequential_zone * _zone_size),
      sequential_size(size - conventional_size),
      block_size(blk_size),
      zone_size(_zone_size),
      first_seq_zone_num(_first_sequential_zone),
      starting_zone_num(first_seq_zone_num),
      num_zones(size / zone_size)
{
  ldout(cct, 10) << " size 0x" << std::hex << size
		 << " zone size 0x" << zone_size << std::dec
		 << " number of zones " << num_zones
		 << " first sequential zone " << starting_zone_num
		 << std::dec
		 << dendl;
  ceph_assert(size % zone_size == 0);

  zone_states.resize(num_zones);
  num_free = num_zones * zone_size;
}

ZonedAllocator::~ZonedAllocator()
{
}

int64_t ZonedAllocator::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  PExtentVector *extents)
{
  std::lock_guard l(lock);

  ceph_assert(want_size % 4096 == 0);

  ldout(cct, 10) << " trying to allocate 0x"
		 << std::hex << want_size << std::dec << dendl;

  uint64_t zone_num = starting_zone_num;
  for ( ; zone_num < num_zones; ++zone_num) {
    if (fits(want_size, zone_num)) {
      break;
    }
    ldout(cct, 10) << " skipping zone 0x" << std::hex << zone_num
		   << " because there is not enough space: "
		   << " want_size = 0x" << want_size
		   << " available = 0x" << get_remaining_space(zone_num)
		   << std::dec
		   << dendl;
  }

  if (zone_num == num_zones) {
    ldout(cct, 10) << " failed to allocate" << dendl;
    return -ENOSPC;
  }

  uint64_t offset = get_offset(zone_num);

  ldout(cct, 10) << " moving zone 0x" << std::hex
		 << zone_num << " write pointer from 0x" << offset
		 << " -> 0x" << offset + want_size
		 << std::dec << dendl;

  increment_write_pointer(zone_num, want_size);
  num_free -= want_size;
  if (get_remaining_space(zone_num) == 0) {
    starting_zone_num = zone_num + 1;
  }

  ldout(cct, 10) << " allocated 0x" << std::hex << offset << "~" << want_size
		 << " from zone 0x" << zone_num
		 << " and zone offset 0x" << (offset % zone_size)
		 << std::dec << dendl;

  extents->emplace_back(bluestore_pextent_t(offset, want_size));
  return want_size;
}

void ZonedAllocator::release(const interval_set<uint64_t>& release_set)
{
  std::lock_guard l(lock);
  for (auto p = cbegin(release_set); p != cend(release_set); ++p) {
    auto offset = p.get_start();
    auto length = p.get_len();
    uint64_t zone_num = offset / zone_size;
    ldout(cct, 10) << " 0x" << std::hex << offset << "~" << length
		   << " from zone 0x" << zone_num << std::dec << dendl;
    uint64_t num_dead = std::min(zone_size - offset % zone_size, length);
    for ( ; length; ++zone_num) {
      increment_num_dead_bytes(zone_num, num_dead);
      length -= num_dead;
      num_dead = std::min(zone_size, length);
    }
  }
}

uint64_t ZonedAllocator::get_free()
{
  return num_free;
}

void ZonedAllocator::dump()
{
  std::lock_guard l(lock);
}

void ZonedAllocator::dump(std::function<void(uint64_t offset,
					     uint64_t length)> notify)
{
  std::lock_guard l(lock);
}

void ZonedAllocator::init_from_zone_pointers(
  std::vector<zone_state_t> _zone_states)
{
  // this is called once, based on the device's zone pointers
  std::lock_guard l(lock);
  ldout(cct, 10) << dendl;
  zone_states = std::move(_zone_states);
  num_free = 0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    num_free += zone_size - (zone_states[i].write_pointer % zone_size);
  }
  uint64_t conventional_size = first_seq_zone_num * zone_size;
  uint64_t sequential_size = size - conventional_size;
  ldout(cct, 10) << "free 0x" << std::hex << num_free
		 << " / 0x" << sequential_size << std::dec
		 << dendl;
}

int64_t ZonedAllocator::pick_zone_to_clean(void)
{
  int32_t best = -1;
  int64_t best_score = 0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    int64_t score = zone_states[i].num_dead_bytes;
    // discount by remaining space so we will tend to clean full zones
    score -= (zone_size - zone_states[i].write_pointer) / 2;
    if (score > 0 && (best < 0 || score > best_score)) {
      best = i;
      best_score = score;
    }
  }
  if (best >= 0) {
    ldout(cct, 10) << " zone 0x" << std::hex << best << " with score 0x" << best_score
		   << ": 0x" << zone_states[best].num_dead_bytes
		   << " dead and 0x"
		   << zone_states[best].write_pointer - zone_states[best].num_dead_bytes
		   << " live bytes" << std::dec << dendl;
  } else {
    ldout(cct, 10) << " no zones found that are good cleaning candidates" << dendl;
  }
  return best;
}

bool ZonedAllocator::low_on_space(void)
{
  std::lock_guard l(lock);
  uint64_t sequential_num_free = num_free - conventional_size;
  double free_ratio = static_cast<double>(sequential_num_free) / sequential_size;

  ldout(cct, 10) << " free 0x" << std::hex << sequential_num_free
		 << "/ 0x" << sequential_size << std::dec
		 << ", free ratio is " << free_ratio << dendl;
  ceph_assert(sequential_num_free <= sequential_size);

  // TODO: make 0.25 tunable
  return free_ratio <= 0.25;
}

void ZonedAllocator::shutdown()
{
  ldout(cct, 1) << dendl;
}
