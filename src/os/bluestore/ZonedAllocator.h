// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// 
// A simple allocator that just hands out space from the next empty zone.  This
// is temporary, just to get the simplest append-only write workload to work.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#ifndef CEPH_OS_BLUESTORE_ZONEDALLOCATOR_H
#define CEPH_OS_BLUESTORE_ZONEDALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "common/ceph_mutex.h"
#include "include/btree_map.h"
#include "include/interval_set.h"
#include "include/mempool.h"
#include "os/bluestore/bluestore_types.h"

class ZonedAllocator : public Allocator {
  CephContext* cct;

  // Currently only one thread at a time calls into ZonedAllocator due to
  // atomic_alloc_and_submit_lock in BlueStore.cc, but we do locking anyway
  // because eventually ZONE_APPEND support will land and
  // atomic_alloc_and_submit_lock will be removed.
  ceph::mutex lock = ceph::make_mutex("ZonedAllocator::lock");

  int64_t num_free;     ///< total bytes in freelist
  uint64_t size;
  uint64_t block_size;
  uint64_t zone_size;
  uint64_t starting_zone;
  uint64_t nr_zones;
  std::vector<uint64_t> write_pointers;

  inline uint64_t zone_offset(uint64_t zone) {
    ceph_assert(zone < nr_zones);
    return zone * zone_size + zone_wp(zone);
  }

  inline uint64_t zone_wp(uint64_t zone) {
    ceph_assert(zone < nr_zones);
    return write_pointers[zone];
  }

  inline uint64_t zone_free_space(uint64_t zone) {
    ceph_assert(zone < nr_zones);
    return zone_size - zone_wp(zone);
  }

  inline void advance_wp(uint64_t zone, uint64_t size) {
    ceph_assert(zone < nr_zones);
    write_pointers[zone] += size;
    ceph_assert(write_pointers[zone] <= zone_size);
  }

  inline bool fits(uint64_t want_size, uint64_t zone) {
    ceph_assert(zone < nr_zones);
    return want_size <= zone_free_space(zone);
  }

public:
  ZonedAllocator(CephContext* cct, int64_t size, int64_t block_size,
                 const std::string& name);
  ~ZonedAllocator() override;

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  void release(const interval_set<uint64_t>& release_set) override;

  uint64_t get_free() override;

  void dump() override;
  void dump(std::function<void(uint64_t offset,
                               uint64_t length)> notify) override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
