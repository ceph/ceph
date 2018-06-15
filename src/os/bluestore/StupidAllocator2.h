// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_STUPIDALLOCATOR2_H
#define CEPH_OS_BLUESTORE_STUPIDALLOCATOR2_H

#include <mutex>

#include "Allocator.h"
#include "include/btree_map.h"
#include "include/interval_set.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "include/intarith.h"

class StupidAllocator2 : public Allocator {
  CephContext* cct;
  std::mutex lock;

  int64_t num_free;     ///< total bytes in freelist

  struct region;
  typedef mempool::bluestore_alloc::pool_allocator<
      pair<const uint64_t, struct region>> allocator_region_t;
  typedef mempool::bluestore_alloc::pool_allocator<
      pair<const uint64_t, struct region>> allocator_iter_t;
  typedef std::map<uint64_t, struct region, std::less<uint64_t>, allocator_region_t> region_map_t;
  typedef std::map<uint64_t, region_map_t::iterator, std::less<uint64_t>, allocator_iter_t> free_map_t;
  struct region {
    uint64_t length;
    free_map_t::iterator to_all;
  };
  std::vector<region_map_t> bins;
  free_map_t all;
  uint64_t last_alloc;
  static constexpr size_t bins_count = 8 * 10 + 1;

  size_t _choose_bin(uint64_t len);
  void _insert_free(uint64_t offset, uint64_t len);
  void _remove_free(uint64_t offset, uint64_t len);

  uint64_t _aligned_len(
      StupidAllocator2::region_map_t::iterator p,
      ceph::math::p2_t<uint64_t> alloc_unit) {
    uint64_t skew = p->first % alloc_unit;
    if (skew)
      skew = alloc_unit - skew;
    if (skew > p->second.length)
      return 0;
    else
      return p->second.length - skew;
  }
  static region_map_t::iterator lower_bound(region_map_t& map, uint64_t offset);
  static free_map_t::iterator lower_bound(free_map_t& map, uint64_t offset);
  void remove(region_map_t::iterator& region, unsigned bin, uint64_t offset, uint64_t length);
public:
  StupidAllocator2(CephContext* cct);
  ~StupidAllocator2() override;

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  int64_t allocate_int(
    uint64_t want_size, uint64_t alloc_unit, int64_t hint,
    uint64_t *offset, uint32_t *length);

  void release(
    const interval_set<uint64_t>& release_set) override;

  uint64_t get_free() override;

  void dump() override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
