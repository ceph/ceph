// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H
#define CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "include/btree_map.h"
#include "include/interval_set.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"

class StupidAllocator : public Allocator {
  CephContext* cct;
  ceph::mutex lock = ceph::make_mutex("StupidAllocator::lock");

  int64_t num_free;     ///< total bytes in freelist

  typedef mempool::bluestore_alloc::pool_allocator<
    pair<const uint64_t,uint64_t>> allocator_t;
  typedef btree::btree_map<uint64_t,uint64_t,std::less<uint64_t>,allocator_t> interval_set_map_t;
  typedef interval_set<uint64_t,interval_set_map_t> interval_set_t;
  std::vector<interval_set_t> free;  ///< leading-edge copy

  uint64_t last_alloc;

  unsigned _choose_bin(uint64_t len);
  void _insert_free(uint64_t offset, uint64_t len);

  uint64_t _aligned_len(
    interval_set_t::iterator p,
    uint64_t alloc_unit);

public:
  StupidAllocator(CephContext* cct, const std::string& name = "");
  ~StupidAllocator() override;

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  int64_t allocate_int(
    uint64_t want_size, uint64_t alloc_unit, int64_t hint,
    uint64_t *offset, uint32_t *length);

  void release(
    const interval_set<uint64_t>& release_set) override;

  uint64_t get_free() override;
  double get_fragmentation(uint64_t alloc_unit) override;

  void dump() override;
  void dump(std::function<void(uint64_t offset, uint64_t length)> notify) override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
