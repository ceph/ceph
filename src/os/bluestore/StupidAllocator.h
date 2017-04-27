// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H
#define CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "include/btree_interval_set.h"
#include "os/bluestore/bluestore_types.h"

class StupidAllocator : public Allocator {
  CephContext* cct;
  std::mutex lock;

  int64_t num_free;     ///< total bytes in freelist
  int64_t num_reserved; ///< reserved bytes

  std::vector<btree_interval_set<uint64_t> > free;        ///< leading-edge copy

  uint64_t last_alloc;

  unsigned _choose_bin(uint64_t len);
  void _insert_free(uint64_t offset, uint64_t len);

public:
  StupidAllocator(CephContext* cct);
  ~StupidAllocator() override;

  int reserve(uint64_t need) override;
  void unreserve(uint64_t unused) override;

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents) override;

  int64_t allocate_int(
    uint64_t want_size, uint64_t alloc_unit, int64_t hint,
    uint64_t *offset, uint32_t *length);

  void release(
    uint64_t offset, uint64_t length) override;

  uint64_t get_free() override;

  void dump() override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
