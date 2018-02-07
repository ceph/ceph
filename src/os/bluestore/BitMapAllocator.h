// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITMAPALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "BitAllocator.h"

class BitMapAllocator : public Allocator {
  CephContext* cct;

  int64_t m_block_size;
  int64_t m_total_size;

  BitAllocator *m_bit_alloc; // Bit allocator instance

  void insert_free(uint64_t offset, uint64_t len);

  int64_t allocate_dis(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents);

public:
  BitMapAllocator(CephContext* cct, int64_t device_size, int64_t block_size);
  ~BitMapAllocator() override;

  int reserve(uint64_t need) override;
  void unreserve(uint64_t unused) override;

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  void release(
    const interval_set<uint64_t>& release_set) override;

  uint64_t get_free() override;

  void dump() override;

  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
