// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_OS_BLUESTORE_ALLOCATOR_H
#define CEPH_OS_BLUESTORE_ALLOCATOR_H

#include <functional>
#include <ostream>
#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "zoned_types.h"

class Allocator {
public:
  explicit Allocator(const std::string& name);
  virtual ~Allocator();

  /*
   * Allocate required number of blocks in n number of extents.
   * Min and Max number of extents are limited by:
   * a. alloc unit
   * b. max_alloc_size.
   * as no extent can be lesser than alloc_unit and greater than max_alloc size.
   * Apart from that extents can vary between these lower and higher limits according
   * to free block search algorithm and availability of contiguous space.
   */
  virtual int64_t allocate(uint64_t want_size, uint64_t alloc_unit,
			   uint64_t max_alloc_size, int64_t hint,
			   PExtentVector *extents) = 0;

  int64_t allocate(uint64_t want_size, uint64_t alloc_unit,
		   int64_t hint, PExtentVector *extents) {
    return allocate(want_size, alloc_unit, want_size, hint, extents);
  }

  /* Bulk release. Implementations may override this method to handle the whole
   * set at once. This could save e.g. unnecessary mutex dance. */
  virtual void release(const interval_set<uint64_t>& release_set) = 0;
  void release(const PExtentVector& release_set);

  virtual void dump() = 0;
  virtual void dump(std::function<void(uint64_t offset, uint64_t length)> notify) = 0;

  virtual void set_zone_states(std::vector<zone_state_t> &&_zone_states) {}
  virtual void init_add_free(uint64_t offset, uint64_t length) = 0;
  virtual void init_rm_free(uint64_t offset, uint64_t length) = 0;

  virtual uint64_t get_free() = 0;
  virtual double get_fragmentation()
  {
    return 0.0;
  }
  virtual double get_fragmentation_score();
  virtual void shutdown() = 0;

  static Allocator *create(CephContext* cct, std::string type, int64_t size,
			   int64_t block_size, const std::string& name = "");

  const string& get_name() const;

private:
  class SocketHook;
  SocketHook* asok_hook = nullptr;
};

#endif
