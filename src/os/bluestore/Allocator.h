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

class Allocator {
public:
  Allocator(std::string_view name,
	    int64_t _capacity,
	    int64_t _block_size);
  virtual ~Allocator();

  /*
  * returns allocator type name as per names in config
  */
  virtual const char* get_type() const = 0;

  /*
   * Allocate required number of blocks in n number of extents.
   * Min and Max number of extents are limited by:
   * a. alloc unit
   * b. max_alloc_size.
   * as no extent can be lesser than block_size and greater than max_alloc size.
   * Apart from that extents can vary between these lower and higher limits according
   * to free block search algorithm and availability of contiguous space.
   */
  virtual int64_t allocate(uint64_t want_size, uint64_t block_size,
			   uint64_t max_alloc_size, int64_t hint,
			   PExtentVector *extents) = 0;

  int64_t allocate(uint64_t want_size, uint64_t block_size,
		   int64_t hint, PExtentVector *extents) {
    return allocate(want_size, block_size, want_size, hint, extents);
  }

  /* Bulk release. Implementations may override this method to handle the whole
   * set at once. This could save e.g. unnecessary mutex dance. */
  virtual void release(const interval_set<uint64_t>& release_set) = 0;
  void release(const PExtentVector& release_set);

  virtual void dump() = 0;
  virtual void dump(std::function<void(uint64_t offset, uint64_t length)> notify) = 0;

  virtual void init_add_free(uint64_t offset, uint64_t length) = 0;
  virtual void init_rm_free(uint64_t offset, uint64_t length) = 0;

  virtual uint64_t get_free() = 0;
  virtual double get_fragmentation()
  {
    return 0.0;
  }
  virtual double get_fragmentation_score();
  virtual void shutdown() = 0;
  void prepare_for_fast_shutdown() {
    m_fast_shutdown = true;
  }

  static Allocator *create(
    CephContext* cct,
    std::string_view type,
    int64_t size,
    int64_t block_size,
    int64_t zone_size = 0,
    int64_t firs_sequential_zone = 0,
    const std::string_view name = ""
    );


  const std::string& get_name() const;
  int64_t get_capacity() const
  {
    return device_size;
  }
  int64_t get_block_size() const
  {
    return block_size;
  }

private:
  class SocketHook;
  SocketHook* asok_hook = nullptr;
protected:
  bool    m_fast_shutdown = false;
  const int64_t device_size = 0;
  const int64_t block_size = 0;
};

#endif
