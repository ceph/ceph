// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <atomic>
#include <functional>
#include <ostream>
#include <utility>
#include <vector>
#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "common/ceph_mutex.h"

typedef interval_set<uint64_t> release_set_t;
typedef release_set_t::value_type release_set_entry_t;

// A batch of free extents as (offset, length) pairs, matching the
// (offset, length) convention used by foreach()/notify throughout the
// allocators. Returned via the out-param of Allocator::get_free_extents().
typedef std::vector<bluestore_interval_t<uint64_t, uint64_t>> free_extent_vector_t;

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
  virtual void release(const release_set_t& release_set) = 0;
  void release(const PExtentVector& release_set);

  virtual void dump() = 0;
  virtual void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) = 0;

  /*
   * Retrieves free extents from [range_begin, range_end) window. Up to max_count
   * extents are to be retrieved. 
   * 
   * Returned value is a cursor to pass as range_begin to the next call:
   *   - cursor >= range_end: the window is fully enumerated, stop.
   *   - cursor < range_end: pass it as range_begin to resume. The cursor is a
   *     valid resume point but is NOT guaranteed to land on a free extent; it
   *     may point into allocated space. An extra call that returns zero extents
   *     and advances cursor to range_end is possible when max_count is reached
   *     exactly at the last free extent in the window.
   *
   * Unlike foreach(), which holds the allocator lock for the entire walk, this
   * call takes the lock only for the duration of a single batch. The extents
   * might not be gloablly consistent between each lock. 
   *
   * Note, when allocator is idle the batches received via foreach() and
   * get_free_extents()  will give the same result.
   *
   * Consistency contract (read carefully):
   *   - The result is NOT a point-in-time snapshot of all the extents present
   *     on the disk. It's only a snapshot of the extents present in the range
   *     given to the function. Each batch observes the allocator state as of
   *     its own lock acquisition;
   *   - Extents within a single batch are mutually consistent and returned in
   *     ascending offset order, clipped to [range_begin, range_end).
   *
   * Therefore the union of all batches is an APPROXIMATION of the free space:
   * off by at most the extents concurrently allocated/released during
   * enumeration. 
   *
   * This is intended for statistical/best-effort consumers (e.g. fragmentation
   * scoring, defrag planning, emergency discard). For an exact, consistent
   * enumeration use foreach() instead.
   *
   * Caveats:
   *   - *out is appended to, not cleared. The caller owns
   *     preallocating/clearing/reusing. 
  *   - max_count bounds extents per batch, not bytes; passing 0 means
  *     unbounded for this call. A single returned extent may be arbitrarily
  *     large (it is the clipped span of one free extent).
   *   - The cursor is an opaque offset; do not assume it lands on a free
   *     extent or even on a block boundary. Only the >= range_end termination
   *     test is meaningful.
   *
   * Preconditions: 
  *    - range_begin <= range_end.
   */
  virtual uint64_t get_free_extents(
    uint64_t range_begin,
    uint64_t range_end,
    size_t max_count,
    free_extent_vector_t* out) = 0;

  /*
   * Weakly-consistent counterpart to foreach()
   * 
   * visit every free extent by driving get_free_extents() in bounded 
   * batches over [0, device_size), so the allocator lock is only held 
   * for one batch at a time instead of the whole walk. 
   * 
   * See get_free_extents() for the weak-consistency contract;
   * use foreach() when an exact, consistent snapshot is required.
   */
  void foreach_interruptible(
    std::function<void(uint64_t offset, uint64_t length)> notify);

  virtual void init_add_free(uint64_t offset, uint64_t length) = 0;
  virtual void init_rm_free(uint64_t offset, uint64_t length) = 0;

  virtual uint64_t get_free() = 0;
  virtual double get_fragmentation()
  {
    return 0.0;
  }
  virtual double get_fragmentation_score();
  virtual void shutdown() = 0;

  static Allocator *create(
    CephContext* cct,
    std::string_view type,
    int64_t size,
    int64_t block_size,
    const std::string_view name = ""
    );


  virtual const std::string& get_name() const = 0;
  int64_t get_capacity() const
  {
    return device_size.load();
  }
  int64_t get_block_size() const
  {
    return block_size;
  }
  virtual void expand(int64_t new_size){
    ceph_assert(new_size >= device_size.load());
    device_size.store(new_size);
  }

protected:
  std::atomic<int64_t> device_size{0};
  const int64_t block_size = 0;
};

#endif
