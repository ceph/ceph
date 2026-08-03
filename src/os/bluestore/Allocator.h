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
#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "common/ceph_mutex.h"

typedef interval_set<uint64_t> release_set_t;
typedef release_set_t::value_type release_set_entry_t;

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

  /*
   * Reports whether this allocator implements claim_range().
   *
   * Support is all-or-nothing: an implementation that returns true must
   * handle *every* legal range. It is illegal to support claim_range() for
   * some ranges only - callers have no way to discover such a restriction
   * and would silently skip the unsupported ones.
   *
   * Defaults to false: an allocator that does not opt in fails safe.
   */
  virtual bool supports_claim_range() const {
    return false;
  }

  /*
   * Claim an exact range of disk space, whatever its current state.
   *
   * Unlike allocate(), where 'hint' is advisory, [offset, offset + length)
   * is binding: no space outside it is ever touched, and no space inside it
   * is ever skipped.
   * 
   * Returns, the number of bytes newly allocated by this call, Zero means
   * the range was fully allocated (or length was 0) and caller has nothing
   * to do.
   *
   * POSTCONDITION
   *   On return, every byte of [offset, offset + length) is allocated. The
   *   parts that were already allocated are left as they were; the parts
   *   that were free have been allocated by this call and are the ones
   *   reported back.
   *
   * OUTPUT
   *   Extents are appended to *extents, preserving existing entries as
   *   allocate() does; callers should pass a fresh vector. Returned extents are
   *   neither sorted nor coalesced. Extents are split at p2align(UINT32_MAX,
   *   block_size) because bluestore_pextent_t::length is 32-bit.
   *
   * PRECONDITIONS (asserted - these are caller bugs, not runtime states)
   *   - offset and length are multiples of block_size. 
   *   - offset + length does not exceed the device capacity.
   * 
   *   Note this differs from init_rm_free(), which asserts on *state* (the
   *   range not being free). claim_range() never asserts on state: any mix
   *   of free and allocated space in the range is legal input.
   *
   * ATOMICITY
   *   The whole operation runs under the allocator lock, so the range is
   *   fully allocated by the time this returns and no concurrent allocate()
   *   can hand out any part of it. This is why the interface exists at all:
   *   the equivalent foreach() + init_rm_free() sequence has a window
   *   between the two calls in which the free space can be given away.
   *
   * PERSISTENCE
   *   Space claimed here is not recorded in the FreelistManager. On an
   *   unclean shutdown it is simply free again after the allocator is
   *   rebuilt at mount, which is the desired behaviour for transient
   *   claims.
   *
   * INTENDED USE
   *   Temporarily taking back space that was released but not yet acted on
   *   - e.g. deferring BlockDevice::discard() past Allocator::release().
   *   The range may by then be free, fully re-allocated, or partly both;
   *   claim_range() pins whatever is still free so it cannot be handed out
   *   while the operation is in flight, and reports exactly that subset so
   *   the caller can release it again afterwards.
   */
  virtual int64_t claim_range(uint64_t offset, uint64_t length,
                              PExtentVector *extents) {
    ceph_abort_msg("claim_range() not implemented; "
                   "check supports_claim_range() first");
  }

  /* Bulk release. Implementations may override this method to handle the whole
   * set at once. This could save e.g. unnecessary mutex dance. */
  virtual void release(const release_set_t& release_set) = 0;
  void release(const PExtentVector& release_set);

  virtual void dump() = 0;
  virtual void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) = 0;

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
