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
#include "common/ceph_mutex.h"

typedef interval_set<uint64_t> release_set_t;
typedef release_set_t::value_type release_set_entry_t;

class Allocator {
protected:

  /**
   * This is a base set of traits for logical placing entries
   * into limited collection of buckets depending on their sizes.
   * Descandants should implement get_bucket(len) method to obtain
   * bucket index using entry length.
   */
  struct LenPartitionedSetTraits {
    size_t num_buckets;
    size_t base_bits; // bits in min entry size
    size_t base;      // min entry size
    size_t factor;    // additional factor to be applied
                      // to entry size when calculating
                      // target bucket


    LenPartitionedSetTraits(size_t _num_buckets,
                            size_t _base_bits = 12,  //= 4096 bytes
                            size_t _factor = 1) :
      num_buckets(_num_buckets),
      base_bits(_base_bits),
      base(1ull << base_bits),
      factor(_factor)
    {
      ceph_assert(factor);
    }
  };

  /**
   * This extends LenPartitionedSetTraits to implement linear bucket indexing:
   * bucket index to be determined as entry's size divided by (base * factor),
   * i.e. buckets are:
   * [0..base)
   * [base, base+base*factor)
   * [base+base*factor, base+base*factor*2)
   * [base+base*factor*2, base+base*factor*3)
   * ...
   */
  struct LenPartitionedSetTraitsLinear : public LenPartitionedSetTraits {
    using LenPartitionedSetTraits::LenPartitionedSetTraits;
    /*
     * Determines bucket index for a given extent's length in a bucket set
     * with linear (len / base / factor) indexing.
     * The first bucket is targeted for lengths < base,
     * the last bucket is used for lengths above the maximum
     * detemined by bucket count.
     */
    inline size_t _get_bucket(uint64_t len) const {
      size_t idx = (len / factor) >> base_bits;
      idx = idx < num_buckets ? idx : num_buckets - 1;
      return idx;
    }
    /*
     * returns upper bound of a specific bucket
     */
    inline size_t _get_bucket_max(size_t bucket) const {
      return
        bucket < num_buckets - 1 ?
        base * factor * (1 + bucket) :
        std::numeric_limits<uint64_t>::max();
    }
  };

  /**
   * This extends LenPartitionedSetTraits to implement exponential bucket indexing:
   * target bucket bounds are determined as
   * [0, base]
   * (base, base*2^factor]
   * (base*2^factor, base*2^(factor*2)]
   * (base*2^(factor*2), base*2^(factor*3)]
   * ...
   *
   */
  struct LenPartitionedSetTraitsPow2 : public LenPartitionedSetTraits {
    /*
     * Determines bucket index for a given extent's length in a bucket collection
     * with log2(len) indexing.
     * The first bucket is targeted for lengths < base,
     * The last bucket index is used for lengths above the maximum
     * detemined by bucket count.
     */
    using LenPartitionedSetTraits::LenPartitionedSetTraits;
    inline size_t _get_bucket(uint64_t len) const {
      size_t idx;
      const size_t len_p2_max =
        base << ((factor * (num_buckets - 2)));
      if (len <= base) {
        idx = 0;
      } else if (len > len_p2_max) {
        idx = num_buckets - 1;
      } else {
        size_t most_bit = cbits(uint64_t(len - 1)) - 1;
        idx = 1 + ((most_bit - base_bits) / factor);
      }
      ceph_assert(idx < num_buckets);
      return idx;
    }
    /*
     * returns upper bound of the bucket with log2(len) indexing.
     */
    inline size_t _get_bucket_max(size_t bucket) const {
      return
        bucket < num_buckets - 1 ?
        base << (factor * bucket) :
        std::numeric_limits<uint64_t>::max();
    }
  };

  /*
   * Lockless stack implementation
   * that permits put/get operation exclusively
   * if no waiting is needed.
   * Conflicting operations are omitted.
   */
  class LocklessOpportunisticStack {
    std::atomic<size_t> ref = 0;
    std::atomic<size_t> count = 0;
    std::vector<uint64_t> data;
  public:
    void init(size_t size) {
      data.resize(size);
    }
    bool try_put(uint64_t& v) {
      bool done = ++ref == 1 && count < data.size();
      if (done) {
        data[count++] = v;
      }
      --ref;
      return done;
    }
    bool try_get(uint64_t& v) {
      bool done = ++ref == 1 && count > 0;
      if (done) {
        v = data[--count];
      }
      --ref;
      return done;
    }
    void foreach(std::function<void(uint64_t)> notify) {
      for (size_t i = 0; i < count; i++) {
        notify(data[i]);
      }
    }
  };
  /*
   * Concurrently accessed extent (offset,length) cache
   * which permits put/get operation exclusively if no waiting is needed.
   * Implemented via a set of independent buckets (aka LocklessOpportunisticStack).
   * Each bucket keeps extents of specific size only: 4K, 8K, 12K...64K
   * which allows to avoid individual extent size tracking.
   * Each bucket permits a single operation at a given time only,
   * additional operations against the bucket are rejected meaning relevant
   * extents aren't not cached.
   */
  class OpportunisticExtentCache {
    const LenPartitionedSetTraitsLinear myTraits;
    enum {
      BUCKET_COUNT = 16,
      EXTENTS_PER_BUCKET = 16, // amount of entries per single bucket,
                               // total amount of entries will be
                               // BUCKET_COUNT * EXTENTS_PER_BUCKET.
    };

    std::vector<LocklessOpportunisticStack> buckets;
    std::atomic<size_t> hits = 0;
    ceph::shared_mutex lock{
      ceph::make_shared_mutex(std::string(), false, false, false)
    };
  public:
    OpportunisticExtentCache() :
      myTraits(BUCKET_COUNT + 1), // 16 regular buckets + 1 "catch-all" pseudo
                                  // one to be used for out-of-bound checking
                                  // since _get_*_size_bucket() methods imply
                                  // the last bucket usage for the entries
                                  // exceeding the max length.
      buckets(BUCKET_COUNT)
    {
      //buckets.resize(BUCKET_COUNT);
      for(auto& b : buckets) {
        b.init(EXTENTS_PER_BUCKET);
      }
    }
    bool try_put(uint64_t offset, uint64_t len) {
      if (!lock.try_lock_shared()) {
        return false;
      }
      bool ret = false;
      ceph_assert(p2aligned(offset, myTraits.base));
      ceph_assert(p2aligned(len, myTraits.base));
      auto idx = myTraits._get_bucket(len);
      if (idx < buckets.size())
        ret = buckets[idx].try_put(offset);
      lock.unlock_shared();
      return ret;
    }
    bool try_get(uint64_t* offset, uint64_t len) {
      if (!lock.try_lock_shared()) {
        return false;
      }
      bool ret = false;
      ceph_assert(offset);
      ceph_assert(p2aligned(len, myTraits.base));
      size_t idx = len >> myTraits.base_bits;
      if (idx < buckets.size()) {
        ret = buckets[idx].try_get(*offset);
        if (ret) {
          ++hits;
        }
      }
      lock.unlock_shared();
      return ret;
    }
    size_t get_hit_count() const {
      return hits.load();
    }
    void foreach(std::function<void(uint64_t offset, uint64_t length)> notify) {
      std::unique_lock _lock(lock);
      for (uint64_t i = 0; i < buckets.size(); i++) {
        auto cb = [&](uint64_t o) {
          notify(o, i << myTraits.base_bits);
        };
        buckets[i].foreach(cb);
      }
    }
  };

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


  const std::string& get_name() const;
  int64_t get_capacity() const
  {
    return device_size;
  }
  int64_t get_block_size() const
  {
    return block_size;
  }

  // The following class implements Allocator's free extents histogram.
  // Which is a set of N buckets to track extents layout.
  // Extent matches a bucket depending on its length using the following
  // length spans:
  // [0..4K] (4K..16K] (16K..64K] .. (4M..16M] (16M..]
  // Each bucket tracks:
  // - total amount of extents of specific lengths
  // - amount of extents aligned with allocation boundary
  // - amount of allocation units in aligned extents
  //
  class FreeStateHistogram {
    const LenPartitionedSetTraitsPow2 myTraits;
    enum {
      BASE_BITS = 12, // 4096 bytes
      FACTOR = 2,
    };
    struct free_state_hist_bucket {
      size_t total = 0;
      size_t aligned = 0;
      size_t alloc_units = 0;
    };
    std::vector<free_state_hist_bucket> buckets;
  public:

    FreeStateHistogram(size_t num_buckets)
      : myTraits(num_buckets, BASE_BITS, FACTOR) {
      buckets.resize(num_buckets);
    }

    void record_extent(uint64_t alloc_unit, uint64_t off, uint64_t len);
    void foreach(
      std::function<void(uint64_t, uint64_t, uint64_t, uint64_t)> cb);
  };

  // The following code build Allocator's free extents spatial histogram.
  // Which is a set of N buckets to track extents location.
  // Extent lands into a bucket depending on its offset using N buckets.
  // An extent which spans multiple buckets lands into all of them
  // Each bucket tracks:
  // - amount of extents
  // - amount of bytes
  //
  struct free_state_spatial_hist_bucket {
    static const size_t base_bits = 12;
    static const size_t base = 1ull << base_bits;
    static const size_t mux = 2;

    uint64_t extents = 0;
    uint64_t bytes = 0;

    static size_t get_bucket(uint64_t offset,
                             size_t num_buckets,
                             uint64_t unit,
                             uint64_t total) {
      auto blen = p2roundup(total / num_buckets, unit);
      return offset / blen;
    }
    // returns starting offset of a bucket
    static uint64_t get_start(size_t bucket,
                              size_t num_buckets,
                              uint64_t unit,
                              uint64_t total) {
      ceph_assert (bucket <= num_buckets);
      auto blen = p2roundup(total / num_buckets, unit);
      return blen * bucket;
    };
    // returns ending offset (exclusive) of a bucket
    static uint64_t get_end(size_t bucket,
                            size_t num_buckets,
                            uint64_t unit,
                            uint64_t total) {
      ceph_assert (bucket < num_buckets);
      auto base = get_start(bucket + 1, num_buckets, unit, total);
      return std::min(base, total);
    };
  };

  typedef std::vector<free_state_spatial_hist_bucket> FreeStateSpatialHistogram;
  void build_free_state_spatial_histogram(FreeStateSpatialHistogram& hist);

private:
  class SocketHook;
  SocketHook* asok_hook = nullptr;
protected:
  const int64_t device_size = 0;
  const int64_t block_size = 0;
};

#endif