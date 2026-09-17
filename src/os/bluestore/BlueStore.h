// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_H
#define CEPH_OSD_BLUESTORE_H

#include "acconfig.h"

#include <tuple>
#include <unistd.h>

#include <atomic>
#include <bit>
#include <chrono>
#include <ratio>
#include <mutex>
#include <queue>
#include <shared_mutex> // for std::shared_lock
#include <unordered_map>
#include <condition_variable>
#include <string>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/optional.hpp>
#include <utility>

#include "include/cpp-btree/btree_set.h"

#include "include/ceph_assert.h"
#include "include/interval_set.h"
#include "include/mempool.h"
#include "include/hash.h"
#include "common/bloom_filter.hpp"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "common/Throttle.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "compressor/Compressor.h"
#include "os/ObjectStore.h"

#include "bluestore_types.h"
#include "bluestore_common.h"
#include "BlueFS.h"
#include "BlueStore_objects_impl.h"
#include "common/EventTrace.h"
#include "common/admin_socket.h"
#ifdef WITH_CPUTRACE
#include "common/cputrace.h"
#endif

#ifdef WITH_BLKIN
#include "common/zipkin_trace.h"
#endif

class Allocator;
class FreelistManager;
class BlueStoreRepairer;
class SimpleBitmap;

//#define DEBUG_CACHE
//#define DEBUG_DEFERRED
#ifdef WITH_CPUTRACE
//change to #define to enable
#undef BLUESTORE_COMMON_CPUTRACE
#endif
// constants for Buffer::optimize()
#define MAX_BUFFER_SLOP_RATIO_DEN  8  // so actually 1/N
#define CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

// kv store prefixes
extern const std::string PREFIX_SUPER;
extern const std::string PREFIX_STAT;
extern const std::string PREFIX_COLL;
extern const std::string PREFIX_OBJ;
extern const std::string PREFIX_OMAP;
extern const std::string PREFIX_PGMETA_OMAP;
extern const std::string PREFIX_PERPOOL_OMAP;
extern const std::string PREFIX_PERPG_OMAP;
extern const std::string PREFIX_DEFERRED;
extern const std::string PREFIX_ALLOC;
extern const std::string PREFIX_ALLOC_BITMAP;
extern const std::string PREFIX_SHARED_BLOB;

extern const std::string BLUESTORE_GLOBAL_STATFS_KEY;

enum {
  l_bluestore_first = 732430,
  // space utilization stats
  //****************************************
  l_bluestore_allocated,
  l_bluestore_stored,
  l_bluestore_omap,
  l_bluestore_fragmentation,
  l_bluestore_alloc_unit,
  //****************************************

  // Update op processing state latencies
  //****************************************
  l_bluestore_state_prepare_lat,
  l_bluestore_state_aio_wait_lat,
  l_bluestore_state_io_done_lat,
  l_bluestore_state_kv_queued_lat,
  l_bluestore_state_kv_committing_lat,
  l_bluestore_state_kv_done_lat,
  l_bluestore_state_finishing_lat,
  l_bluestore_state_done_lat,

  l_bluestore_state_deferred_queued_lat,
  l_bluestore_state_deferred_aio_wait_lat,
  l_bluestore_state_deferred_cleanup_lat,

  l_bluestore_commit_lat,
  //****************************************

  // Update Transaction stats
  //****************************************
  l_bluestore_throttle_lat,
  l_bluestore_submit_lat,
  l_bluestore_txc,
  //****************************************

  // Read op stats
  //****************************************
  l_bluestore_read_onode_meta_lat,
  l_bluestore_read_wait_aio_lat,
  l_bluestore_csum_lat,
  l_bluestore_read_eio,
  l_bluestore_reads_with_retries,
  l_bluestore_read_lat,
  //****************************************

  // kv_thread latencies
  //****************************************
  l_bluestore_kv_flush_lat,
  l_bluestore_kv_commit_lat,
  l_bluestore_kv_sync_lat,
  l_bluestore_kv_final_lat,
  //****************************************

  // write op stats
  //****************************************
  l_bluestore_write_lat,
  l_bluestore_write_big,
  l_bluestore_write_big_bytes,
  l_bluestore_write_big_blobs,
  l_bluestore_write_big_deferred,

  l_bluestore_write_small,
  l_bluestore_write_small_bytes,
  l_bluestore_write_small_unused,
  l_bluestore_write_small_pre_read,

  l_bluestore_write_pad_bytes,
  l_bluestore_write_penalty_read_ops,
  l_bluestore_write_new,

  l_bluestore_issued_deferred_writes,
  l_bluestore_issued_deferred_write_bytes,
  l_bluestore_submitted_deferred_writes,
  l_bluestore_submitted_deferred_write_bytes,

  l_bluestore_write_big_skipped_blobs,
  l_bluestore_write_big_skipped_bytes,
  l_bluestore_write_small_skipped,
  l_bluestore_write_small_skipped_bytes,
  //****************************************

  // compressions stats
  //****************************************
  l_bluestore_compressed,
  l_bluestore_compressed_allocated,
  l_bluestore_compressed_original,
  l_bluestore_compress_lat,
  l_bluestore_decompress_lat,
  l_bluestore_compress_success_count,
  l_bluestore_compress_rejected_count,
  //****************************************

  // onode cache stats
  //****************************************
  l_bluestore_onodes,
  l_bluestore_pinned_onodes,
  l_bluestore_onode_hits,
  l_bluestore_onode_misses,
  l_bluestore_onode_shard_hits,
  l_bluestore_onode_shard_misses,
  l_bluestore_extents,
  l_bluestore_blobs,
  l_bluestore_spanning_blobs,
  l_bluestore_onode_miss_lat,  
  l_bluestore_onode_shard_miss_lat,           
  //****************************************

  // buffer cache stats
  //****************************************
  l_bluestore_buffers,
  l_bluestore_buffer_bytes,
  l_bluestore_buffer_hit_bytes,
  l_bluestore_buffer_miss_bytes,
  l_bluestore_buffer_miss_lat, ////cost per miss
  //****************************************

  // internal stats
  //****************************************
  l_bluestore_onode_reshard,
  l_bluestore_blob_split,
  l_bluestore_extent_compress,
  l_bluestore_gc_merged,
  //****************************************

  // misc
  //****************************************
  l_bluestore_omap_iterator_count,
  l_bluestore_omap_rmkeys_count,
  l_bluestore_omap_rmkey_ranges_count,
  l_bluestore_omap_setheader_count,
  l_bluestore_omap_setheader_bytes,
  l_bluestore_omap_setkeys_count,
  l_bluestore_omap_setkeys_records,
  l_bluestore_omap_setkeys_bytes,
  //****************************************

  // other client ops latencies
  //****************************************
  l_bluestore_omap_upper_bound_lat,
  l_bluestore_omap_lower_bound_lat,
  l_bluestore_omap_next_lat,
  l_bluestore_omap_get_keys_lat,
  l_bluestore_omap_get_values_lat,
  l_bluestore_omap_clear_lat,
  l_bluestore_clist_lat,
  l_bluestore_remove_lat,
  l_bluestore_truncate_lat,
  l_bluestore_exists_lat,
  l_bluestore_stat_lat,
  l_bluestore_getattr_lat,      // shared: getattr/getattrs
  l_bluestore_fiemap_lat,
  l_bluestore_omap_get_lat,
  l_bluestore_clone_lat,
  l_bluestore_change_attr_lat,      // shared: setattr/setattrs/rmattr/rmattrs
  l_bluestore_touch_lat,
  l_bluestore_zero_lat,
  l_bluestore_omap_set_lat,   // shared: omap setkeys/setheader/rmkeys/rmkey_range
  l_bluestore_rename_lat,
  l_bluestore_collection_lat,   // shared read: list/exists/bits
  l_bluestore_other_ops_lat,        // shared write: set_alloc_hint, set_collection_opts, collection create
  l_bluestore_split_collection_lat,
  l_bluestore_merge_collection_lat,
  l_bluestore_remove_collection_lat,
  //****************************************

  // allocation stats
  //****************************************
  l_bluestore_allocate_hist,
  l_bluestore_allocator_lat,
  //****************************************

  // slow op counter
  //****************************************
  l_bluestore_slow_aio_wait_count,
  l_bluestore_slow_committed_kv_count,
  l_bluestore_slow_read_onode_meta_count,
  l_bluestore_slow_read_wait_aio_count,
  l_bluestore_slow_op_normal_count,
  l_bluestore_slow_op_scrub_count,
  //****************************************

  // Fragmentation tracking
  //****************************************
  l_bluestore_runtime_frag_lat,
  l_bluestore_static_frag_lat,
  //****************************************
  l_bluestore_last
};

#define META_POOL_ID ((uint64_t)-1ull)
using bptr_c_it_t = buffer::ptr::const_iterator;

extern const std::vector<uint64_t> bdev_label_positions;

class BlueStore : public ObjectStore,
		  public md_config_obs_t {
  // -----------------------------------------------------
  // types
public:
  // config observer
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) override;

  //handler for discard event
  void handle_discard(interval_set<uint64_t>& to_release);

  void _set_csum();
  void _set_compression();
  void _set_throttle_params();
  int _set_cache_sizes();
  void _set_max_defer_interval() {
    max_defer_interval =
	cct->_conf.get_val<double>("bluestore_max_defer_interval");
  }

  typedef std::map<uint64_t, ceph::buffer::list> ready_regions_t;

  // aliases for types from bluestore namespace
  using Collection = bluestore::Collection;
  friend struct bluestore::Collection;
  using CollectionRef = bluestore::CollectionRef;

  using Blob = bluestore::Blob;
  using BlobRef = bluestore::BlobRef;

  using Onode = bluestore::Onode;
  friend struct bluestore::Onode;
  using OnodeRef = bluestore::OnodeRef;
  using OnodeSpace = bluestore::OnodeSpace;
  using OnodeCacheShard = bluestore::OnodeCacheShard;

  using Extent = bluestore::Extent;
  using ExtentMap = bluestore::ExtentMap;
  friend struct bluestore::ExtentMap;
  using OldExtent = bluestore::OldExtent;
  using OldExtentMap = bluestore::OldExtentMap;

  using SharedBlob = bluestore::SharedBlob;
  using SharedBlobRef = bluestore::SharedBlobRef;
  using SharedBlobSet = bluestore::SharedBlobSet;

  using TransContext = bluestore::TransContext;

  using DeferredBatch = bluestore::DeferredBatch;
  friend struct bluestore::DeferredBatch;

  using OpSequencer = bluestore::OpSequencer;
  using OpSequencerRef = bluestore::OpSequencerRef;
  using deferred_osr_queue_t = bluestore::deferred_osr_queue_t;

  using WriteContext = bluestore::WriteContext;

  using BigDeferredWriteContext = bluestore::BigDeferredWriteContext;

  using GarbageCollector = bluestore::GarbageCollector;

  using printer = bluestore::printer;
  ///////////////////////////////

  // BlueStore's forward declarations
  struct BufferSpace;

  class Scanner;

  class Estimator;

  struct BufferCacheShard;

  class Writer;
  friend class Writer;

  class SocketHook;
  friend class SocketHook;

  class Decoder_AllocationsAndStatFS;

  class ExtentDecoderPartial;
  /////////////////////////////

  Estimator* create_estimator();

  static constexpr uint32_t OBJECT_MAX_SIZE = 0xffffffff; // 32 bits
  /// cached buffer
  struct Buffer {
    MEMPOOL_CLASS_HELPERS();

    enum {
      STATE_EMPTY,     ///< empty buffer -- used for cache history
      STATE_CLEAN,     ///< clean data that is up to date
      STATE_WRITING,   ///< data that is being written (io not yet complete)
    };
    static const char *get_state_name(int s) {
      switch (s) {
      case STATE_EMPTY: return "empty";
      case STATE_CLEAN: return "clean";
      case STATE_WRITING: return "writing";
      default: return "???";
      }
    }
    // Short version of state name.
    // Not print "clean", as it is most frequent.
    static const char *get_state_name_short(int s) {
      switch (s) {
      case STATE_EMPTY: return ",empty";
      case STATE_CLEAN: return "";
      case STATE_WRITING: return ",writing";
      default: return "???";
      }
    }
    enum {
      FLAG_NOCACHE = 1,  ///< trim when done WRITING (do not become CLEAN)
      // NOTE: fix operator<< when you define a second flag
    };
    static const char *get_flag_name(int s) {
      switch (s) {
      case FLAG_NOCACHE: return "nocache";
      default: return "???";
      }
    }

    BufferSpace *space;
    uint16_t state;             ///< STATE_*
    uint16_t cache_private = 0; ///< opaque (to us) value used by Cache impl
    uint32_t flags;             ///< FLAG_*
    TransContext* txc;
    uint32_t offset, length;
    ceph::buffer::list data;
    std::shared_ptr<int64_t> cache_age_bin;  ///< cache age bin

    boost::intrusive::list_member_hook<> lru_item;
    boost::intrusive::set_member_hook<>  set_item;

    static std::atomic<uint64_t> total;

    Buffer(BufferSpace *space, unsigned s, TransContext* _txc,
           uint32_t o, uint32_t l, unsigned f = 0)
      : space(space), state(s), flags(f), txc(_txc), offset(o), length(l) { total++; }
    Buffer(BufferSpace *space, unsigned s, TransContext* _txc,
           uint32_t o, ceph::buffer::list& b, unsigned f = 0)
      : space(space), state(s), flags(f), txc(_txc), offset(o),
	length(b.length()), data(b) { total++; }
    Buffer(BufferSpace *space, unsigned s, TransContext* _txc,
           uint32_t o, ceph::buffer::list&& b, unsigned f = 0)
      : space(space), state(s), flags(f), txc(_txc), offset(o),
	length(b.length()), data(std::move(b)) { total++; }

    ~Buffer() { total--; }

    bool is_empty() const {
      return state == STATE_EMPTY;
    }
    bool is_clean() const {
      return state == STATE_CLEAN;
    }
    bool is_writing() const {
      return state == STATE_WRITING;
    }

    uint32_t end() const {
      return offset + length;
    }

    void truncate(uint32_t newlen) {
      ceph_assert(newlen < length);
      if (data.length()) {
	ceph::buffer::list t;
	t.substr_of(data, 0, newlen);
	data = std::move(t);
      }
      length = newlen;
    }
    void maybe_rebuild() {
      if (data.length() &&
	  (data.get_num_buffers() > 1 ||
	   data.front().wasted() > data.length() / MAX_BUFFER_SLOP_RATIO_DEN)) {
	data.rebuild();
      }
    }

    void dump(ceph::Formatter *f) const {
      f->dump_string("state", get_state_name(state));
      f->dump_unsigned("txc", (uint64_t)txc);
      f->dump_unsigned("offset", offset);
      f->dump_unsigned("length", length);
      f->dump_unsigned("data_length", data.length());
    }
    friend std::ostream& operator<<(std::ostream& out, const Buffer& b);
  };

  /// map logical extent range (object) onto buffers
  struct BufferSpace {
    enum {
      BYPASS_CLEAN_CACHE = 0x1,  // bypass clean cache
    };

    struct BufferKey {
      using type = uint32_t;
      const type &operator() (const Buffer& b) {
        return b.offset;
      }
    };
    typedef boost::intrusive::set<
      Buffer,
      boost::intrusive::member_hook<
        Buffer,
	boost::intrusive::set_member_hook<>,
	&Buffer::set_item>,
	boost::intrusive::key_of_value<BufferKey> > buffer_map_t;

    buffer_map_t buffer_map;

    Onode& onode;

    BufferSpace(Onode& _onode) : onode(_onode) {}
    ~BufferSpace() {
      ceph_assert(buffer_map.empty());
    }

    void _add_buffer(BufferCacheShard* cache,
                     Buffer* b,
                     uint16_t cache_private, int level, Buffer *near);

    void _rm_buffer(BufferCacheShard* cache,
                    Buffer* b) {
      ceph_assert(b->set_item.is_linked());
      __rm_buffer(cache, b);
    }
    void __rm_buffer(BufferCacheShard* cache, Buffer* b);
    void __erase_from_map(Buffer* b);

    buffer_map_t::iterator _data_lower_bound(uint32_t offset) {
      auto i = buffer_map.lower_bound(offset);
      if (i != buffer_map.begin()) {
	--i;
	if (i->offset + i->length <= offset)
	  ++i;
      }
      return i;
    }

    // must be called under protection of the Cache lock
    void _clear(BufferCacheShard* cache);

    // return value is the highest cache_private of a trimmed buffer, or 0.
    int discard(BufferCacheShard* cache,
                uint32_t offset, uint32_t length) {
      std::lock_guard l(cache->lock);
      int ret = _discard(cache, offset, length);
      cache->_trim();
      return ret;
    }
    int _discard(BufferCacheShard* cache,
                 uint32_t offset, uint32_t length);

    void write(BufferCacheShard* cache,
               TransContext* txc, uint32_t offset, ceph::buffer::list&& bl,
	       unsigned flags) {
      std::lock_guard l(cache->lock);
      uint16_t cache_private = _discard(cache, offset, bl.length());
      _add_buffer(cache,
                  new Buffer(this, Buffer::STATE_WRITING, txc, offset, std::move(bl), flags),
                  cache_private, (flags & Buffer::FLAG_NOCACHE) ? 0 : 1, nullptr);
      cache->_trim();
    }
    void write(BufferCacheShard* cache,
               TransContext* txc, uint32_t offset, ceph::buffer::list& bl,
	       unsigned flags) {
      std::lock_guard l(cache->lock);
      uint16_t cache_private = _discard(cache, offset, bl.length());
      _add_buffer(cache,
                  new Buffer(this, Buffer::STATE_WRITING, txc, offset, bl, flags),
                  cache_private, (flags & Buffer::FLAG_NOCACHE) ? 0 : 1,
                  nullptr);
      cache->_trim();
    }
    void _finish_write(BufferCacheShard* cache, TransContext* txc,
                       uint32_t offset, uint32_t length);
    void did_read(BufferCacheShard* cache,
                  uint32_t offset, ceph::buffer::list&& bl) {
      std::lock_guard l(cache->lock);
      uint16_t cache_private = _discard(cache, offset, bl.length());
      _add_buffer(
          cache,
          new Buffer(this, Buffer::STATE_CLEAN, 0, offset, std::move(bl), 0),
          cache_private, 1, nullptr);
      cache->_trim();
    }

    void read(BufferCacheShard* cache,
              uint32_t offset, uint32_t length,
	      BlueStore::ready_regions_t& res,
	      interval_set<uint32_t>& res_intervals,
	      int flags = 0);

    void truncate(BufferCacheShard* cache,
                  uint32_t offset) {
      discard(cache, offset, (uint32_t)-1 - offset);
    }

    void _dup_writing(TransContext* txc, Collection* collection, OnodeRef onode, uint32_t offset, uint32_t length);

    void dump(BufferCacheShard* cache, ceph::Formatter *f) const {
      std::lock_guard l(cache->lock);
      f->open_array_section("buffers");
      for (auto& b : buffer_map) {
	f->open_object_section("buffer");
	b.dump(f);
	f->close_section();
      }
      f->close_section();
    }
    friend std::ostream& operator<<(std::ostream& out, const BufferSpace& bc);
  };

  /// A generic Cache Shard
  struct CacheShard {
    CephContext *cct;
    PerfCounters *logger;

    /// protect lru and other structures
    ceph::recursive_mutex lock = {
      ceph::make_recursive_mutex("BlueStore::CacheShard::lock") };

    std::atomic<uint64_t> max = {0};
    std::atomic<uint64_t> num = {0};
    boost::circular_buffer<std::shared_ptr<int64_t>> age_bins;

    CacheShard(CephContext* cct) : cct(cct), logger(nullptr), age_bins(1) {
      shift_bins();
    }
    virtual ~CacheShard() {}

    void set_max(uint64_t max_) {
      max = max_;
      if (cct->_conf->bluestore_cache_meta_evict_in_autotune) {
        std::lock_guard l(lock);
        _trim_some();
      }
    }

    uint64_t _get_num() {
      return num;
    }

    virtual void _trim_to(uint64_t new_size) = 0;
    void _trim() {
      if (cct->_conf->objectstore_blackhole) {
	// do not trim if we are throwing away IOs a layer down
	return;
      }
      _trim_to(max);
    }
    void _trim_some() {
      int32_t max_steps = cct->_conf->bluestore_cache_meta_evict_limit;
      int64_t new_level = max.load();
      if (max_steps >= 2) {
        new_level = std::max((int64_t)num.load() - max_steps, new_level);
      }
      _trim_to(new_level);
    }
    void trim() {
      std::lock_guard l(lock);
      _trim();
    }
    void flush() {
      std::lock_guard l(lock);
      // we should not be shutting down after the blackhole is enabled
      ceph_assert(!cct->_conf->objectstore_blackhole);
      _trim_to(0);
    }

    void shift_bins() {
      std::lock_guard l(lock);
      age_bins.push_front(std::make_shared<int64_t>(0));
    }
    uint32_t get_bin_count() {
      std::lock_guard l(lock);
      return age_bins.capacity();
    }
    void set_bin_count(uint32_t count) {
      std::lock_guard l(lock);
      age_bins.set_capacity(count);
    }
    uint64_t sum_bins(uint32_t start, uint32_t end) {
      std::lock_guard l(lock);
      auto size = age_bins.size();
      if (size < start) {
        return 0;
      }
      uint64_t count = 0;
      end = (size < end) ? size : end;
      for (auto i = start; i < end; i++) {
        count += *(age_bins[i]);
      }
      return count;
    }

#ifdef DEBUG_CACHE
    virtual void _audit(const char *s) = 0;
#else
    void _audit(const char *s) { /* no-op */ }
#endif
  };

  /// A Generic buffer Cache Shard
  struct BufferCacheShard : public CacheShard {
    std::atomic<uint64_t> num_extents = {0};
    std::atomic<uint64_t> num_blobs = {0};
    uint64_t buffer_bytes = 0;
  public:
    BufferCacheShard(BlueStore* store)
      : CacheShard(store->cct) {
    }
    virtual ~BufferCacheShard() {
      ceph_assert(num_blobs == 0);
      ceph_assert(num_extents == 0);
    }
    static BufferCacheShard *create(BlueStore* store, std::string type,
                                    PerfCounters *logger);
    virtual void _add(Buffer *b, int level, Buffer *near) = 0;
    virtual void _rm(Buffer *b) = 0;
    virtual void _move(BufferCacheShard *src, Buffer *b) = 0;
    virtual void _touch(Buffer *b) = 0;
    virtual void _adjust_size(Buffer *b, int64_t delta) = 0;

    uint64_t _get_bytes() {
      return buffer_bytes;
    }

    void add_extent() {
      ++num_extents;
    }
    void rm_extent() {
      --num_extents;
    }

    void add_blob() {
      ++num_blobs;
    }
    void rm_blob() {
      --num_blobs;
    }

    virtual void add_stats(uint64_t *extents,
                           uint64_t *blobs,
                           uint64_t *buffers,
                           uint64_t *bytes) = 0;

    bool empty() {
      std::lock_guard l(lock);
      return _get_bytes() == 0;
    }
  };

  class BlueStoreThrottle {
#if defined(WITH_LTTNG)
    const std::chrono::time_point<ceph::mono_clock> time_base = ceph::mono_clock::now();

    // Time of last chosen io (microseconds)
    std::atomic<uint64_t> previous_emitted_tp_time_mono_mcs = {0};
    std::atomic<uint64_t> ios_started_since_last_traced = {0};
    std::atomic<uint64_t> ios_completed_since_last_traced = {0};

    std::atomic_uint pending_kv_ios = {0};
    std::atomic_uint pending_deferred_ios = {0};

    // Min period between trace points (microseconds)
    std::atomic<uint64_t> trace_period_mcs = {0};

    bool should_trace(
      uint64_t *started,
      uint64_t *completed) {
      uint64_t min_period_mcs = trace_period_mcs.load(
	std::memory_order_relaxed);

      if (min_period_mcs == 0) {
	*started = 1;
	*completed = ios_completed_since_last_traced.exchange(0);
	return true;
      } else {
	ios_started_since_last_traced++;
	auto now_mcs = ceph::to_microseconds<uint64_t>(
	  ceph::mono_clock::now() - time_base);
	uint64_t previous_mcs = previous_emitted_tp_time_mono_mcs;
	uint64_t period_mcs = now_mcs - previous_mcs;
	if (period_mcs > min_period_mcs) {
	  if (previous_emitted_tp_time_mono_mcs.compare_exchange_strong(
		previous_mcs, now_mcs)) {
	    // This would be racy at a sufficiently extreme trace rate, but isn't
	    // worth the overhead of doing it more carefully.
	    *started = ios_started_since_last_traced.exchange(0);
	    *completed = ios_completed_since_last_traced.exchange(0);
	    return true;
	  }
	}
	return false;
      }
    }
#endif

#if defined(WITH_LTTNG)
    void emit_initial_tracepoint(
      KeyValueDB &db,
      TransContext &txc,
      ceph::mono_clock::time_point);
#else
    void emit_initial_tracepoint(
      KeyValueDB &db,
      TransContext &txc,
      ceph::mono_clock::time_point) {}
#endif

    Throttle throttle_bytes;           ///< submit to commit
    Throttle throttle_deferred_bytes;  ///< submit to deferred complete

  public:
    ceph::mutex lock = ceph::make_mutex("BlueStoreThrottle::max_lock");

    std::atomic<uint64_t> transactions = 0;

    int64_t  bytes_observed_max = 0;
    utime_t  bytes_max_ts;
    uint64_t transactions_observed_max = 0;
    utime_t  transactions_max_ts;

    uint64_t get_current() {
      return throttle_bytes.get_current();
    }

  public:
    BlueStoreThrottle(CephContext *cct) :
      throttle_bytes(cct, "bluestore_throttle_bytes", 0),
      throttle_deferred_bytes(cct, "bluestore_throttle_deferred_bytes", 0)
    {
      reset_throttle(cct->_conf);
    }

#if defined(WITH_LTTNG)
    void complete_kv(TransContext &txc);
    void complete(TransContext &txc);
#else
    void complete_kv(TransContext &txc) {}
    void complete(TransContext &txc) {}
#endif

    ceph::mono_clock::duration log_state_latency(
      TransContext &txc, PerfCounters *logger, int state);
    bool try_start_transaction(
      KeyValueDB &db,
      TransContext &txc,
      ceph::mono_clock::time_point);
    void finish_start_transaction(
      KeyValueDB &db,
      TransContext &txc,
      ceph::mono_clock::time_point);
    void release_kv_throttle(uint64_t cost, uint64_t txcs) {
      throttle_bytes.put(cost);
      transactions -= txcs;
    }
    void release_deferred_throttle(uint64_t cost) {
      throttle_deferred_bytes.put(cost);
    }
    bool should_submit_deferred() {
      return throttle_deferred_bytes.past_midpoint();
    }
    void reset_throttle(const ConfigProxy &conf) {
      throttle_bytes.reset_max(conf->bluestore_throttle_bytes);
      throttle_deferred_bytes.reset_max(
	conf->bluestore_throttle_bytes +
	conf->bluestore_throttle_deferred_bytes);
#if defined(WITH_LTTNG)
      double rate = conf.get_val<double>("bluestore_throttle_trace_rate");
      trace_period_mcs = rate > 0 ? std::floor((1/rate) * 1000000.0) : 0;
#endif
    }
  } throttle;

  struct KVSyncThread : public Thread {
    BlueStore *store;
    explicit KVSyncThread(BlueStore *s) : store(s) {}
    void *entry() override {
      store->_kv_sync_thread();
      return NULL;
    }
  };
  struct KVFinalizeThread : public Thread {
    BlueStore *store;
    explicit KVFinalizeThread(BlueStore *s) : store(s) {}
    void *entry() override {
      store->_kv_finalize_thread();
      return NULL;
    }
  };

  // --------------------------------------------------------
  // members
private:
  BlueFS *bluefs = nullptr;
  bluefs_layout_t bluefs_layout;
  utime_t next_dump_on_bluefs_alloc_failure;

  KeyValueDB *db = nullptr;
  BlockDevice *bdev = nullptr;
  std::string freelist_type;
  FreelistManager *fm = nullptr;

  std::string ebd_health_alert; ///< used to report ExtBlkDev plugin problem up the health chain

  Allocator *alloc = nullptr;   ///< allocator consumed by BlueStore
  bluefs_shared_alloc_context_t shared_alloc; ///< consumed by BlueFS (may be == alloc)

  uuid_d fsid;
  int path_fd = -1;  ///< open handle to $path
  int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
  bool mounted = false;

  // Whether a caller may tolerate undecodable onodes during allocation recovery
  enum class alloc_recovery_policy_t {
    strict,
    tolerate_corrupt_onodes,
  };

  // store open_db options:
  bool db_was_opened_read_only = true;
  bool need_to_destage_allocation_file = false;

  alloc_recovery_policy_t alloc_recovery_policy = alloc_recovery_policy_t::strict;
  std::atomic<uint64_t> alloc_recovery_skipped_onodes = {0};
  bool _alloc_recovery_tolerates_corruption() const;

  ///< rwlock to protect coll_map/new_coll_map
  ceph::shared_mutex coll_lock = ceph::make_shared_mutex("BlueStore::coll_lock");
  mempool::bluestore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
  bool collections_had_errors = false;
  std::map<coll_t,CollectionRef> new_coll_map;

  mempool::bluestore_cache_buffer::vector<BufferCacheShard*> buffer_cache_shards;
  mempool::bluestore_cache_onode::vector<OnodeCacheShard*> onode_cache_shards;

public:
  struct CacheStatsSnapshot {
    using tavg_t = std::pair<uint64_t, uint64_t>; // that's in fact a result of PerfCounters::get_tavg_ns

    ceph::mono_time timestamp;
    uint64_t onode_hits;
    uint64_t onode_misses;
    tavg_t onode_miss_latency;
    uint64_t onode_shard_hits;
    uint64_t onode_shard_misses;
    tavg_t onode_shard_miss_latency;
    uint64_t buffer_hit_bytes;
    uint64_t buffer_miss_bytes;
    tavg_t buffer_miss_latency;

    CacheStatsSnapshot()
      : timestamp(ceph::mono_clock::zero()),
        onode_hits(0), onode_misses(0), onode_miss_latency({0,0}),
        onode_shard_hits(0), onode_shard_misses(0), onode_shard_miss_latency({0,0}),
        buffer_hit_bytes(0), buffer_miss_bytes(0), buffer_miss_latency({0,0}) {}

    CacheStatsSnapshot delta(const CacheStatsSnapshot& older) const {
      auto sub = [](uint64_t a, uint64_t b) { return a >= b ? a - b : 0; };
      auto sub_tavg = [&](const tavg_t& a, const tavg_t& b) { return tavg_t(sub(a.first, b.first), sub(a.second, b.second)); };
      CacheStatsSnapshot d;
      d.timestamp = timestamp;
      d.onode_hits = sub(onode_hits, older.onode_hits);
      d.onode_misses = sub(onode_misses, older.onode_misses);
      d.onode_miss_latency = sub_tavg(onode_miss_latency, older.onode_miss_latency);
      d.onode_shard_hits = sub(onode_shard_hits, older.onode_shard_hits);
      d.onode_shard_misses = sub(onode_shard_misses, older.onode_shard_misses);
      d.onode_shard_miss_latency = sub_tavg(onode_shard_miss_latency, older.onode_shard_miss_latency);
      d.buffer_hit_bytes = sub(buffer_hit_bytes, older.buffer_hit_bytes);
      d.buffer_miss_bytes = sub(buffer_miss_bytes, older.buffer_miss_bytes);
      d.buffer_miss_latency = sub_tavg(buffer_miss_latency, older.buffer_miss_latency);
      return d;
    }
  };

private:
  ceph::mutex cache_stats_lock = ceph::make_mutex("BlueStore::cache_stats_lock");
  std::deque<CacheStatsSnapshot> cache_stats_snapshots;
  static constexpr size_t MAX_CACHE_SNAPSHOTS = 5;

  /// take a fresh cache-stats snapshot and return it alongside the
  /// most-recent and oldest snapshots retained for short/long term tracking
  void get_snapshot_windows(CacheStatsSnapshot& current,
                            CacheStatsSnapshot& most_recent,
                            CacheStatsSnapshot& oldest);


  /// protect zombie_osr_set
  ceph::mutex zombie_osr_lock = ceph::make_mutex("BlueStore::zombie_osr_lock");
  uint32_t next_sequencer_id = 0;
  std::map<coll_t,OpSequencerRef> zombie_osr_set; ///< std::set of OpSequencers for deleted collections

  std::atomic<uint64_t> nid_last = {0};
  std::atomic<uint64_t> nid_max = {0};
  std::atomic<uint64_t> blobid_last = {0};
  std::atomic<uint64_t> blobid_max = {0};

  ceph::mutex deferred_lock = ceph::make_mutex("BlueStore::deferred_lock");
  ceph::mutex atomic_alloc_and_submit_lock =
      ceph::make_mutex("BlueStore::atomic_alloc_and_submit_lock");
  std::atomic<uint64_t> deferred_seq = {0};
  std::unique_ptr<deferred_osr_queue_t> deferred_queue; ///< osr's with deferred io pending
  std::atomic_int deferred_queue_size = {0};         ///< num txc's queued across all osrs
  std::atomic_int deferred_aggressive = {0}; ///< aggressive wakeup of kv thread
  Finisher  finisher;
  utime_t  deferred_last_submitted = utime_t();

  KVSyncThread kv_sync_thread;
  ceph::mutex kv_lock = ceph::make_mutex("BlueStore::kv_lock");
  ceph::condition_variable kv_cond;
  bool _kv_only = false;
  bool kv_sync_started = false;
  bool kv_stop = false;
  bool kv_finalize_started = false;
  bool kv_finalize_stop = false;
  std::deque<TransContext*> kv_queue;             ///< ready, already submitted
  std::deque<TransContext*> kv_queue_unsubmitted; ///< ready, need submit by kv thread
  std::deque<TransContext*> kv_committing;        ///< currently syncing
  std::deque<DeferredBatch*> deferred_done_queue;   ///< deferred ios done
  bool kv_sync_in_progress = false;

  KVFinalizeThread kv_finalize_thread;
  ceph::mutex kv_finalize_lock = ceph::make_mutex("BlueStore::kv_finalize_lock");
  ceph::condition_variable kv_finalize_cond;
  std::deque<TransContext*> kv_committing_to_finalize;   ///< pending finalization
  std::deque<DeferredBatch*> deferred_stable_to_finalize; ///< pending finalization
  bool kv_finalize_in_progress = false;

  PerfCounters *logger = nullptr;

  std::list<CollectionRef> removed_collections;

  ceph::shared_mutex debug_read_error_lock =
    ceph::make_shared_mutex("BlueStore::debug_read_error_lock");
  std::set<ghobject_t> debug_data_error_objects;
  std::set<ghobject_t> debug_mdata_error_objects;

  std::atomic<int> csum_type = {Checksummer::CSUM_CRC32C};

  uint64_t block_size = 0;     ///< block size of block device (power of 2)
  uint64_t block_mask = 0;     ///< mask to get just the block offset
  size_t block_size_order = 0; ///< bits to shift to get block size
  uint64_t optimal_io_size = 0;///< best performance io size for block device

  uint64_t min_alloc_size;     ///< minimum allocation unit (power of 2)
  uint8_t  min_alloc_size_order = 0;///< bits to shift to get min_alloc_size
  uint64_t min_alloc_size_mask;///< mask for fast checking of allocation alignment
  static_assert(std::numeric_limits<uint8_t>::max() >
		std::numeric_limits<decltype(min_alloc_size)>::digits,
		"not enough bits for min_alloc_size");
  bool elastic_shared_blobs = false; ///< use smart ExtentMap::dup to reduce shared blob count
  bool use_write_v2 = false; ///< use new write path
  bool debug_extent_map_encode_check = false;

  enum {
    // Please preserve the order since it's DB persistent
    OMAP_BULK = 0,
    OMAP_PER_POOL = 1,
    OMAP_PER_PG = 2,
    } per_pool_omap = OMAP_BULK;

  ///< maximum allocation unit (power of 2)
  std::atomic<uint64_t> max_alloc_size = {0};

  ///< number threshold for forced deferred writes
  std::atomic<int> deferred_batch_ops = {0};

  ///< size threshold for forced deferred writes
  std::atomic<uint64_t> prefer_deferred_size = {0};

  ///< approx cost per io, in bytes
  std::atomic<uint64_t> throttle_cost_per_io = {0};

  std::atomic<Compressor::CompressionMode> comp_mode =
    {Compressor::COMP_NONE}; ///< compression mode
  std::atomic<int> def_compressor_alg = {Compressor::COMP_ALG_NONE};
  std::vector<CompressorRef> compressors;
  std::atomic<uint64_t> comp_min_blob_size = {0};
  std::atomic<uint64_t> comp_max_blob_size = {0};

  std::atomic<uint64_t> max_blob_size = {0};  ///< maximum blob size
  std::atomic<uint32_t> segment_size = {0}; ///< snapshot of conf value "bluestore_onode_segment_size"
                                            /// When 0 onode_bluestore_t v2 is in force, otherwise v3 is used.
                                            /// Ability to disable is important for efficient testing.

  uint64_t kv_ios = 0;
  uint64_t kv_throttle_costs = 0;
  uint64_t kv_throttle_txcs = 0;

  // cache trim control
  uint64_t cache_size = 0;       ///< total cache size
  double cache_meta_ratio = 0;   ///< cache ratio dedicated to metadata
  double cache_kv_ratio = 0;     ///< cache ratio dedicated to kv (e.g., rocksdb)
  double cache_kv_onode_ratio = 0; ///< cache ratio dedicated to kv onodes (e.g., rocksdb onode CF)
  double cache_data_ratio = 0;   ///< cache ratio dedicated to object data
  bool cache_autotune = false;   ///< cache autotune setting
  double cache_age_bin_interval = 0; ///< time to wait between cache age bin rotations
  double cache_autotune_interval = 0; ///< time to wait between cache rebalancing
  std::vector<uint64_t> kv_bins; ///< kv autotune bins
  std::vector<uint64_t> kv_onode_bins; ///< kv onode autotune bins
  std::vector<uint64_t> meta_bins; ///< meta autotune bins
  std::vector<uint64_t> data_bins; ///< data autotune bins
  uint64_t osd_memory_target = 0;   ///< OSD memory target when autotuning cache
  uint64_t osd_memory_base = 0;     ///< OSD base memory when autotuning cache
  double osd_memory_expected_fragmentation = 0; ///< expected memory fragmentation
  uint64_t osd_memory_cache_min = 0; ///< Min memory to assign when autotuning cache
  double osd_memory_cache_resize_interval = 0; ///< Time to wait between cache resizing 
  double max_defer_interval = 0; ///< Time to wait between last deferred submit
  std::atomic<uint32_t> config_changed = {0}; ///< Counter to determine if there is a configuration change.

  // caching of bdev_label
  bluestore_bdev_label_t bdev_label;                 // this value is valid if
  std::vector<uint64_t>  bdev_label_valid_locations; // this has any elements
  bool bdev_label_multi = false;
  int64_t bdev_label_epoch = -1;
  bool bluestore_bdev_label_require_all = false;
  uint64_t before_expansion_bdev_size = 0; // having non-zero indicates we need
                                           // to expand allocator in NCB mode,
                                           // perhaps could be removed when
                                           // https://tracker.ceph.com/issues/70008
                                           // is resolved.

  typedef std::map<uint64_t, volatile_statfs> osd_pools_map;

  ceph::mutex vstatfs_lock = ceph::make_mutex("BlueStore::vstatfs_lock");
  volatile_statfs vstatfs;
  osd_pools_map osd_pools; // protected by vstatfs_lock as well

  bool per_pool_stat_collection = true;

  AdminSocketHook* asok_hook = nullptr;

  bool use_last_allocator_lookup_position = true;

  struct MempoolThread : public Thread {
  public:
    BlueStore *store;

    ceph::condition_variable cond;
    ceph::mutex lock = ceph::make_mutex("BlueStore::MempoolThread::lock");
    bool stop = false;
    std::shared_ptr<PriorityCache::PriCache> binned_kv_cache = nullptr;
    std::shared_ptr<PriorityCache::PriCache> binned_kv_onode_cache = nullptr;
    std::shared_ptr<PriorityCache::Manager> pcm = nullptr;

    struct MempoolCache : public PriorityCache::PriCache {
      BlueStore *store;
      uint64_t bins[PriorityCache::Priority::LAST+1] = {0};
      int64_t cache_bytes[PriorityCache::Priority::LAST+1] = {0};
      int64_t committed_bytes = 0;
      double cache_ratio = 0;

      MempoolCache(BlueStore *s) : store(s) {};

      virtual uint64_t _get_used_bytes() const = 0;
      virtual uint64_t _sum_bins(uint32_t start, uint32_t end) const = 0;

      virtual int64_t request_cache_bytes(
          PriorityCache::Priority pri, uint64_t total_cache) const {
        int64_t assigned = get_cache_bytes(pri);

        switch (pri) {
        case PriorityCache::Priority::PRI0:
	  {
            // BlueStore caches currently don't put anything in PRI0
	    break;
	  }
        case PriorityCache::Priority::LAST:
          {
            uint32_t max = get_bin_count();
	    int64_t request = _get_used_bytes() - _sum_bins(0, max);
            return(request > assigned) ? request - assigned : 0;
          }
        default:
	  {
	    ceph_assert(pri > 0 && pri < PriorityCache::Priority::LAST);
            auto prev_pri = static_cast<PriorityCache::Priority>(pri - 1);
            uint64_t start = get_bins(prev_pri);
            uint64_t end = get_bins(pri);
            int64_t request = _sum_bins(start, end);
            return(request > assigned) ? request - assigned : 0;
	  }
	}
        return -EOPNOTSUPP;
      }
 
      virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
        return cache_bytes[pri];
      }
      virtual int64_t get_cache_bytes() const { 
        int64_t total = 0;

        for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
          PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
          total += get_cache_bytes(pri);
        }
        return total;
      }
      virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
        cache_bytes[pri] = bytes;
      }
      virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
        cache_bytes[pri] += bytes;
      }
      virtual int64_t commit_cache_size(uint64_t total_cache) {
        committed_bytes = PriorityCache::get_chunk(
            get_cache_bytes(), total_cache);
        return committed_bytes;
      }
      virtual int64_t get_committed_size() const {
        return committed_bytes;
      }
      virtual uint64_t get_bins(PriorityCache::Priority pri) const {
        if (pri > PriorityCache::Priority::PRI0 &&
            pri < PriorityCache::Priority::LAST) {
          return bins[pri];
        }
        return 0;
      }
      virtual void set_bins(PriorityCache::Priority pri, uint64_t end_bin) {
        if (pri <= PriorityCache::Priority::PRI0 ||
            pri >= PriorityCache::Priority::LAST) {
          return;
        }
        bins[pri] = end_bin;
        uint64_t max = 0;
        for (int pri = 1; pri < PriorityCache::Priority::LAST; pri++) {
          if (bins[pri] > max) {
            max = bins[pri];
          }
        }
        set_bin_count(max);
      }
      virtual void import_bins(const std::vector<uint64_t> &bins_v) {
        uint64_t max = 0;
        for (int pri = 1; pri < PriorityCache::Priority::LAST; pri++) {
          unsigned i = (unsigned) pri - 1;
          if (i < bins_v.size()) {
            bins[pri] = bins_v[i];
            if (bins[pri] > max) {
              max = bins[pri];
            }
          } else {
            bins[pri] = 0;
          }
        }
        set_bin_count(max);
      }
      virtual double get_cache_ratio() const {
        return cache_ratio;
      }
      virtual void set_cache_ratio(double ratio) {
        cache_ratio = ratio;
      }
      virtual std::string get_cache_name() const = 0;
      virtual uint32_t get_bin_count() const = 0;
      virtual void set_bin_count(uint32_t count) = 0;
    };

    struct MetaCache : public MempoolCache {
      MetaCache(BlueStore *s) : MempoolCache(s) {};

      virtual uint32_t get_bin_count() const;
      virtual void set_bin_count(uint32_t count);
      virtual uint64_t _get_used_bytes() const {
        return mempool::bluestore_blob::allocated_bytes() +
          mempool::bluestore_extent::allocated_bytes() +
          mempool::bluestore_cache_buffer::allocated_bytes() +
          mempool::bluestore_cache_meta::allocated_bytes() +
          mempool::bluestore_cache_other::allocated_bytes() +
	   mempool::bluestore_cache_onode::allocated_bytes() +
          mempool::bluestore_shared_blob::allocated_bytes() +
          mempool::bluestore_inline_bl::allocated_bytes();
      }
      virtual void shift_bins();
      virtual uint64_t _sum_bins(uint32_t start, uint32_t end) const;
      virtual std::string get_cache_name() const {
        return "BlueStore Meta Cache";
      }
      uint64_t _get_num_onodes() const {
        uint64_t onode_num =
            mempool::bluestore_cache_onode::allocated_items();
        return (2 > onode_num) ? 2 : onode_num;
      }
      double get_bytes_per_onode() const {
        return (double)_get_used_bytes() / (double)_get_num_onodes();
      }
    };
    std::shared_ptr<MetaCache> meta_cache;

    struct DataCache : public MempoolCache {
      DataCache(BlueStore *s) : MempoolCache(s) {};

      virtual uint32_t get_bin_count() const {
        return store->buffer_cache_shards[0]->get_bin_count();
      }
      virtual void set_bin_count(uint32_t count) {
        for (auto i : store->buffer_cache_shards) {
          i->set_bin_count(count);
        }
      }
      virtual uint64_t _get_used_bytes() const {
        uint64_t bytes = 0;
        for (auto i : store->buffer_cache_shards) {
          bytes += i->_get_bytes();
        }
        return bytes; 
      }
      virtual void shift_bins() {
        for (auto i : store->buffer_cache_shards) {
          i->shift_bins();
        }
      }
      virtual uint64_t _sum_bins(uint32_t start, uint32_t end) const {
        uint64_t bytes = 0;
        for (auto i : store->buffer_cache_shards) {
          bytes += i->sum_bins(start, end);
        }
        return bytes;
      }
      virtual std::string get_cache_name() const {
        return "BlueStore Data Cache";
      }
    };
    std::shared_ptr<DataCache> data_cache;

  public:
    explicit MempoolThread(BlueStore *s)
      : store(s),
        meta_cache(new MetaCache(s)),
        data_cache(new DataCache(s)) {}

    void *entry() override;
    void init() {
      ceph_assert(stop == false);
      create("bstore_mempool");
    }
    void shutdown() {
      lock.lock();
      stop = true;
      cond.notify_all();
      lock.unlock();
      join();
    }

  private:
    void _update_cache_settings();
    void _resize_shards(bool interval_stats);

    mono_clock::time_point last_fragmentation_check;
  } mempool_thread;

#ifdef WITH_BLKIN
  ZTracer::Endpoint trace_endpoint {"0.0.0.0", 0, "BlueStore"};
#endif

  // --------------------------------------------------------
  // private methods

  void _init_logger();
  void _shutdown_logger();
  int _reload_logger();

  int _open_path();
  void _close_path();
  int _open_fsid(bool create);
  int _lock_fsid();
  int _read_fsid(uuid_d *f);
  int _write_fsid();
  void _close_fsid();
  void _set_alloc_sizes();
  void _set_blob_size();
  void _set_finisher_num();
  void _set_per_pool_omap();
  void _update_osd_memory_options();
  void _update_allocator_lookup_policy();

  int _open_bdev(bool create);
  // Verifies if disk space is enough for reserved + min bluefs
  // and alters the latter if needed.
  // Depends on min_alloc_size hence should be called after
  // its initialization (and outside of _open_bdev)
  void _validate_bdev();
  void _close_bdev();

  int _minimal_open_bluefs(bool create);
  void _minimal_close_bluefs();
  int _open_bluefs(bool create, bool read_only);
  void _close_bluefs();

  int _is_bluefs(bool create, bool* ret);
  /*
  * opens both DB and dependant super_meta, FreelistManager and allocator
  * in the proper order
  */
  int _open_db_and_around(bool read_only, bool to_repair = false,
            alloc_recovery_policy_t policy = alloc_recovery_policy_t::strict);
  void _close_db_and_around();
  void _close_around_db();

  int _prepare_db_environment(bool create, bool read_only,
			      std::string* kv_dir, std::string* kv_backend);

  /*
   * @warning to_repair_db means that we open this db to repair it, will not
   * hold the rocksdb's file lock.
   */
  int _open_db(bool create,
	       bool to_repair_db=false,
	       bool read_only = false);
  void _close_db();
  int _open_fm(KeyValueDB::Transaction t,
               bool read_only,
               bool db_avail,
               bool fm_restore = false);
  void _close_fm();
  int _write_out_fm_meta(uint64_t target_size);
  int _create_alloc();
  int _init_alloc();
  void _post_init_alloc();
  void _close_alloc();
  int _open_collections();
  void _fsck_collections(int64_t* errors);
  void _close_collections();

  int _setup_block_symlink_or_file(std::string name, std::string path, uint64_t size,
				   bool create);

public:
  utime_t get_deferred_last_submitted() {
    std::lock_guard l(deferred_lock);
    return deferred_last_submitted;
  }
private:
  static int _write_bdev_label(
    CephContext* cct,
    BlockDevice* bdev,
    const std::string &path,
    bluestore_bdev_label_t label,
    std::vector<uint64_t> locations = std::vector<uint64_t>({BDEV_FIRST_LABEL_POSITION}));
  static int _read_bdev_label(
    CephContext* cct, BlockDevice* bdev, const std::string &path,
    bluestore_bdev_label_t *label, uint64_t disk_position = BDEV_FIRST_LABEL_POSITION);
  int _check_or_set_bdev_label(BlockDevice* bdev, const std::string& path,
                               const std::string& desc, bool create);
  int _set_main_bdev_label();
  int _check_main_bdev_label();
  static int _read_multi_bdev_label(
    CephContext* cct,
    BlockDevice* bdev,
    const std::string& path,
    uuid_d fsid,
    bluestore_bdev_label_t *out_label,
    std::vector<uint64_t>* out_valid_positions = nullptr,
    bool* out_is_multi = nullptr,
    int64_t* out_epoch = nullptr);
  void _main_bdev_label_try_reserve();
  void _main_bdev_label_remove(Allocator* alloc);

  int _open_super_meta();

  void _open_statfs();
  void _get_statfs_overall(struct store_statfs_t *buf);

  void _dump_alloc_on_failure();

  CollectionRef _get_collection(const coll_t& cid);
  CollectionRef _get_collection_by_oid(const ghobject_t& oid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();

  void _assign_nid(TransContext *txc, OnodeRef& o);
  uint64_t _assign_blobid(TransContext *txc);

  friend void _dump_transaction(CephContext *cct, Transaction *t);

  TransContext *_txc_create(Collection *c, OpSequencer *osr,
			    std::list<Context*> *on_commits,
			    TrackedOpRef osd_op=TrackedOpRef());
  void _txc_update_store_statfs(TransContext *txc);
  void _txc_add_transaction(TransContext *txc, Transaction *t);
  void _txc_calc_cost(TransContext *txc);
  void _txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t);
  void _txc_state_proc(TransContext *txc);
  void _txc_aio_submit(TransContext *txc);
public:
  void txc_aio_finish(void *p) {
    _txc_state_proc(static_cast<TransContext*>(p));
  }
private:
  void _txc_finish_io(TransContext *txc);
  void _txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t);
  void _txc_apply_kv(TransContext *txc, bool sync_submit_transaction);
  void _txc_committed_kv(TransContext *txc);
  void _txc_finish(TransContext *txc);
  void _txc_release_alloc(TransContext *txc);

  void _osr_attach(Collection *c);
  void _osr_register_zombie(OpSequencer *osr);
  void _osr_drain(OpSequencer *osr);
  void _osr_drain_preceding(TransContext *txc);
  void _osr_drain_all();

  void _kv_start();
  void _kv_stop();
  void _kv_sync_thread();
  void _kv_finalize_thread();

  bluestore_deferred_op_t *_get_deferred_op(TransContext *txc, uint64_t len);
  void _deferred_queue(TransContext *txc);
public:
  void deferred_try_submit();
private:
  void _deferred_submit_unlock(OpSequencer *osr);
  void _deferred_aio_finish(OpSequencer *osr);
  int _deferred_replay();
  bool _eliminate_outdated_deferred(bluestore_deferred_transaction_t* deferred_txn,
				    interval_set<uint64_t>& bluefs_extents);

public:
  using mempool_dynamic_bitset =
    boost::dynamic_bitset<uint64_t,
			  mempool::bluestore_fsck::pool_allocator<uint64_t>>;
  using  per_pool_statfs =
    mempool::bluestore_fsck::map<uint64_t, store_statfs_t>;

  struct pool_fsck_stats_t {
    uint64_t num_objects = 0;
    uint64_t shared_blobs = 0;
    uint64_t omaps = 0;
    uint64_t omap_key_size = 0;
    uint64_t omap_val_size = 0;
    uint64_t stored = 0;
    uint64_t allocated = 0;

    void add(const pool_fsck_stats_t& other) {
      num_objects += other.num_objects;
      shared_blobs += other.shared_blobs;
      omaps += other.omaps;
      omap_key_size += other.omap_key_size;
      omap_val_size += other.omap_val_size;
      stored += other.stored;
      allocated += other.allocated;
    }
    friend std::ostream& operator<<(std::ostream& out, const pool_fsck_stats_t& s);
  };
  using  per_pool_fsck_stats_t =
    mempool::bluestore_fsck::map<int64_t, pool_fsck_stats_t>; // pool_id -> stats

  enum FSCKDepth {
    FSCK_REGULAR,
    FSCK_DEEP,
    FSCK_SHALLOW
  };
  enum {
    MAX_FSCK_ERROR_LINES = 100,
  };

private:
  int _fsck_check_extents(
    std::string_view ctx_descr,
    const PExtentVector& extents,
    bool compressed,
    mempool_dynamic_bitset &used_blocks,
    uint64_t granularity,
    BlueStoreRepairer* repairer,
    store_statfs_t& expected_statfs,
    pool_fsck_stats_t& pool_fsck_stat,
    FSCKDepth depth);

  void _fsck_check_statfs(
    const store_statfs_t& expected_store_statfs,
    const per_pool_statfs& expected_pool_statfs,
    int64_t& errors,
    int64_t &warnings,
    BlueStoreRepairer* repairer);
  // When cb returns false stops iterating.
  void _fsck_foreach_shared_blob(
    std::function< bool (coll_t, ghobject_t, uint64_t, const bluestore_blob_t&)> cb);
  void _fsck_repair_shared_blobs(
    BlueStoreRepairer& repairer,
    shared_blob_2hash_tracker_t& sb_ref_counts,
    sb_info_space_efficient_map_t& sb_info);

  int _fsck(FSCKDepth depth, bool repair, bluestore_stats_t *store_stats = nullptr);
  int _fsck_on_open(BlueStore::FSCKDepth depth, bool repair, bluestore_stats_t *store_stats);

  void _buffer_cache_write(
    TransContext *txc,
    OnodeRef onode,
    uint32_t offset,
    ceph::buffer::list&& bl,
    unsigned flags);

  void _buffer_cache_write(
    TransContext *txc,
    OnodeRef onode,
    uint32_t offset,
    ceph::buffer::list& bl,
    unsigned flags);

  int _collection_list(
    Collection *c, const ghobject_t& start, const ghobject_t& end,
    int max, bool legacy, std::vector<ghobject_t> *ls, ghobject_t *next);

  template <typename T, typename F>
  T select_option(const std::string& opt_name, T val1, F f) {
    //NB: opt_name reserved for future use
    std::optional<T> val2 = f();
    if (val2) {
      return *val2;
    }
    return val1;
  }

  void _apply_padding(uint64_t head_pad,
		      uint64_t tail_pad,
		      ceph::buffer::list& padded);

  void _record_onode(OnodeRef &o, KeyValueDB::Transaction &txn);

  // -- ondisk version ---
public:
  const int32_t latest_ondisk_format = 4;        ///< our version
  const int32_t min_readable_ondisk_format = 1;  ///< what we can read
  const int32_t min_compat_ondisk_format = 3;    ///< who can read us

private:
  int32_t ondisk_format = 0;  ///< value detected on mount
  bool    m_fast_shutdown = false;
  int _upgrade_super();  ///< upgrade (called during open_super)
  uint64_t _get_ondisk_reserved() const;
  void _prepare_ondisk_format_super(KeyValueDB::Transaction& t);

  // --- public interface ---
public:
  BlueStore(CephContext *cct, const std::string& path);
  BlueStore(CephContext *cct, const std::string& path, uint64_t min_alloc_size); // Ctor for UT only
  ~BlueStore() override;

  std::string get_type() override {
    return "bluestore";
  }

  bool needs_journal() override { return false; };
  bool wants_journal() override { return false; };
  bool allows_journal() override { return false; };

  void prepare_for_fast_shutdown() override;
  bool has_null_manager() const override;

  uint64_t get_min_alloc_size() const override {
    return min_alloc_size;
  }

  int get_devices(std::set<std::string> *ls) override;

  bool is_rotational() override;
  bool is_journal_rotational() override;
  bool is_db_rotational();
  bool is_statfs_recoverable() const;

  std::string get_default_device_class() override {
    std::string device_class;
    std::map<std::string, std::string> metadata;
    collect_metadata(&metadata);
    auto it = metadata.find("bluestore_bdev_type");
    if (it != metadata.end()) {
      device_class = it->second;
    }
    return device_class;
  }

  int get_numa_node(
    int *numa_node,
    std::set<int> *nodes,
    std::set<std::string> *failed) override;

  static int get_block_device_fsid(CephContext* cct, const std::string& path,
				   uuid_d *fsid);

  bool test_mount_in_use() override;

private:
  int _mount();
  int _mount_readonly();
  int _umount_readonly();

public:
  int mount_readonly() override;
  int umount_readonly() override;

  int mount() override {
    return _mount();
  }
  int umount() override;

  int open_db_environment(KeyValueDB **pdb, bool read_only, bool to_repair);
  int close_db_environment();
  BlueFS* get_bluefs();

  int write_meta(const std::string& key, const std::string& value) override;
  int read_meta(const std::string& key, std::string *value) override;
  int read_meta_check(const std::string& key, std::string *value);

  int read_meta_conf_check_env();

  // open in read-only and limited mode
  int cold_open();
  int cold_close();

  int fsck(bool deep) override {
    return _fsck(deep ? FSCK_DEEP : FSCK_REGULAR, false);
  }
  int repair(bool deep) override {
    return _fsck(deep ? FSCK_DEEP : FSCK_REGULAR, true);
  }
  int revert_wal_to_plain();
  int quick_fix() override {
    return _fsck(FSCK_SHALLOW, true);
  }

  void set_cache_shards(unsigned num) override;
  void dump_cache_stats(ceph::Formatter *f) override;
  void dump_cache_stats(std::ostream& ss) override;

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;
  int mkjournal() override {
    return 0;
  }

  void get_db_statistics(ceph::Formatter *f) override;
  void generate_db_histogram(ceph::Formatter *f) override;
  void _shutdown_cache();
  int flush_cache(std::ostream *os = NULL) override;
  void dump_perf_counters(ceph::Formatter *f) override {
    f->open_object_section("perf_counters");
    logger->dump_formatted(f, false, select_labeled_t::unlabeled);
    f->close_section();
  }

  int add_new_bluefs_device(int id, const std::string& path);
  int migrate_to_existing_bluefs_device(const std::set<int>& devs_source,
    int id);
  int migrate_to_new_bluefs_device(const std::set<int>& devs_source,
    int id,
    const std::string& path);
  int expand_devices(std::ostream& out);
  std::string get_device_path(unsigned id);

  bool get_db_sharding(std::string& res_sharding);

  int dump_bluefs_sizes(std::ostream& out);
  void trim_free_space(const std::string& type, std::ostream& outss);
  static int zap_device(CephContext* cct, const std::string& dev);


public:
  int fsck_with_stats(bool deep, bluestore_stats_t &store_stats) {
    return _fsck(deep ? FSCK_DEEP : FSCK_REGULAR, false, &store_stats);
  }
  int statfs(struct store_statfs_t *buf,
             osd_alert_list_t* alerts = nullptr) override;
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
		  bool *per_pool_omap) override;

  void collect_metadata(std::map<std::string,std::string> *pm) override;

  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int set_collection_opts(
    CollectionHandle& c,
    const pool_opts_t& opts) override;
  int stat(
    CollectionHandle &c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0) override;

private:

  // --------------------------------------------------------
  // intermediate data structures used while reading
  struct region_t {
    uint64_t logical_offset;
    uint64_t blob_xoffset;   //region offset within the blob
    uint64_t length;

    // used later in read process
    uint64_t front = 0;

    region_t(uint64_t offset, uint64_t b_offs, uint64_t len, uint64_t front = 0)
      : logical_offset(offset),
      blob_xoffset(b_offs),
      length(len),
      front(front){}
    region_t(const region_t& from)
      : logical_offset(from.logical_offset),
      blob_xoffset(from.blob_xoffset),
      length(from.length),
      front(from.front){}

    friend std::ostream& operator<<(std::ostream& out, const region_t& r) {
      return out << "0x" << std::hex << r.logical_offset << ":"
        << r.blob_xoffset << "~" << r.length << std::dec;
    }
  };

  // merged blob read request
  struct read_req_t {
    uint64_t r_off = 0;
    uint64_t r_len = 0;
    ceph::buffer::list bl;
    std::list<region_t> regs; // original read regions

    read_req_t(uint64_t off, uint64_t len) : r_off(off), r_len(len) {}

    friend std::ostream& operator<<(std::ostream& out, const read_req_t& r) {
      out << "{<0x" << std::hex << r.r_off << ", 0x" << r.r_len << "> : [";
      for (const auto& reg : r.regs)
        out << reg;
      return out << "]}" << std::dec;
    }
  };

  typedef std::list<read_req_t> regions2read_t;
  typedef std::map<BlueStore::BlobRef, regions2read_t> blobs2read_t;

  void _read_cache(
    OnodeRef& o,
    uint64_t offset,
    size_t length,
    int read_cache_policy,
    ready_regions_t& ready_regions,
    blobs2read_t& blobs2read);


  int _prepare_read_ioc(
    blobs2read_t& blobs2read,
    std::vector<ceph::buffer::list>* compressed_blob_bls,
    IOContext* ioc);

  int _generate_read_result_bl(
    OnodeRef& o,
    uint64_t offset,
    size_t length,
    ready_regions_t& ready_regions,
    std::vector<ceph::buffer::list>& compressed_blob_bls,
    blobs2read_t& blobs2read,
    bool buffered,
    bool* csum_error,
    ceph::buffer::list& bl);

  void _measure_runtime_frag(Collection *c, const blobs2read_t& blobs2read);

  void _measure_static_frag(Collection *c, const OnodeRef& o);

  int _do_read(
    Collection *c,
    OnodeRef& o,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0,
    uint64_t retry_count = 0);

  void _do_read_and_pad(
    Collection* c,
    OnodeRef& o,
    uint32_t offset,
    uint32_t length,
    ceph::buffer::list& bl);

  int _do_readv(
    Collection *c,
    OnodeRef& o,
    const interval_set<uint64_t>& m,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0,
    uint64_t retry_count = 0);

  int _fiemap(CollectionHandle &c_, const ghobject_t& oid,
	      uint64_t offset, size_t len, interval_set<uint64_t>& destset);
public:
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, ceph::buffer::list& bl) override;
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap) override;

  int readv(
    CollectionHandle &c_,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    ceph::buffer::list& bl,
    uint32_t op_flags) override;

  int dump_onode(CollectionHandle &c, const ghobject_t& oid,
    const std::string& section_name, ceph::Formatter *f) override;

  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      ceph::buffer::ptr& value) override;

  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       std::map<std::string,ceph::buffer::ptr, std::less<>>& aset) override;

  int list_collections(std::vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t &c) override;
  CollectionHandle create_new_collection(const coll_t& cid) override;
  void set_collection_commit_queue(const coll_t& cid,
				   ContextQueue *commit_queue) override;

  bool collection_exists(const coll_t& c) override;
  int collection_empty(CollectionHandle& c, bool *empty) override;
  int collection_bits(CollectionHandle& c) override;

  int collection_list(CollectionHandle &c,
		      const ghobject_t& start,
		      const ghobject_t& end,
		      int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next) override;

  int collection_list_legacy(CollectionHandle &c,
                             const ghobject_t& start,
                             const ghobject_t& end,
                             int max,
                             std::vector<ghobject_t> *ls,
                             ghobject_t *next) override;

  int omap_get(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
    ) override;
  int _omap_get(
    Collection *c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
    );
  int _onode_omap_get(
    const OnodeRef& o,           ///< [in] Object containing omap
    ceph::buffer::list *header,          ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
  );


  /// Get omap header
  int omap_get_header(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  /// Get key values
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::set<std::string> &keys,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  int omap_iterate(
    CollectionHandle &c,   ///< [in] collection
    const ghobject_t &oid, ///< [in] object
    omap_iter_seek_t start_from, ///< [in] where the iterator should point to at the beginning
    std::function<omap_iter_ret_t(std::string_view, std::string_view)> f
  ) override;

  void set_fsid(uuid_d u) override {
    fsid = u;
  }
  uuid_d get_fsid() override {
    return fsid;
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return num_objects * 300; //assuming per-object overhead is 300 bytes
  }

  struct BSPerfTracker {
    PerfCounters::avg_tracker<uint64_t> os_commit_latency_ns;
    PerfCounters::avg_tracker<uint64_t> os_apply_latency_ns;

    objectstore_perf_stat_t get_cur_stats() const {
      objectstore_perf_stat_t ret;
      ret.os_commit_latency_ns = os_commit_latency_ns.current_avg();
      ret.os_apply_latency_ns = os_apply_latency_ns.current_avg();
      return ret;
    }

    void update_from_perfcounters(PerfCounters &logger);
  } perf_tracker;

  objectstore_perf_stat_t get_cur_stats() override {
    perf_tracker.update_from_perfcounters(*logger);
    return perf_tracker.get_cur_stats();
  }
  const PerfCounters* get_perf_counters() const override {
    return logger;
  }
  void refresh_perf_counters() override;

  const PerfCounters* get_bluefs_perf_counters() const {
    return bluefs->get_perf_counters();
  }
  KeyValueDB* get_kv() {
    return db;
  }
  BlockDevice* get_bdev() {
    return bdev;
  }

  int queue_transactions(
    CollectionHandle& ch,
    std::vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;

  // error injection
  void inject_data_error(const ghobject_t& o) override {
    std::unique_lock l(debug_read_error_lock);
    debug_data_error_objects.insert(o);
  }
  void inject_mdata_error(const ghobject_t& o) override {
    std::unique_lock l(debug_read_error_lock);
    debug_mdata_error_objects.insert(o);
  }

  /// methods to inject various errors fsck can repair
  int get_shared_blob(const std::string& key,
		       ceph::buffer::list& bl);
  void inject_broken_shared_blob_key(const std::string& key,
			 const ceph::buffer::list& bl);
  void inject_no_shared_blob_key();
  void inject_stray_shared_blob_key(uint64_t sbid);

  void inject_leaked(uint64_t len);
  void inject_false_free(coll_t cid, ghobject_t oid);
  void inject_statfs(const std::string& key, const store_statfs_t& new_statfs);
  void inject_global_statfs(const store_statfs_t& new_statfs);
  void inject_misreference(coll_t cid1, ghobject_t oid1,
			   coll_t cid2, ghobject_t oid2,
			   uint64_t offset);
  void inject_zombie_spanning_blob(coll_t cid, ghobject_t oid, int16_t blob_id);
  // resets global per_pool_omap in DB
  void inject_legacy_omap();
  // resets per_pool_omap | pgmeta_omap for onode
  void inject_legacy_omap(coll_t cid, ghobject_t oid);
  void inject_stray_omap(uint64_t head, const std::string& name);

  void inject_bluefs_file(std::string_view dir,
			  std::string_view name,
			  size_t new_size);

  int compact() override;
  bool has_builtin_csum() const override {
    return true;
  }

  static int debug_write_bdev_label(
    CephContext* cct, BlockDevice* bdev, const std::string &path,
    const bluestore_bdev_label_t& label, uint64_t disk_position) {
      return _write_bdev_label(cct, bdev, path, label,
        std::vector<uint64_t>({disk_position}));
    }
  static int read_bdev_label_at_pos(
    CephContext* cct,
    const std::string &bdev_path,
    uint64_t disk_position,
    bluestore_bdev_label_t *label);
  static int read_bdev_label(
    CephContext* cct,
    const std::string &path,
    bluestore_bdev_label_t *out_label,
    std::vector<uint64_t>* out_valid_positions = nullptr,
    bool* out_is_multi = nullptr,
    int64_t* out_epoch = nullptr);
  static int write_bdev_label(
    CephContext* cct, const std::string &path,
    const bluestore_bdev_label_t& label, uint64_t disk_position = 0);

  Allocator*& debug_get_alloc() {
    return alloc;
  }
  void debug_set_block_size(uint64_t _block_size) {
    block_size = _block_size;
    block_mask = ~(block_size - 1);
    block_size_order = std::countr_zero(block_size);
  }
  void debug_set_prefer_deferred_size(uint64_t s) {
    prefer_deferred_size = s;
  }
  OnodeRef debug_get_onode(const coll_t& cid, const ghobject_t& hoid);

  // a debug punch_hole function, to use internals of _wctx_finish
  // to remove old_extents from object
  void debug_punch_hole(
    CollectionRef& c,
    OnodeRef& o,
    uint32_t off,
    uint32_t len);
  void debug_punch_hole_2(
    CollectionRef& c,
    OnodeRef& o,
    uint32_t offset,
    uint32_t length,
    PExtentVector& released,
    std::vector<BlobRef>& pruned_blobs,
    std::set<SharedBlobRef>& shared_changed,
    volatile_statfs& statfs_delta);

  inline void log_latency(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    const char* info = "",
    int idx2 = l_bluestore_first);

  inline void log_latency_scrub(const char* name,
    int idx,
    const ceph::timespan& l,
    double lat_threshold,
    const char* info = "",
    int idx2 = l_bluestore_first);

  inline void log_latency_fn(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    std::function<std::string (const ceph::timespan& lat)> fn,
    int idx2 = l_bluestore_first);

  inline void log_latency_fn_scrub(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    std::function<std::string (const ceph::timespan& lat)> fn,
    int idx2 = l_bluestore_first);

private:
  bool _debug_data_eio(const ghobject_t& o) {
    if (!cct->_conf->bluestore_debug_inject_read_err) {
      return false;
    }
    std::shared_lock l(debug_read_error_lock);
    return debug_data_error_objects.count(o);
  }
  bool _debug_mdata_eio(const ghobject_t& o) {
    if (!cct->_conf->bluestore_debug_inject_read_err) {
      return false;
    }
    std::shared_lock l(debug_read_error_lock);
    return debug_mdata_error_objects.count(o);
  }
  void _debug_obj_on_delete(const ghobject_t& o) {
    if (cct->_conf->bluestore_debug_inject_read_err) {
      std::unique_lock l(debug_read_error_lock);
      debug_data_error_objects.erase(o);
      debug_mdata_error_objects.erase(o);
    }
  }
private:
  ceph::mutex qlock = ceph::make_mutex("BlueStore::Alerts::qlock");
  std::string failed_cmode;
  std::set<std::string> failed_compressors;
  std::string spillover_alert;
  std::string legacy_statfs_alert;
  std::string no_per_pool_omap_alert;
  std::string no_per_pg_omap_alert;
  std::string disk_size_mismatch_alert;
  std::string spurious_read_errors_alert;
  std::string no_db_sharding_alert;
  std::string legacy_min_alloc_size_alert;
  std::queue <std::pair<ceph::mono_clock::time_point, bool>> slow_op_event_queue;
  size_t slow_op_event_count = 0;
  size_t slow_scrub_op_event_count = 0;

  std::pair<size_t, size_t> _trim_slow_op_event_queue(ceph::mono_clock::time_point cur_time);
  void _add_slow_op_event();
  void _add_slow_scrub_op_event();
  void _log_alerts(osd_alert_list_t& alerts);
  bool _set_compression_alert(bool cmode, const char* s) {
    std::lock_guard l(qlock);
    if (cmode) {
      bool ret = failed_cmode.empty();
      failed_cmode = s;
      return ret;
    }
    return failed_compressors.emplace(s).second;
  }
  void _clear_compression_alert() {
    std::lock_guard l(qlock);
    failed_compressors.clear();
    failed_cmode.clear();
  }

  void _check_legacy_statfs_alert();
  void _check_no_per_pg_or_pool_omap_alert();
  void _check_no_db_sharding_alert();
  uint64_t _get_default_min_alloc_size();
  void _check_legacy_min_alloc_size_alert();
  void _set_disk_size_mismatch_alert(const std::string& s) {
    std::lock_guard l(qlock);
    disk_size_mismatch_alert = s;
  }
  void _set_spurious_read_errors_alert(const std::string& s) {
    std::lock_guard l(qlock);
    spurious_read_errors_alert = s;
  }

private:

  // --------------------------------------------------------
  // read processing internal methods
  int _verify_csum(
    OnodeRef& o,
    const bluestore_blob_t* blob,
    uint64_t blob_xoffset,
    const ceph::buffer::list& bl,
    uint64_t logical_offset);
  int _decompress(ceph::buffer::list& source, ceph::buffer::list* result);


  // --------------------------------------------------------
  // write ops
  private:
  void _do_write_small(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    ceph::buffer::list::iterator& blp,
    WriteContext *wctx);

  /// Determines if small write can reuse existing blob
  /// and hence omit blob relocation.
  /// Returns the amount of remaining bytes which need relocation,
  /// effectively the possibe return values are for now:
  /// * 0 - blob has been reused and writing has been staged
  /// * min_alloc_size - no writing staged, blob to be relocated.
  uint32_t _do_write_small_with_maybe_blob_reuse(
    TransContext* txc,
    CollectionRef& c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    bufferlist& bl,
    WriteContext* wctx);
  void _do_write_big_apply_deferred(
    TransContext* txc,
    CollectionRef& c,
    OnodeRef& o,
    BigDeferredWriteContext& dctx,
    bufferlist::iterator& blp,
    WriteContext* wctx);
  void _do_write_big(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    ceph::buffer::list::iterator& blp,
    WriteContext *wctx);
  int _do_alloc_write(
    TransContext *txc,
    CollectionRef c,
    OnodeRef& o,
    WriteContext *wctx);
  void _wctx_finish(
    TransContext *txc,
    CollectionRef& c,
    OnodeRef& o,
    WriteContext *wctx,
    std::set<SharedBlob*> *maybe_unshared_blobs=0);

  int _write(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o,
	     uint64_t offset, size_t len,
	     ceph::buffer::list& bl,
	     uint32_t fadvise_flags);
  void _pad_zeros(ceph::buffer::list *bl, uint64_t *offset,
		  uint64_t chunk_size);

  void _choose_write_options(CollectionRef& c,
                             OnodeRef& o,
                             uint32_t fadvise_flags,
                             WriteContext *wctx);

  int _do_gc(TransContext *txc,
             CollectionRef& c,
             OnodeRef& o,
             const WriteContext& wctx,
             uint64_t *dirty_start,
             uint64_t *dirty_end);

  int _do_write(TransContext *txc,
		CollectionRef &c,
		OnodeRef& o,
		uint64_t offset, uint64_t length,
		ceph::buffer::list& bl,
		uint32_t fadvise_flags);
  void _do_write_data(TransContext *txc,
                      CollectionRef& c,
                      OnodeRef& o,
                      uint64_t offset,
                      uint64_t length,
                      ceph::buffer::list& bl,
                      WriteContext *wctx);
  int _do_write_v2(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    ceph::buffer::list& bl,
    uint32_t fadvise_flags);
  int _do_write_v2_compressed(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    WriteContext& wctx,
    uint32_t offset, uint32_t length,
    ceph::buffer::list& bl,
    uint32_t scan_left, uint32_t scan_right);
  int _touch(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o);
  int _do_zero(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       uint64_t offset, size_t len);
  int _zero(TransContext *txc,
	    CollectionRef& c,
	    OnodeRef& o,
	    uint64_t offset, size_t len);
  void _do_truncate(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   uint64_t offset,
		   std::set<SharedBlob*> *maybe_unshared_blobs=0);
  int _truncate(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		uint64_t offset);
  int _remove(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o);
  int _do_remove(TransContext *txc,
		 CollectionRef& c,
		 OnodeRef& o);
  int _maybe_unshare_on_remove(TransContext *txc,
                               CollectionRef& c,
                               OnodeRef& head_o,
                               std::set<SharedBlob*>&& maybe_unshared_blobs);
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       const std::string& name,
	       ceph::buffer::list& val);
  int _setattrs(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		const std::map<std::string,ceph::buffer::ptr>& aset);
  int _rmattr(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o,
	      const std::string& name);
  int _rmattrs(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o);
  void _do_omap_clear(TransContext *txc, OnodeRef& o);
  int _omap_clear(TransContext *txc,
		  CollectionRef& c,
		  OnodeRef& o);
  int _omap_setkeys(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    ceph::buffer::list& bl);
  int _omap_setheader(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      ceph::buffer::list& header);
  int _omap_rmkeys(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   ceph::buffer::list& bl);
  int _omap_rmkey_range(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const std::string& first, const std::string& last);
  int _set_alloc_hint(
    TransContext *txc,
    CollectionRef& c,
    OnodeRef& o,
    uint64_t expected_object_size,
    uint64_t expected_write_size,
    uint32_t flags);
  int _do_clone_range(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& oldo,
		      OnodeRef& newo,
		      uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _clone(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& oldo,
	     OnodeRef& newo);
  int _clone_range(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& oldo,
		   OnodeRef& newo,
		   uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _rename(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& oldo,
	      OnodeRef& newo,
	      const ghobject_t& new_oid);
  int _create_collection(TransContext *txc, const coll_t &cid,
			 unsigned bits, CollectionRef *c);
  int _remove_collection(TransContext *txc, const coll_t &cid,
                         CollectionRef *c);
  void _do_remove_collection(TransContext *txc, CollectionRef *c);
  int _split_collection(TransContext *txc,
			CollectionRef& c,
			CollectionRef& d,
			unsigned bits, int rem);
  int _merge_collection(TransContext *txc,
			CollectionRef *c,
			CollectionRef& d,
			unsigned bits);

  void _collect_allocation_stats(uint64_t need, uint32_t alloc_size,
                                 const PExtentVector&);
  void _record_allocation_stats();
private:
  uint64_t probe_count = 0;
  std::atomic<uint64_t> alloc_stats_count = {0};
  std::atomic<uint64_t> alloc_stats_fragments = { 0 };
  std::atomic<uint64_t> alloc_stats_size = { 0 };
  // 
  std::array<std::tuple<uint64_t, uint64_t, uint64_t>, 5> alloc_stats_history =
  { std::make_tuple(0ul, 0ul, 0ul) };

  bool _is_main_rotational();
  inline bool _use_rotational_settings();

public:
  typedef btree::btree_set<
    uint64_t, std::less<uint64_t>,
    mempool::bluestore_fsck::pool_allocator<uint64_t>> uint64_t_btree_t;

  struct FSCK_ObjectCtx {
    int64_t& errors;
    int64_t& warnings;
    uint64_t& num_objects;
    uint64_t& num_extents;
    uint64_t& num_blobs;
    uint64_t& num_sharded_objects;
    uint64_t& num_spanning_blobs;

    mempool_dynamic_bitset* used_blocks;
    uint64_t_btree_t* used_omap_head;
    std::vector<std::unordered_map<ghobject_t, uint64_t>> *zone_refs;

    ceph::mutex* sb_info_lock;
    sb_info_space_efficient_map_t& sb_info;
    // approximate amount of references per <shared blob, chunk>
    shared_blob_2hash_tracker_t& sb_ref_counts;

    store_statfs_t& expected_store_statfs;
    per_pool_statfs& expected_pool_statfs;
    per_pool_fsck_stats_t& per_pool_fsck_stats;
    BlueStoreRepairer* repairer;

    FSCK_ObjectCtx(int64_t& e,
                   int64_t& w,
                   uint64_t& _num_objects,
                   uint64_t& _num_extents,
                   uint64_t& _num_blobs,
                   uint64_t& _num_sharded_objects,
                   uint64_t& _num_spanning_blobs,
                   mempool_dynamic_bitset* _ub,
                   uint64_t_btree_t* _used_omap_head,
		   std::vector<std::unordered_map<ghobject_t, uint64_t>> *_zone_refs,

                   ceph::mutex* _sb_info_lock,
                   sb_info_space_efficient_map_t& _sb_info,
		   shared_blob_2hash_tracker_t& _sb_ref_counts,
                   store_statfs_t& _store_statfs,
                   per_pool_statfs& _pool_statfs,
		   per_pool_fsck_stats_t& _per_pool_fsck_stats,
                   BlueStoreRepairer* _repairer) :
      errors(e),
      warnings(w),
      num_objects(_num_objects),
      num_extents(_num_extents),
      num_blobs(_num_blobs),
      num_sharded_objects(_num_sharded_objects),
      num_spanning_blobs(_num_spanning_blobs),
      used_blocks(_ub),
      used_omap_head(_used_omap_head),
      zone_refs(_zone_refs),
      sb_info_lock(_sb_info_lock),
      sb_info(_sb_info),
      sb_ref_counts(_sb_ref_counts),
      expected_store_statfs(_store_statfs),
      expected_pool_statfs(_pool_statfs),
      per_pool_fsck_stats(_per_pool_fsck_stats),
      repairer(_repairer) {
    }
  };

  OnodeRef fsck_check_objects_shallow(
    FSCKDepth depth,
    int64_t pool_id,
    CollectionRef c,
    const ghobject_t& oid,
    const std::string& key,
    const ceph::buffer::list& value,
    mempool::bluestore_fsck::list<std::string>* expecting_shards,
    std::map<BlobRef, bluestore_blob_t::unused_t>* referenced,
    BlueStore::FSCK_ObjectCtx& ctx);
#ifdef CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION
  int  push_allocation_to_rocksdb();
  int  read_allocation_from_drive_for_bluestore_tool();
#endif
  int compare_allocation_recovery_for_bluestore_tool(std::ostream& out);

  void set_allocation_in_simple_bmap(SimpleBitmap* sbmap, uint64_t offset, uint64_t length);

private:
  struct  read_alloc_stats_t {
    uint32_t onode_count             = 0;
    uint32_t shard_count             = 0;

    uint32_t skipped_illegal_extent  = 0;

    uint64_t shared_blob_count      = 0;
    uint64_t compressed_blob_count   = 0;
    uint64_t spanning_blob_count     = 0;
    uint64_t insert_count            = 0;
    uint64_t extent_count            = 0;

    std::map<uint64_t, volatile_statfs> actual_pool_vstatfs;
    volatile_statfs actual_store_vstatfs;
  };
  int allocation_recover_and_compare(
    SimpleBitmap *sbmap,
    read_alloc_stats_t &stats,
    std::ostream* extra_out = nullptr);

  friend std::ostream& operator<<(std::ostream& out, const read_alloc_stats_t& stats) {
    out << "==========================================================" << std::endl
        << "onode_count             = " ;out.width(10);out << stats.onode_count << std::endl
        << "shard_count             = " ;out.width(10);out << stats.shard_count << std::endl
        << "shared_blob_count       = " ;out.width(10);out << stats.shared_blob_count << std::endl
        << "compressed_blob_count   = " ;out.width(10);out << stats.compressed_blob_count << std::endl
        << "spanning_blob_count     = " ;out.width(10);out << stats.spanning_blob_count << std::endl
        << "skipped_illegal_extent  = " ;out.width(10);out << stats.skipped_illegal_extent << std::endl
        << "extent_count            = " ;out.width(10);out << stats.extent_count << std::endl
        << "insert_count            = " ;out.width(10);out << stats.insert_count << std::endl;
    store_statfs_t s;
    stats.actual_store_vstatfs.publish(&s);
    out << "store " << s << std::endl;
    for (auto& ps :stats.actual_pool_vstatfs) {
      store_statfs_t s;
      ps.second.publish(&s);
      out << "pool " << ps.first << " " << s << std::endl;
    }
    out << "==========================================================" << std::endl;
    return out;
  }

  int  compare_allocators(Allocator* alloc1, Allocator* alloc2, uint64_t req_extent_count, uint64_t memory_target);
  Allocator* create_bitmap_allocator(uint64_t bdev_size);
  int  add_existing_bluefs_allocation(Allocator* allocator, read_alloc_stats_t& stats);
  int  allocator_add_restored_entries(Allocator *allocator, const void *buff, unsigned extent_count, uint64_t *p_read_alloc_size,
				      uint64_t  *p_extent_count, const void *v_header, BlueFS::FileReader *p_handle, uint64_t offset);

  int  copy_allocator(Allocator* src_alloc, Allocator *dest_alloc, uint64_t* p_num_entries);
  int  store_allocator(Allocator* allocator);
  int  invalidate_allocation_file_on_bluefs();
  int  __restore_allocator(Allocator* allocator, uint64_t *num, uint64_t *bytes);
  int  restore_allocator(Allocator* allocator, uint64_t *num, uint64_t *bytes);
  int  read_allocation_from_drive_on_startup();
  int  reconstruct_allocations(SimpleBitmap *smbmp, read_alloc_stats_t &stats);
  int  read_allocation_from_onodes(SimpleBitmap *smbmp, read_alloc_stats_t& stats);
  int  read_allocation_from_onodes_mt(SimpleBitmap *smbmp, read_alloc_stats_t& stats);
  class OnodeScanMT;
  friend OnodeScanMT;
  int  commit_freelist_type();
  int  commit_to_null_manager();
  int  commit_to_real_manager();
  int  db_cleanup(int ret);
  int  reset_fm_for_restore();
  int  verify_rocksdb_allocations(Allocator *allocator);
  Allocator* clone_allocator_without_bluefs(Allocator *src_allocator);
  Allocator* initialize_allocator_from_freelist(FreelistManager *real_fm);
  void copy_allocator_content_to_fm(Allocator *allocator, FreelistManager *real_fm);


  void _fsck_check_object_omap(FSCKDepth depth,
    OnodeRef& o,
    const BlueStore::FSCK_ObjectCtx& ctx);

  void _fsck_check_objects(FSCKDepth depth,
    FSCK_ObjectCtx& ctx);

public:
  static int create_bdev_labels(CephContext *cct,
                          const std::string& path,
                          const std::vector<std::string>& devs,
			  std::vector<uint64_t>* valid_positions,
			  bool force);

#ifdef BLUESTORE_COMMON_CPUTRACE
  static cpucounter_group cputrace_bluestore;
#define BLUE_SCOPE(y) MEASURE_SCOPE(BlueStore::cputrace_bluestore, y)
#else //BLUESTORE_COMMON_CPUTRACE
#define BLUE_SCOPE(y)
#endif //BLUESTORE_COMMON_CPUTRACE
};

class BlueStoreRepairer
{
  ceph::mutex lock = ceph::make_mutex("BlueStore::BlueStoreRepairer::lock");

public:
  // to simplify future potential migration to mempools
  using fsck_interval = interval_set<uint64_t>;

  // Structure to track what pextents are used for specific cid/oid.
  // Similar to Bloom filter positive and false-positive matches are 
  // possible only.
  // Maintains two lists of bloom filters for both cids and oids
  //   where each list entry is a BF for specific disk pextent
  //   The length of the extent per filter is measured on init.
  // Allows to filter out 'uninteresting' pextents to speadup subsequent
  //  'is_used' access. 
  struct StoreSpaceTracker {
    const uint64_t BLOOM_FILTER_SALT_COUNT = 2;
    const uint64_t BLOOM_FILTER_TABLE_SIZE = 32; // bytes per single filter
    const uint64_t BLOOM_FILTER_EXPECTED_COUNT = 16; // arbitrary selected
    static const uint64_t DEF_MEM_CAP = 128 * 1024 * 1024;

    typedef mempool::bluestore_fsck::vector<bloom_filter> bloom_vector;
    bloom_vector collections_bfs;
    bloom_vector objects_bfs;
    
    bool was_filtered_out = false; 
    uint64_t granularity = 0; // extent length for a single filter

    StoreSpaceTracker() {
    }
    StoreSpaceTracker(const StoreSpaceTracker& from) :
      collections_bfs(from.collections_bfs),
      objects_bfs(from.objects_bfs),
      granularity(from.granularity) {
    }

    void init(uint64_t total,
	      uint64_t min_alloc_size,
	      uint64_t mem_cap = DEF_MEM_CAP) {
      ceph_assert(!granularity); // not initialized yet
      ceph_assert(std::has_single_bit(min_alloc_size));
      ceph_assert(mem_cap);
      
      total = round_up_to(total, min_alloc_size);
      granularity = total * BLOOM_FILTER_TABLE_SIZE * 2 / mem_cap;

      if (!granularity) {
	granularity = min_alloc_size;
      } else {
	granularity = round_up_to(granularity, min_alloc_size);
      }

      uint64_t entries = round_up_to(total, granularity) / granularity;
      collections_bfs.resize(entries,
        bloom_filter(BLOOM_FILTER_SALT_COUNT,
                     BLOOM_FILTER_TABLE_SIZE,
                     0,
                     BLOOM_FILTER_EXPECTED_COUNT));
      objects_bfs.resize(entries, 
        bloom_filter(BLOOM_FILTER_SALT_COUNT,
                     BLOOM_FILTER_TABLE_SIZE,
                     0,
                     BLOOM_FILTER_EXPECTED_COUNT));
    }
    inline uint32_t get_hash(const coll_t& cid) const {
      return cid.hash_to_shard(1);
    }
    inline void set_used(uint64_t offset, uint64_t len,
			 const coll_t& cid, const ghobject_t& oid) {
      ceph_assert(granularity); // initialized
      
      // can't call this func after filter_out has been applied
      ceph_assert(!was_filtered_out);
      if (!len) {
	return;
      }
      auto pos = offset / granularity;
      auto end_pos = (offset + len - 1) / granularity;
      while (pos <= end_pos) {
        collections_bfs[pos].insert(get_hash(cid));
        objects_bfs[pos].insert(oid.hobj.get_hash());
        ++pos;
      }
    }
    // filter-out entries unrelated to the specified(broken) extents.
    // 'is_used' calls are permitted after that only
    size_t filter_out(const fsck_interval& extents);

    // determines if collection's present after filtering-out 
    inline bool is_used(const coll_t& cid) const {
      ceph_assert(was_filtered_out);
      for(auto& bf : collections_bfs) {
        if (bf.contains(get_hash(cid))) {
          return true;
        }
      }
      return false;
    }
    // determines if object's present after filtering-out 
    inline bool is_used(const ghobject_t& oid) const {
      ceph_assert(was_filtered_out);
      for(auto& bf : objects_bfs) {
        if (bf.contains(oid.hobj.get_hash())) {
          return true;
        }
      }
      return false;
    }
    // determines if collection's present before filtering-out 
    inline bool is_used(const coll_t& cid, uint64_t offs) const {
      ceph_assert(granularity); // initialized
      ceph_assert(!was_filtered_out);
      auto &bf = collections_bfs[offs / granularity];
      if (bf.contains(get_hash(cid))) {
        return true;
      }
      return false;
    }
    // determines if object's present before filtering-out 
    inline bool is_used(const ghobject_t& oid, uint64_t offs) const {
      ceph_assert(granularity); // initialized
      ceph_assert(!was_filtered_out);
      auto &bf = objects_bfs[offs / granularity];
      if (bf.contains(oid.hobj.get_hash())) {
        return true;
      }
      return false;
    }
  };

public:
  void fix_per_pool_omap(KeyValueDB *db, int);
  bool remove_key(KeyValueDB *db, const std::string& prefix, const std::string& key);
  bool fix_shared_blob(KeyValueDB::Transaction txn,
			uint64_t sbid,
			bluestore_extent_ref_map_t* ref_map,
			size_t repaired = 1);
  bool fix_statfs(KeyValueDB *db, const std::string& key,
    const store_statfs_t& new_statfs);

  bool fix_leaked(KeyValueDB *db,
		  FreelistManager* fm,
		  uint64_t offset, uint64_t len);
  bool fix_false_free(KeyValueDB *db,
		      FreelistManager* fm,
		      uint64_t offset, uint64_t len);
  bool fix_spanning_blobs(
    KeyValueDB* db,
    std::function<void(KeyValueDB::Transaction)> f);

  bool preprocess_misreference(KeyValueDB *db);

  unsigned apply(KeyValueDB* db);

  void note_misreference(uint64_t offs, uint64_t len, bool inc_error) {
    std::lock_guard l(lock);
    misreferenced_extents.union_insert(offs, len);
    if (inc_error) {
      ++to_repair_cnt;
    }
  }
  //////////////////////
  //In fact two methods below are the only ones in this class which are thread-safe!!
  void inc_repaired(size_t n = 1) {
    to_repair_cnt += n;
  }
  void request_compaction() {
    need_compact = true;
  }
  //////////////////////

  void init_space_usage_tracker(
    uint64_t total_space, uint64_t lres_tracking_unit_size)
  {
    //NB: not for use in multithreading mode!!!
    space_usage_tracker.init(total_space, lres_tracking_unit_size);
  }
  void set_space_used(uint64_t offset, uint64_t len,
    const coll_t& cid, const ghobject_t& oid) {
    std::lock_guard l(lock);
    space_usage_tracker.set_used(offset, len, cid, oid);
  }
  inline bool is_used(const coll_t& cid) const {
    //NB: not for use in multithreading mode!!!
    return space_usage_tracker.is_used(cid);
  }
  inline bool is_used(const ghobject_t& oid) const {
    //NB: not for use in multithreading mode!!!
    return space_usage_tracker.is_used(oid);
  }

  const fsck_interval& get_misreferences() const {
    //NB: not for use in multithreading mode!!!
    return misreferenced_extents;
  }
  KeyValueDB::Transaction get_fix_misreferences_txn() {
    //NB: not for use in multithreading mode!!!
    return fix_misreferences_txn;
  }

private:
  std::atomic<unsigned> to_repair_cnt = { 0 };
  std::atomic<bool> need_compact = { false };
  KeyValueDB::Transaction fix_per_pool_omap_txn;
  KeyValueDB::Transaction fix_fm_leaked_txn;
  KeyValueDB::Transaction fix_fm_false_free_txn;
  KeyValueDB::Transaction remove_key_txn;
  KeyValueDB::Transaction fix_statfs_txn;
  KeyValueDB::Transaction fix_shared_blob_txn;

  KeyValueDB::Transaction fix_misreferences_txn;
  KeyValueDB::Transaction fix_onode_txn;

  StoreSpaceTracker space_usage_tracker;

  // non-shared extents with multiple references
  fsck_interval misreferenced_extents;

};

struct FragMetric {
  // Computes fragmentation as the number of disjoint segments
  // produced by a stream of mapped ranges.
  // frag_score == current disjoint segment count.

  std::unordered_set<uint64_t> endpoints;
  uint64_t frag_score = 0;

  FragMetric() {}

  inline void note(uint64_t offset, uint64_t length) {
    bool merge_left = endpoints.count(offset);
    bool merge_right = endpoints.count(offset + length);
    if (merge_left && merge_right) {
      endpoints.erase(offset);
      endpoints.erase(offset + length);
      frag_score--;
    } else if (merge_left) {
      endpoints.erase(offset);
      endpoints.insert(offset + length);
    } else if (merge_right) {
      endpoints.erase(offset + length);
      endpoints.insert(offset);
    } else {
      endpoints.insert(offset);
      endpoints.insert(offset + length);
      frag_score++;
    }
  }
};

#endif
