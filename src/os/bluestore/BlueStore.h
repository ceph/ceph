// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include <unistd.h>

#include <atomic>
#include <bit>
#include <chrono>
#include <ratio>
#include <mutex>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>

#include "include/cpp-btree/btree_set.h"

#include "include/ceph_assert.h"
#include "include/interval_set.h"
#include "include/unordered_map.h"
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
#include "BlueFS.h"
#include "common/EventTrace.h"

#ifdef WITH_BLKIN
#include "common/zipkin_trace.h"
#endif

class Allocator;
class FreelistManager;
class BlueStoreRepairer;
class SimpleBitmap;
//#define DEBUG_CACHE
//#define DEBUG_DEFERRED

// constants for Buffer::optimize()
#define MAX_BUFFER_SLOP_RATIO_DEN  8  // so actually 1/N
#define CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

enum {
  l_bluestore_first = 732430,
  // space utilization stats
  //****************************************
  l_bluestore_allocated,
  l_bluestore_stored,
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
  //****************************************

  // buffer cache stats
  //****************************************
  l_bluestore_buffers,
  l_bluestore_buffer_bytes,
  l_bluestore_buffer_hit_bytes,
  l_bluestore_buffer_miss_bytes,
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
  //****************************************

  // other client ops latencies
  //****************************************
  l_bluestore_omap_seek_to_first_lat,
  l_bluestore_omap_upper_bound_lat,
  l_bluestore_omap_lower_bound_lat,
  l_bluestore_omap_next_lat,
  l_bluestore_omap_get_keys_lat,
  l_bluestore_omap_get_values_lat,
  l_bluestore_omap_clear_lat,
  l_bluestore_clist_lat,
  l_bluestore_remove_lat,
  l_bluestore_truncate_lat,
  //****************************************

  // allocation stats
  //****************************************
  l_bluestore_allocate_hist,
  //****************************************

  // slow op counter
  //****************************************
  l_bluestore_slow_aio_wait_count,
  l_bluestore_slow_committed_kv_count,
  l_bluestore_slow_read_onode_meta_count,
  l_bluestore_slow_read_wait_aio_count,
  //****************************************
  l_bluestore_last
};

#define META_POOL_ID ((uint64_t)-1ull)
using bptr_c_it_t = buffer::ptr::const_iterator;

class BlueStore : public ObjectStore,
		  public md_config_obs_t {
  // -----------------------------------------------------
  // types
public:
  // config observer
  const char** get_tracked_conf_keys() const override;
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

  struct TransContext;

  typedef std::map<uint64_t, ceph::buffer::list> ready_regions_t;


  struct BufferSpace;
  struct Collection;
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  struct AioContext {
    virtual void aio_finish(BlueStore *store) = 0;
    virtual ~AioContext() {}
  };

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
    uint64_t seq;
    uint32_t offset, length;
    ceph::buffer::list data;
    std::shared_ptr<int64_t> cache_age_bin;  ///< cache age bin

    boost::intrusive::list_member_hook<> lru_item;
    boost::intrusive::list_member_hook<> state_item;

    Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, uint32_t l,
	   unsigned f = 0)
      : space(space), state(s), flags(f), seq(q), offset(o), length(l) {}
    Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, ceph::buffer::list& b,
	   unsigned f = 0)
      : space(space), state(s), flags(f), seq(q), offset(o),
	length(b.length()), data(b) {}

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
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("offset", offset);
      f->dump_unsigned("length", length);
      f->dump_unsigned("data_length", data.length());
    }
  };

  struct BufferCacheShard;

  /// map logical extent range (object) onto buffers
  struct BufferSpace {
    enum {
      BYPASS_CLEAN_CACHE = 0x1,  // bypass clean cache
    };

    typedef boost::intrusive::list<
      Buffer,
      boost::intrusive::member_hook<
        Buffer,
	boost::intrusive::list_member_hook<>,
	&Buffer::state_item> > state_list_t;

    mempool::bluestore_cache_meta::map<uint32_t, std::unique_ptr<Buffer>>
      buffer_map;

    // we use a bare intrusive list here instead of std::map because
    // it uses less memory and we expect this to be very small (very
    // few IOs in flight to the same Blob at the same time).
    state_list_t writing;   ///< writing buffers, sorted by seq, ascending

    ~BufferSpace() {
      ceph_assert(buffer_map.empty());
      ceph_assert(writing.empty());
    }

    void _add_buffer(BufferCacheShard* cache, Buffer* b, int level, Buffer* near) {
      cache->_audit("_add_buffer start");
      buffer_map[b->offset].reset(b);
      if (b->is_writing()) {
        // we might get already cached data for which resetting mempool is inppropriate
        // hence calling try_assign_to_mempool
        b->data.try_assign_to_mempool(mempool::mempool_bluestore_writing);
        if (writing.empty() || writing.rbegin()->seq <= b->seq) {
          writing.push_back(*b);
        } else {
          auto it = writing.begin();
          while (it->seq < b->seq) {
            ++it;
          }

          ceph_assert(it->seq >= b->seq);
          // note that this will insert b before it
          // hence the order is maintained
          writing.insert(it, *b);
        }
      } else {
        b->data.reassign_to_mempool(mempool::mempool_bluestore_cache_data);
        cache->_add(b, level, near);
      }
      cache->_audit("_add_buffer end");
    }
    void _rm_buffer(BufferCacheShard* cache, Buffer *b) {
      _rm_buffer(cache, buffer_map.find(b->offset));
    }
    std::map<uint32_t, std::unique_ptr<Buffer>>::iterator
    _rm_buffer(BufferCacheShard* cache,
		    std::map<uint32_t, std::unique_ptr<Buffer>>::iterator p) {
      ceph_assert(p != buffer_map.end());
      cache->_audit("_rm_buffer start");
      if (p->second->is_writing()) {
        writing.erase(writing.iterator_to(*p->second));
      } else {
	cache->_rm(p->second.get());
      }
      p = buffer_map.erase(p);
      cache->_audit("_rm_buffer end");
      return p;
    }

    std::map<uint32_t,std::unique_ptr<Buffer>>::iterator _data_lower_bound(
      uint32_t offset) {
      auto i = buffer_map.lower_bound(offset);
      if (i != buffer_map.begin()) {
	--i;
	if (i->first + i->second->length <= offset)
	  ++i;
      }
      return i;
    }

    // must be called under protection of the Cache lock
    void _clear(BufferCacheShard* cache);

    // return value is the highest cache_private of a trimmed buffer, or 0.
    int discard(BufferCacheShard* cache, uint32_t offset, uint32_t length) {
      std::lock_guard l(cache->lock);
      int ret = _discard(cache, offset, length);
      cache->_trim();
      return ret;
    }
    int _discard(BufferCacheShard* cache, uint32_t offset, uint32_t length);

    void write(BufferCacheShard* cache, uint64_t seq, uint32_t offset, ceph::buffer::list& bl,
	       unsigned flags) {
      std::lock_guard l(cache->lock);
      Buffer *b = new Buffer(this, Buffer::STATE_WRITING, seq, offset, bl,
			     flags);
      b->cache_private = _discard(cache, offset, bl.length());
      _add_buffer(cache, b, (flags & Buffer::FLAG_NOCACHE) ? 0 : 1, nullptr);
      cache->_trim();
    }
    void _finish_write(BufferCacheShard* cache, uint64_t seq);
    void did_read(BufferCacheShard* cache, uint32_t offset, ceph::buffer::list& bl) {
      std::lock_guard l(cache->lock);
      Buffer *b = new Buffer(this, Buffer::STATE_CLEAN, 0, offset, bl);
      b->cache_private = _discard(cache, offset, bl.length());
      _add_buffer(cache, b, 1, nullptr);
      cache->_trim();
    }

    void read(BufferCacheShard* cache, uint32_t offset, uint32_t length,
	      BlueStore::ready_regions_t& res,
	      interval_set<uint32_t>& res_intervals,
	      int flags = 0);

    void truncate(BufferCacheShard* cache, uint32_t offset) {
      discard(cache, offset, (uint32_t)-1 - offset);
    }

    bool _dup_writing(BufferCacheShard* cache, BufferSpace* to);
    void split(BufferCacheShard* cache, size_t pos, BufferSpace &r);

    void dump(BufferCacheShard* cache, ceph::Formatter *f) const {
      std::lock_guard l(cache->lock);
      f->open_array_section("buffers");
      for (auto& i : buffer_map) {
	f->open_object_section("buffer");
	ceph_assert(i.first == i.second->offset);
	i.second->dump(f);
	f->close_section();
      }
      f->close_section();
    }
    friend std::ostream& operator<<(std::ostream& out, const BufferSpace& bc);
  };

  struct SharedBlobSet;

  /// in-memory shared blob state (incl cached buffers)
  struct SharedBlob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0}; ///< reference count
    bool loaded = false;

    CollectionRef coll;
    union {
      uint64_t sbid_unloaded;              ///< sbid if persistent isn't loaded
      bluestore_shared_blob_t *persistent; ///< persistent part of the shared blob if any
    };

    SharedBlob(Collection *_coll) : coll(_coll), sbid_unloaded(0) {
    }
    SharedBlob(uint64_t i, Collection *_coll);
    ~SharedBlob();

    uint64_t get_sbid() const {
      return loaded ? persistent->sbid : sbid_unloaded;
    }

    friend void intrusive_ptr_add_ref(SharedBlob *b) { b->get(); }
    friend void intrusive_ptr_release(SharedBlob *b) { b->put(); }

    void dump(ceph::Formatter* f) const;
    friend std::ostream& operator<<(std::ostream& out, const SharedBlob& sb);

    void get() {
      ++nref;
    }
    void put();

    /// get logical references
    void get_ref(uint64_t offset, uint32_t length);

    /// put logical references, and get back any released extents
    void put_ref(uint64_t offset, uint32_t length,
		 PExtentVector *r, bool *unshare);
    friend bool operator==(const SharedBlob &l, const SharedBlob &r) {
      return l.get_sbid() == r.get_sbid();
    }
    inline BufferCacheShard* get_cache() {
      return coll ? coll->cache : nullptr;
    }
    inline SharedBlobSet* get_parent() {
      return coll ? &(coll->shared_blob_set) : nullptr;
    }
    inline bool is_loaded() const {
      return loaded;
    }

  };
  typedef boost::intrusive_ptr<SharedBlob> SharedBlobRef;

  /// a lookup table of SharedBlobs
  struct SharedBlobSet {
    /// protect lookup, insertion, removal
    ceph::mutex lock = ceph::make_mutex("BlueStore::SharedBlobSet::lock");

    // we use a bare pointer because we don't want to affect the ref
    // count
    mempool::bluestore_cache_meta::unordered_map<uint64_t,SharedBlob*> sb_map;

    SharedBlobRef lookup(uint64_t sbid) {
      std::lock_guard l(lock);
      auto p = sb_map.find(sbid);
      if (p == sb_map.end() ||
	  p->second->nref == 0) {
        return nullptr;
      }
      return p->second;
    }

    void add(Collection* coll, SharedBlob *sb) {
      std::lock_guard l(lock);
      sb_map[sb->get_sbid()] = sb;
      sb->coll = coll;
    }

    bool remove(SharedBlob *sb, bool verify_nref_is_zero=false) {
      std::lock_guard l(lock);
      ceph_assert(sb->get_parent() == this);
      if (verify_nref_is_zero && sb->nref != 0) {
	return false;
      }
      // only remove if it still points to us
      auto p = sb_map.find(sb->get_sbid());
      if (p != sb_map.end() &&
	  p->second == sb) {
	sb_map.erase(p);
      }
      return true;
    }

    bool empty() {
      std::lock_guard l(lock);
      return sb_map.empty();
    }

    template <int LogLevelV>
    void dump(CephContext *cct);
  };

//#define CACHE_BLOB_BL  // not sure if this is a win yet or not... :/

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    SharedBlobRef shared_blob;      ///< shared blob state (if any)

    void set_shared_blob(SharedBlobRef sb) {
      ceph_assert((bool)sb);
      ceph_assert(!shared_blob);
      shared_blob = sb;
      ceph_assert(shared_blob->get_cache());
      shared_blob->get_cache()->add_blob();
    }
    BufferSpace bc;
  private:
    mutable bluestore_blob_t blob;  ///< decoded blob metadata
#ifdef CACHE_BLOB_BL
    mutable ceph::buffer::list blob_bl;     ///< cached encoded blob, blob is dirty if empty
#endif
    /// refs from this shard.  ephemeral if id<0, persisted if spanning.
    bluestore_blob_use_tracker_t used_in_blob;

  public:

    friend void intrusive_ptr_add_ref(Blob *b) { b->get(); }
    friend void intrusive_ptr_release(Blob *b) { b->put(); }

    void dump(ceph::Formatter* f) const;
    friend std::ostream& operator<<(std::ostream& out, const Blob &b);

    const bluestore_blob_use_tracker_t& get_blob_use_tracker() const {
      return used_in_blob;
    }
    bluestore_blob_use_tracker_t& dirty_blob_use_tracker() {
      return used_in_blob;
    }
    bool is_referenced() const {
      return used_in_blob.is_not_empty();
    }
    uint32_t get_referenced_bytes() const {
      return used_in_blob.get_referenced_bytes();
    }

    bool is_spanning() const {
      return id >= 0;
    }

    bool can_split() const {
      std::lock_guard l(shared_blob->get_cache()->lock);
      // splitting a BufferSpace writing list is too hard; don't try.
      return get_bc().writing.empty() &&
             used_in_blob.can_split() &&
             get_blob().can_split();
    }

    bool can_merge_blob(const Blob* other, uint32_t& blob_end) const;
    uint32_t merge_blob(CephContext* cct, Blob* blob_to_dissolve);

    bool can_split_at(uint32_t blob_offset) const {
      return used_in_blob.can_split_at(blob_offset) &&
             get_blob().can_split_at(blob_offset);
    }

    bool can_reuse_blob(uint32_t min_alloc_size,
			uint32_t target_blob_size,
			uint32_t b_offset,
			uint32_t *length0);

    void dup(Blob& o) {
      o.set_shared_blob(shared_blob);
      o.blob = blob;
#ifdef CACHE_BLOB_BL
      o.blob_bl = blob_bl;
#endif
    }
    void dup(const Blob& from, bool copy_used_in_blob);
    void copy_from(CephContext* cct, const Blob& from,
		   uint32_t min_release_size, uint32_t start, uint32_t len);
    void copy_extents(CephContext* cct, const Blob& from, uint32_t start,
		      uint32_t pre_len, uint32_t main_len, uint32_t post_len);
    void copy_extents_over_empty(CephContext* cct, const Blob& from, uint32_t start, uint32_t len);

    inline const bluestore_blob_t& get_blob() const {
      return blob;
    }
    inline bluestore_blob_t& dirty_blob() {
#ifdef CACHE_BLOB_BL
      blob_bl.clear();
#endif
      return blob;
    }
    /// clear buffers from unused sections
    void discard_unused_buffers(CephContext* cct, BufferCacheShard* cache);

    inline const BufferSpace& get_bc() const {
      return bc;
    }
    inline BufferSpace& dirty_bc() {
      return bc;
    }

    /// discard buffers for unallocated regions
    void discard_unallocated(Collection *coll);

    /// get logical references
    void get_ref(Collection *coll, uint32_t offset, uint32_t length);
    /// put logical references, and get back any released extents
    bool put_ref(Collection *coll, uint32_t offset, uint32_t length,
		 PExtentVector *r);
    // update caches to reflect content up to seq
    void finish_write(uint64_t seq);
    /// split the blob
    void split(Collection *coll, uint32_t blob_offset, Blob *o);

    void get() {
      ++nref;
    }
    void put() {
      if (--nref == 0)
	delete this;
    }
    ~Blob();

#ifdef CACHE_BLOB_BL
    void _encode() const {
      if (blob_bl.length() == 0 ) {
	encode(blob, blob_bl);
      } else {
	ceph_assert(blob_bl.length());
      }
    }
    void bound_encode(
      size_t& p,
      bool include_ref_map) const {
      _encode();
      p += blob_bl.length();
      if (include_ref_map) {
	used_in_blob.bound_encode(p);
      }
    }
    void encode(
      ceph::buffer::list::contiguous_appender& p,
      bool include_ref_map) const {
      _encode();
      p.append(blob_bl);
      if (include_ref_map) {
	used_in_blob.encode(p);
      }
    }
    void decode(
      ceph::buffer::ptr::const_iterator& p,
      bool include_ref_map,
      Collection */*coll*/) {
      const char *start = p.get_pos();
      denc(blob, p);
      const char *end = p.get_pos();
      blob_bl.clear();
      blob_bl.append(start, end - start);
      if (include_ref_map) {
	used_in_blob.decode(p);
      }
    }
#else
    void bound_encode(
      size_t& p,
      uint64_t struct_v,
      uint64_t sbid,
      bool include_ref_map) const {
      denc(blob, p, struct_v);
      if (blob.is_shared()) {
        denc(sbid, p);
      }
      if (include_ref_map) {
	used_in_blob.bound_encode(p);
      }
    }
    void encode(
      ceph::buffer::list::contiguous_appender& p,
      uint64_t struct_v,
      uint64_t sbid,
      bool include_ref_map) const {
      denc(blob, p, struct_v);
      if (blob.is_shared()) {
        denc(sbid, p);
      }
      if (include_ref_map) {
	used_in_blob.encode(p);
      }
    }
    void decode(
      ceph::buffer::ptr::const_iterator& p,
      uint64_t struct_v,
      uint64_t* sbid,
      bool include_ref_map,
      Collection *coll);
#endif
  };
  typedef boost::intrusive_ptr<Blob> BlobRef;
  typedef mempool::bluestore_cache_meta::map<int,BlobRef> blob_map_t;

  /// a logical extent, pointing to (some portion of) a blob
  typedef boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true> > ExtentBase; //making an alias to avoid build warnings
  struct Extent : public ExtentBase {
    MEMPOOL_CLASS_HELPERS();

    uint32_t logical_offset = 0;      ///< logical offset
    uint32_t blob_offset = 0;         ///< blob offset
    uint32_t length = 0;              ///< length
    BlobRef  blob;                    ///< the blob with our data

    /// ctor for lookup only
    explicit Extent(uint32_t lo) : ExtentBase(), logical_offset(lo) { }
    /// ctor for delayed initialization (see decode_some())
    explicit Extent() : ExtentBase() {
    }
    /// ctor for general usage
    Extent(uint32_t lo, uint32_t o, uint32_t l, BlobRef& b)
      : ExtentBase(),
        logical_offset(lo), blob_offset(o), length(l) {
      assign_blob(b);
    }
    ~Extent() {
      if (blob) {
	blob->shared_blob->get_cache()->rm_extent();
      }
    }

    void dump(ceph::Formatter* f) const;

    void assign_blob(const BlobRef& b) {
      ceph_assert(!blob);
      blob = b;
      blob->shared_blob->get_cache()->add_extent();
    }

    // comparators for intrusive_set
    friend bool operator<(const Extent &a, const Extent &b) {
      return a.logical_offset < b.logical_offset;
    }
    friend bool operator>(const Extent &a, const Extent &b) {
      return a.logical_offset > b.logical_offset;
    }
    friend bool operator==(const Extent &a, const Extent &b) {
      return a.logical_offset == b.logical_offset;
    }

    uint32_t blob_start() const {
      return logical_offset - blob_offset;
    }

    uint32_t blob_end() const {
      return blob_start() + blob->get_blob().get_logical_length();
    }

    uint32_t logical_end() const {
      return logical_offset + length;
    }

    // return true if any piece of the blob is out of
    // the given range [o, o + l].
    bool blob_escapes_range(uint32_t o, uint32_t l) const {
      return blob_start() < o || blob_end() > o + l;
    }
  };
  typedef boost::intrusive::set<Extent> extent_map_t;


  friend std::ostream& operator<<(std::ostream& out, const Extent& e);

  struct OldExtent {
    boost::intrusive::list_member_hook<> old_extent_item;
    Extent e;
    PExtentVector r;
    bool blob_empty; // flag to track the last removed extent that makes blob
                     // empty - required to update compression stat properly
    OldExtent(uint32_t lo, uint32_t o, uint32_t l, BlobRef& b)
      : e(lo, o, l, b), blob_empty(false) {
    }
    static OldExtent* create(CollectionRef c,
                             uint32_t lo,
			     uint32_t o,
			     uint32_t l,
			     BlobRef& b);
  };
  typedef boost::intrusive::list<
      OldExtent,
      boost::intrusive::member_hook<
        OldExtent,
    boost::intrusive::list_member_hook<>,
    &OldExtent::old_extent_item> > old_extent_map_t;

  struct Onode;

  /// a sharded extent map, mapping offsets to lextents to blobs
  struct ExtentMap {
    Onode *onode;
    extent_map_t extent_map;        ///< map of Extents to Blobs
    blob_map_t spanning_blob_map;   ///< blobs that span shards
    typedef boost::intrusive_ptr<Onode> OnodeRef;

    struct Shard {
      bluestore_onode_t::shard_info *shard_info = nullptr;
      unsigned extents = 0;  ///< count extents in this shard
      bool loaded = false;   ///< true if shard is loaded
      bool dirty = false;    ///< true if shard is dirty and needs reencoding
    };

    mempool::bluestore_cache_meta::vector<Shard> shards;    ///< shards

    ceph::buffer::list inline_bl;    ///< cached encoded map, if unsharded; empty=>dirty

    uint32_t needs_reshard_begin = 0;
    uint32_t needs_reshard_end = 0;

    void scan_shared_blobs(uint64_t start, uint64_t length,
			   std::multimap<uint64_t /*blob_start*/, Blob*>& candidates);
    Blob* find_mergable_companion(Blob* blob_to_dissolve, uint32_t blob_start, uint32_t& blob_width,
				  std::multimap<uint64_t /*blob_start*/, Blob*>& candidates);
    void reblob_extents(uint32_t blob_start, uint32_t blob_end,
			BlobRef from_blob, BlobRef to_blob);
    void make_range_shared_maybe_merge(TransContext* txc, OnodeRef& onode,
				       uint64_t srcoff, uint64_t length);

    void dup(BlueStore* b, TransContext*, CollectionRef&, OnodeRef&, OnodeRef&,
      uint64_t&, uint64_t&, uint64_t&);
    void dup_esb(BlueStore* b, TransContext*, CollectionRef&, OnodeRef&, OnodeRef&,
      uint64_t&, uint64_t&, uint64_t&);

    bool needs_reshard() const {
      return needs_reshard_end > needs_reshard_begin;
    }
    void clear_needs_reshard() {
      needs_reshard_begin = needs_reshard_end = 0;
    }
    void request_reshard(uint32_t begin, uint32_t end) {
      if (begin < needs_reshard_begin) {
	needs_reshard_begin = begin;
      }
      if (end > needs_reshard_end) {
	needs_reshard_end = end;
      }
    }
    // signals that there was a modification on range <begin, end)
    // if this spans over a shard boundary, then shards no longer
    // can be encoded separately, and reshard run is needed
    void maybe_reshard(uint32_t begin, uint32_t end) {
      if (spans_shard(begin, end - begin)) {
	request_reshard(begin, end);
      }
    }

    struct DeleteDisposer {
      void operator()(Extent *e) { delete e; }
    };

    ExtentMap(Onode *o, size_t inline_shard_prealloc_size);
    ~ExtentMap() {
      extent_map.clear_and_dispose(DeleteDisposer());
    }

    void clear() {
      extent_map.clear_and_dispose(DeleteDisposer());
      shards.clear();
      inline_bl.clear();
      clear_needs_reshard();
    }

    void dump(ceph::Formatter* f) const;

    bool encode_some(uint32_t offset, uint32_t length, ceph::buffer::list& bl,
		     unsigned *pn);

    class ExtentDecoder {
      uint64_t pos = 0;
      uint64_t prev_len = 0;
      uint64_t extent_pos = 0;
    protected:
      virtual void consume_blobid(Extent* le,
                                  bool spanning,
                                  uint64_t blobid) = 0;
      virtual void consume_blob(Extent* le,
                                uint64_t extent_no,
                                uint64_t sbid,
                                BlobRef b) = 0;
      virtual void consume_spanning_blob(uint64_t sbid, BlobRef b) = 0;
      virtual Extent* get_next_extent() = 0;
      virtual void add_extent(Extent*) = 0;

      void decode_extent(Extent* le,
                         __u8 struct_v,
                         bptr_c_it_t& p,
                         Collection* c);
    public:
      virtual ~ExtentDecoder() {
      }

      unsigned decode_some(const ceph::buffer::list& bl, Collection* c);
      void decode_spanning_blobs(bptr_c_it_t& p, Collection* c);
    };

    class ExtentDecoderFull : public ExtentDecoder {
      ExtentMap& extent_map;
      std::vector<BlobRef> blobs;
    protected:
      void consume_blobid(Extent* le, bool spanning, uint64_t blobid) override;
      void consume_blob(Extent* le,
                        uint64_t extent_no,
                        uint64_t sbid,
                        BlobRef b) override;
      void consume_spanning_blob(uint64_t sbid, BlobRef b) override;
      Extent* get_next_extent() override;
      void add_extent(Extent* ) override;
    public:
      ExtentDecoderFull (ExtentMap& _extent_map) : extent_map(_extent_map) {
      }
    };

    unsigned decode_some(ceph::buffer::list& bl);

    void bound_encode_spanning_blobs(size_t& p);
    void encode_spanning_blobs(ceph::buffer::list::contiguous_appender& p);
    BlobRef get_spanning_blob(int id) {
      auto p = spanning_blob_map.find(id);
      ceph_assert(p != spanning_blob_map.end());
      return p->second;
    }

    void update(KeyValueDB::Transaction t, bool force);
    decltype(BlueStore::Blob::id) allocate_spanning_blob_id();
    void reshard(
      KeyValueDB *db,
      KeyValueDB::Transaction t);

    /// initialize Shards from the onode
    void init_shards(bool loaded, bool dirty);

    /// return index of shard containing offset
    /// or -1 if not found
    int seek_shard(uint32_t offset) {
      size_t end = shards.size();
      size_t mid, left = 0;
      size_t right = end; // one passed the right end

      while (left < right) {
        mid = left + (right - left) / 2;
        if (offset >= shards[mid].shard_info->offset) {
          size_t next = mid + 1;
          if (next >= end || offset < shards[next].shard_info->offset)
            return mid;
          //continue to search forwards
          left = next;
        } else {
          //continue to search backwards
          right = mid;
        }
      }

      return -1; // not found
    }

    /// check if a range spans a shard
    bool spans_shard(uint32_t offset, uint32_t length) {
      if (shards.empty()) {
	return false;
      }
      int s = seek_shard(offset);
      ceph_assert(s >= 0);
      if (s == (int)shards.size() - 1) {
	return false; // last shard
      }
      if (offset + length <= shards[s+1].shard_info->offset) {
	return false;
      }
      return true;
    }

    /// ensure that a range of the map is loaded
    void fault_range(KeyValueDB *db,
		     uint32_t offset, uint32_t length);

    /// ensure a range of the map is marked dirty
    void dirty_range(uint32_t offset, uint32_t length);

    /// for seek_lextent test
    extent_map_t::iterator find(uint64_t offset);

    /// seek to the first lextent including or after offset
    extent_map_t::iterator seek_lextent(uint64_t offset);
    extent_map_t::const_iterator seek_lextent(uint64_t offset) const;

    /// add a new Extent
    void add(uint32_t lo, uint32_t o, uint32_t l, BlobRef& b) {
      extent_map.insert(*new Extent(lo, o, l, b));
    }

    /// remove (and delete) an Extent
    void rm(extent_map_t::iterator p) {
      extent_map.erase_and_dispose(p, DeleteDisposer());
    }

    bool has_any_lextents(uint64_t offset, uint64_t length);

    /// consolidate adjacent lextents in extent_map
    int compress_extent_map(uint64_t offset, uint64_t length);

    /// punch a logical hole.  add lextents to deref to target list.
    void punch_hole(CollectionRef &c,
		    uint64_t offset, uint64_t length,
		    old_extent_map_t *old_extents);

    /// put new lextent into lextent_map overwriting existing ones if
    /// any and update references accordingly
    Extent *set_lextent(CollectionRef &c,
			uint64_t logical_offset,
			uint64_t offset, uint64_t length,
                        BlobRef b,
			old_extent_map_t *old_extents);

    /// split a blob (and referring extents)
    BlobRef split_blob(BlobRef lb, uint32_t blob_offset, uint32_t pos);

    /// allocation unit status
    struct debug_au_state_t {
      uint64_t disk_offset; //< offset of the data on disk (in bytes)
      uint32_t disk_length; //< length of the data on disk
                            //  <offset, offset + length) never crosses AU boundary
      uint32_t chksum;      //< checksum of the AU
      uint32_t ref_cnts;    //< how many times AU is shared
      debug_au_state_t(
	uint64_t disk_offset, uint32_t disk_length,
	uint32_t chksum, uint32_t ref_cnts)
	: disk_offset(disk_offset)
	, disk_length(disk_length)
	, chksum(chksum)
	, ref_cnts(ref_cnts) {}
    };
    using debug_au_vector_t = std::vector<debug_au_state_t>;
    /// Produces a sequence of allocation units representing logical offsets.
    /// If there is a discontinuity, it is encoded as disk_offset==-1.
    debug_au_vector_t debug_list_disk_layout();

    friend std::ostream& operator<<(std::ostream& out, const debug_au_vector_t& auv);
  };

  /// Compressed Blob Garbage collector
  /*
  The primary idea of the collector is to estimate a difference between
  allocation units(AU) currently present for compressed blobs and new AUs
  required to store that data uncompressed. 
  Estimation is performed for protrusive extents within a logical range
  determined by a concatenation of old_extents collection and specific(current)
  write request.
  The root cause for old_extents use is the need to handle blob ref counts
  properly. Old extents still hold blob refs and hence we need to traverse
  the collection to determine if blob to be released.
  Protrusive extents are extents that fit into the blob std::set in action
  (ones that are below the logical range from above) but not removed totally
  due to the current write. 
  E.g. for
  extent1 <loffs = 100, boffs = 100, len  = 100> -> 
    blob1<compressed, len_on_disk=4096, logical_len=8192>
  extent2 <loffs = 200, boffs = 200, len  = 100> ->
    blob2<raw, len_on_disk=4096, llen=4096>
  extent3 <loffs = 300, boffs = 300, len  = 100> ->
    blob1<compressed, len_on_disk=4096, llen=8192>
  extent4 <loffs = 4096, boffs = 0, len  = 100>  ->
    blob3<raw, len_on_disk=4096, llen=4096>
  write(300~100)
  protrusive extents are within the following ranges <0~300, 400~8192-400>
  In this case existing AUs that might be removed due to GC (i.e. blob1) 
  use 2x4K bytes.
  And new AUs expected after GC = 0 since extent1 to be merged into blob2.
  Hence we should do a collect.
  */
  class GarbageCollector
  {
  public:
    /// return amount of allocation units that might be saved due to GC
    int64_t estimate(
      uint64_t offset,
      uint64_t length,
      const ExtentMap& extent_map,
      const old_extent_map_t& old_extents,
      uint64_t min_alloc_size);

    /// return a collection of extents to perform GC on
    const interval_set<uint64_t>& get_extents_to_collect() const {
      return extents_to_collect;
    }
    GarbageCollector(CephContext* _cct) : cct(_cct) {}

  private:
    struct BlobInfo {
      uint64_t referenced_bytes = 0;    ///< amount of bytes referenced in blob
      int64_t expected_allocations = 0; ///< new alloc units required 
                                        ///< in case of gc fulfilled
      bool collect_candidate = false;   ///< indicate if blob has any extents 
                                        ///< eligible for GC.
      extent_map_t::const_iterator first_lextent; ///< points to the first 
                                                  ///< lextent referring to 
                                                  ///< the blob if any.
                                                  ///< collect_candidate flag 
                                                  ///< determines the validity
      extent_map_t::const_iterator last_lextent;  ///< points to the last 
                                                  ///< lextent referring to 
                                                  ///< the blob if any.

      BlobInfo(uint64_t ref_bytes) :
        referenced_bytes(ref_bytes) {
      }
    };
    CephContext* cct;
    std::map<Blob*, BlobInfo> affected_blobs; ///< compressed blobs and their ref_map
                                         ///< copies that are affected by the
                                         ///< specific write

    ///< protrusive extents that should be collected if GC takes place
    interval_set<uint64_t> extents_to_collect;

    boost::optional<uint64_t > used_alloc_unit; ///< last processed allocation
                                                ///<  unit when traversing 
                                                ///< protrusive extents. 
                                                ///< Other extents mapped to
                                                ///< this AU to be ignored 
                                                ///< (except the case where
                                                ///< uncompressed extent follows
                                                ///< compressed one - see below).
    BlobInfo* blob_info_counted = nullptr; ///< std::set if previous allocation unit
                                           ///< caused expected_allocations
					   ///< counter increment at this blob.
                                           ///< if uncompressed extent follows 
                                           ///< a decrement for the 
                                	   ///< expected_allocations counter 
                                           ///< is needed
    int64_t expected_allocations = 0;      ///< new alloc units required in case
                                           ///< of gc fulfilled
    int64_t expected_for_release = 0;      ///< alloc units currently used by
                                           ///< compressed blobs that might
                                           ///< gone after GC

  protected:
    void process_protrusive_extents(const BlueStore::ExtentMap& extent_map, 
				    uint64_t start_offset,
				    uint64_t end_offset,
				    uint64_t start_touch_offset,
				    uint64_t end_touch_offset,
				    uint64_t min_alloc_size);
  };

  struct OnodeSpace;
  struct OnodeCacheShard;
  /// an in-memory object
  struct Onode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = 0;      ///< reference count
    std::atomic_int pin_nref = 0;  ///< reference count replica to track pinning
    Collection *c;
    ghobject_t oid;

    /// key under PREFIX_OBJ where we are stored
    mempool::bluestore_cache_meta::string key;

    boost::intrusive::list_member_hook<> lru_item;

    bluestore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;              ///< true if object logically exists
    bool cached;              ///< Onode is logically in the cache
                              /// (it can be pinned and hence physically out
                              /// of it at the moment though)
    ExtentMap extent_map;

    // track txc's that have not been committed to kv store (and whose
    // effects cannot be read via the kvdb read methods)
    std::atomic<int> flushing_count = {0};
    std::atomic<int> waiting_count = {0};
    /// protect flush_txns
    ceph::mutex flush_lock = ceph::make_mutex("BlueStore::Onode::flush_lock");
    ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns
    std::shared_ptr<int64_t> cache_age_bin;  ///< cache age bin

    Onode(Collection *c, const ghobject_t& o,
	  const mempool::bluestore_cache_meta::string& k)
      : c(c),
	oid(o),
	key(k),
	exists(false),
        cached(false),
	extent_map(this,
	  c->store->cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size) {
    }
    Onode(Collection* c, const ghobject_t& o,
      const std::string& k)
      : c(c),
        oid(o),
        key(k),
        exists(false),
        cached(false),
        extent_map(this,
	  c->store->cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size) {
    }
    Onode(Collection* c, const ghobject_t& o,
      const char* k)
      : c(c),
        oid(o),
        key(k),
        exists(false),
        cached(false),
        extent_map(this,
	  c->store->cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size) {
    }
    Onode(CephContext* cct)
      : c(nullptr),
        exists(false),
        cached(false),
        extent_map(this,
	  cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size) {
    }
    static void decode_raw(
      BlueStore::Onode* on,
      const bufferlist& v,
      ExtentMap::ExtentDecoder& dencoder);

    static Onode* create_decode(
      CollectionRef c,
      const ghobject_t& oid,
      const std::string& key,
      const ceph::buffer::list& v,
      bool allow_empty = false);

    void dump(ceph::Formatter* f) const;

    void flush();
    void get();
    void put();

    inline bool is_cached() const {
      return cached;
    }
    inline void set_cached() {
      ceph_assert(!cached);
      cached = true;
    }
    inline void clear_cached() {
      ceph_assert(cached);
      cached = false;
    }

    static const std::string& calc_omap_prefix(uint8_t flags);
    static void calc_omap_header(uint8_t flags, const Onode* o,
      std::string* out);
    static void calc_omap_key(uint8_t flags, const Onode* o,
      const std::string& key, std::string* out);
    static void calc_omap_tail(uint8_t flags, const Onode* o,
      std::string* out);

    const std::string& get_omap_prefix() {
      return calc_omap_prefix(onode.flags);
    }
    void get_omap_header(std::string* out) {
      calc_omap_header(onode.flags, this, out);
    }
    void get_omap_key(const std::string& key, std::string* out) {
      calc_omap_key(onode.flags, this, key, out);
    }
    void get_omap_tail(std::string* out) {
      calc_omap_tail(onode.flags, this, out);
    }

    void rewrite_omap_key(const std::string& old, std::string *out);
    void decode_omap_key(const std::string& key, std::string *user_key);

#ifdef HAVE_LIBZBD
    // Return the offset of an object on disk.  This function is intended *only*
    // for use with zoned storage devices because in these devices, the objects
    // are laid out contiguously on disk, which is not the case in general.
    // Also, it should always be called after calling extent_map.fault_range(),
    // so that the extent map is loaded.
    int64_t zoned_get_ondisk_starting_offset() const {
      return extent_map.extent_map.begin()->blob->
	  get_blob().calc_offset(0, nullptr);
    }
#endif
private:
    void _decode(const ceph::buffer::list& v);
  };
  typedef boost::intrusive_ptr<Onode> OnodeRef;

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

    virtual void shift_bins() {
      std::lock_guard l(lock);
      age_bins.push_front(std::make_shared<int64_t>(0));
    }
    virtual uint32_t get_bin_count() {
      std::lock_guard l(lock);
      return age_bins.capacity();
    }
    virtual void set_bin_count(uint32_t count) {
      std::lock_guard l(lock);
      age_bins.set_capacity(count);
    }
    virtual uint64_t sum_bins(uint32_t start, uint32_t end) {
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

  /// A Generic onode Cache Shard
  struct OnodeCacheShard : public CacheShard {
    std::array<std::pair<ghobject_t, ceph::mono_clock::time_point>, 64> dumped_onodes;

  public:
    OnodeCacheShard(CephContext* cct) : CacheShard(cct) {}
    static OnodeCacheShard *create(CephContext* cct, std::string type,
                                   PerfCounters *logger);

    //The following methods prefixed with '_' to be called under
    // Shard's lock
    virtual void _add(Onode* o, int level) = 0;
    virtual void _rm(Onode* o) = 0;
    virtual void _move_pinned(OnodeCacheShard *to, Onode *o) = 0;

    virtual void maybe_unpin(Onode* o) = 0;
    virtual void add_stats(uint64_t *onodes, uint64_t *pinned_onodes) = 0;
    bool empty() {
      return _get_num() == 0;
    }
  };

  /// A Generic buffer Cache Shard
  struct BufferCacheShard : public CacheShard {
    std::atomic<uint64_t> num_extents = {0};
    std::atomic<uint64_t> num_blobs = {0};
    uint64_t buffer_bytes = 0;

  public:
    BufferCacheShard(CephContext* cct) : CacheShard(cct) {}
    virtual ~BufferCacheShard() {
      ceph_assert(num_blobs == 0);
      ceph_assert(num_extents == 0);
    }
    static BufferCacheShard *create(CephContext* cct, std::string type, 
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

  struct OnodeSpace {
    OnodeCacheShard *cache;

  private:
    /// forward lookups
    mempool::bluestore_cache_meta::unordered_map<ghobject_t,OnodeRef> onode_map;

    friend struct Collection; // for split_cache()
    friend struct Onode; // for put()
    friend struct LruOnodeCacheShard;
    void _remove(const ghobject_t& oid);
  public:
    OnodeSpace(OnodeCacheShard *c) : cache(c) {}
    ~OnodeSpace() {
      clear();
    }

    OnodeRef add_onode(const ghobject_t& oid, OnodeRef& o);
    OnodeRef lookup(const ghobject_t& o);
    void rename(OnodeRef& o, const ghobject_t& old_oid,
		const ghobject_t& new_oid,
		const mempool::bluestore_cache_meta::string& new_okey);
    void clear();
    bool empty();

    template <int LogLevelV>
    void dump(CephContext *cct);

    /// return true if f true for any item
    bool map_any(std::function<bool(Onode*)> f);
  };

  class OpSequencer;
  using OpSequencerRef = ceph::ref_t<OpSequencer>;

  struct Collection : public CollectionImpl {
    BlueStore *store;
    OpSequencerRef osr;
    BufferCacheShard *cache;       ///< our cache shard
    bluestore_cnode_t cnode;
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("BlueStore::Collection::lock", true, false);

    bool exists;

    SharedBlobSet shared_blob_set;      ///< open SharedBlobs

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    OnodeSpace onode_space;

    //pool options
    pool_opts_t pool_opts;
    ContextQueue *commit_queue;

    OnodeCacheShard* get_onode_cache() const {
      return onode_space.cache;
    }
    OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop=false);

    // the terminology is confusing here, sorry!
    //
    //  blob_t     shared_blob_t
    //  !shared    unused                -> open
    //  shared     !loaded               -> open + shared
    //  shared     loaded                -> open + shared + loaded
    //
    // i.e.,
    //  open = SharedBlob is instantiated
    //  shared = blob_t shared flag is std::set; SharedBlob is hashed.
    //  loaded = SharedBlob::shared_blob_t is loaded from kv store
    void open_shared_blob(uint64_t sbid, BlobRef b);
    void load_shared_blob(SharedBlobRef sb);
    void make_blob_shared(uint64_t sbid, BlobRef b);
    uint64_t make_blob_unshared(SharedBlob *sb);

    BlobRef new_blob() {
      BlobRef b = new Blob();
      b->set_shared_blob(new SharedBlob(this));
      return b;
    }

    bool contains(const ghobject_t& oid) {
      if (cid.is_meta())
	return oid.hobj.pool == -1;
      spg_t spgid;
      if (cid.is_pg(&spgid))
	return
	  spgid.pgid.contains(cnode.bits, oid) &&
	  oid.shard_id == spgid.shard;
      return false;
    }

    int64_t pool() const {
      return cid.pool();
    }

    void split_cache(Collection *dest);

    bool flush_commit(Context *c) override;
    void flush() override;
    void flush_all_but_last();

    Collection(BlueStore *ns, OnodeCacheShard *oc, BufferCacheShard *bc, coll_t c);
  };

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {

    PerfCounters* logger = nullptr;
    CollectionRef c;
    OnodeRef o;
    KeyValueDB::Iterator it;
    std::string head, tail;

    std::string _stringify() const;
  public:
    OmapIteratorImpl(PerfCounters* l, CollectionRef c, OnodeRef& o, KeyValueDB::Iterator it);
    virtual ~OmapIteratorImpl();
    int seek_to_first() override;
    int upper_bound(const std::string &after) override;
    int lower_bound(const std::string &to) override;
    bool valid() override;
    int next() override;
    std::string key() override;
    ceph::buffer::list value() override;
    std::string tail_key() override {
      return tail;
    }

    int status() override {
      return 0;
    }
  };

  struct volatile_statfs{
    enum {
      STATFS_ALLOCATED = 0,
      STATFS_STORED,
      STATFS_COMPRESSED_ORIGINAL,
      STATFS_COMPRESSED,
      STATFS_COMPRESSED_ALLOCATED,
      STATFS_LAST
    };
    int64_t values[STATFS_LAST];
    volatile_statfs() {
      memset(this, 0, sizeof(volatile_statfs));
    }
    void reset() {
      *this = volatile_statfs();
    }
    bool empty() const {
      for (size_t i = 0; i < STATFS_LAST; ++i) {
	if (values[i]) {
	  return false;
	}
      }
      return true;
    }
    void publish(store_statfs_t* buf) const {
      buf->allocated = allocated();
      buf->data_stored = stored();
      buf->data_compressed = compressed();
      buf->data_compressed_original = compressed_original();
      buf->data_compressed_allocated = compressed_allocated();
    }

    volatile_statfs& operator+=(const volatile_statfs& other) {
      for (size_t i = 0; i < STATFS_LAST; ++i) {
	values[i] += other.values[i];
      }
      return *this;
    }
    int64_t& allocated() {
      return values[STATFS_ALLOCATED];
    }
    int64_t& stored() {
      return values[STATFS_STORED];
    }
    int64_t& compressed_original() {
      return values[STATFS_COMPRESSED_ORIGINAL];
    }
    int64_t& compressed() {
      return values[STATFS_COMPRESSED];
    }
    int64_t& compressed_allocated() {
      return values[STATFS_COMPRESSED_ALLOCATED];
    }
    int64_t allocated() const {
      return values[STATFS_ALLOCATED];
    }
    int64_t stored() const {
      return values[STATFS_STORED];
    }
    int64_t compressed_original() const {
      return values[STATFS_COMPRESSED_ORIGINAL];
    }
    int64_t compressed() const {
      return values[STATFS_COMPRESSED];
    }
    int64_t compressed_allocated() const {
      return values[STATFS_COMPRESSED_ALLOCATED];
    }
    volatile_statfs& operator=(const store_statfs_t& st) {
      values[STATFS_ALLOCATED] = st.allocated;
      values[STATFS_STORED] = st.data_stored;
      values[STATFS_COMPRESSED_ORIGINAL] = st.data_compressed_original;
      values[STATFS_COMPRESSED] = st.data_compressed;
      values[STATFS_COMPRESSED_ALLOCATED] = st.data_compressed_allocated;
      return *this;
    }
    bool is_empty() {
      return values[STATFS_ALLOCATED] == 0 &&
	values[STATFS_STORED] == 0 &&
	values[STATFS_COMPRESSED] == 0 &&
	values[STATFS_COMPRESSED_ORIGINAL] == 0 &&
	values[STATFS_COMPRESSED_ALLOCATED] == 0;
    }
    void decode(ceph::buffer::list::const_iterator& it) {
      using ceph::decode;
      for (size_t i = 0; i < STATFS_LAST; i++) {
	decode(values[i], it);
      }
    }

    void encode(ceph::buffer::list& bl) {
      using ceph::encode;
      for (size_t i = 0; i < STATFS_LAST; i++) {
	encode(values[i], bl);
      }
    }
  };

  struct TransContext final : public AioContext {
    MEMPOOL_CLASS_HELPERS();

    typedef enum {
      STATE_PREPARE,
      STATE_AIO_WAIT,
      STATE_IO_DONE,
      STATE_KV_QUEUED,     // queued for kv_sync_thread submission
      STATE_KV_SUBMITTED,  // submitted to kv; not yet synced
      STATE_KV_DONE,
      STATE_DEFERRED_QUEUED,    // in deferred_queue (pending or running)
      STATE_DEFERRED_CLEANUP,   // remove deferred kv record
      STATE_DEFERRED_DONE,
      STATE_FINISHING,
      STATE_DONE,
    } state_t;

    const char *get_state_name() {
      switch (state) {
      case STATE_PREPARE: return "prepare";
      case STATE_AIO_WAIT: return "aio_wait";
      case STATE_IO_DONE: return "io_done";
      case STATE_KV_QUEUED: return "kv_queued";
      case STATE_KV_SUBMITTED: return "kv_submitted";
      case STATE_KV_DONE: return "kv_done";
      case STATE_DEFERRED_QUEUED: return "deferred_queued";
      case STATE_DEFERRED_CLEANUP: return "deferred_cleanup";
      case STATE_DEFERRED_DONE: return "deferred_done";
      case STATE_FINISHING: return "finishing";
      case STATE_DONE: return "done";
      }
      return "???";
    }

#if defined(WITH_LTTNG)
    const char *get_state_latency_name(int state) {
      switch (state) {
      case l_bluestore_state_prepare_lat: return "prepare";
      case l_bluestore_state_aio_wait_lat: return "aio_wait";
      case l_bluestore_state_io_done_lat: return "io_done";
      case l_bluestore_state_kv_queued_lat: return "kv_queued";
      case l_bluestore_state_kv_committing_lat: return "kv_committing";
      case l_bluestore_state_kv_done_lat: return "kv_done";
      case l_bluestore_state_deferred_queued_lat: return "deferred_queued";
      case l_bluestore_state_deferred_cleanup_lat: return "deferred_cleanup";
      case l_bluestore_state_finishing_lat: return "finishing";
      case l_bluestore_state_done_lat: return "done";
      }
      return "???";
    }
#endif

    inline void set_state(state_t s) {
       state = s;
#ifdef WITH_BLKIN
       if (trace) {
         trace.event(get_state_name());
       } 
#endif
    }
    inline state_t get_state() {
      return state;
    }

    CollectionRef ch;
    OpSequencerRef osr;  // this should be ch->osr
    boost::intrusive::list_member_hook<> sequencer_item;

    uint64_t bytes = 0, ios = 0, cost = 0;

    std::set<OnodeRef> onodes;     ///< these need to be updated/written
    std::set<OnodeRef> modified_objects;  ///< objects we modified (and need a ref)

#ifdef HAVE_LIBZBD
    // zone refs to add/remove.  each zone ref is a (zone, offset) tuple.  The offset
    // is the first offset in the zone that the onode touched; subsequent writes
    // to that zone do not generate additional refs.  This is a bit imprecise but
    // is sufficient to generate reasonably sequential reads when doing zone
    // cleaning with less metadata than a ref for every extent.
    std::map<std::pair<OnodeRef, uint32_t>, uint64_t> new_zone_offset_refs;
    std::map<std::pair<OnodeRef, uint32_t>, uint64_t> old_zone_offset_refs;
#endif
    
    std::set<SharedBlobRef> shared_blobs;  ///< these need to be updated/written
    std::set<BlobRef> blobs_written; ///< update these on io completion
    KeyValueDB::Transaction t; ///< then we will commit this
    std::list<Context*> oncommits;  ///< more commit completions
    std::list<CollectionRef> removed_collections; ///< colls we removed

    boost::intrusive::list_member_hook<> deferred_queue_item;
    bluestore_deferred_transaction_t *deferred_txn = nullptr; ///< if any

    interval_set<uint64_t> allocated, released;
    volatile_statfs statfs_delta;	   ///< overall store statistics delta
    uint64_t osd_pool_id = META_POOL_ID;    ///< osd pool id we're operating on

    IOContext ioc;
    bool had_ios = false;  ///< true if we submitted IOs before our kv txn

    uint64_t seq = 0;
    ceph::mono_clock::time_point start;
    ceph::mono_clock::time_point last_stamp;

    uint64_t last_nid = 0;     ///< if non-zero, highest new nid we allocated
    uint64_t last_blobid = 0;  ///< if non-zero, highest new blobid we allocated

#if defined(WITH_LTTNG)
    bool tracing = false;
#endif

#ifdef WITH_BLKIN
    ZTracer::Trace trace;
#endif

    explicit TransContext(CephContext* cct, Collection *c, OpSequencer *o,
			  std::list<Context*> *on_commits)
      : ch(c),
	osr(o),
	ioc(cct, this),
	start(ceph::mono_clock::now()) {
      last_stamp = start;
      if (on_commits) {
	oncommits.swap(*on_commits);
      }
    }
    ~TransContext() {
#ifdef WITH_BLKIN
      if (trace) {
        trace.event("txc destruct");
      }
#endif
      delete deferred_txn;
    }

    void write_onode(OnodeRef& o) {
      onodes.insert(o);
    }
    void write_shared_blob(SharedBlobRef &sb) {
      shared_blobs.insert(sb);
    }
    void unshare_blob(SharedBlob *sb) {
      shared_blobs.erase(sb);
    }

    /// note we logically modified object (when onode itself is unmodified)
    void note_modified_object(OnodeRef& o) {
      // onode itself isn't written, though
      modified_objects.insert(o);
    }
    void note_removed_object(OnodeRef& o) {
      modified_objects.insert(o);
      onodes.erase(o);
    }

#ifdef HAVE_LIBZBD
    void note_write_zone_offset(OnodeRef& o, uint32_t zone, uint64_t offset) {
      o->onode.zone_offset_refs[zone] = offset;
      new_zone_offset_refs[std::make_pair(o, zone)] = offset;
    }
    void note_release_zone_offset(OnodeRef& o, uint32_t zone, uint64_t offset) {
      old_zone_offset_refs[std::make_pair(o, zone)] = offset;
      o->onode.zone_offset_refs.erase(zone);
    }
#endif

    void aio_finish(BlueStore *store) override {
      store->txc_aio_finish(this);
    }
  private:
    state_t state = STATE_PREPARE;
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
    void release_kv_throttle(uint64_t cost) {
      throttle_bytes.put(cost);
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

  typedef boost::intrusive::list<
    TransContext,
    boost::intrusive::member_hook<
      TransContext,
      boost::intrusive::list_member_hook<>,
      &TransContext::deferred_queue_item> > deferred_queue_t;

  struct DeferredBatch final : public AioContext {
    OpSequencer *osr;
    struct deferred_io {
      ceph::buffer::list bl;    ///< data
      uint64_t seq;     ///< deferred transaction seq
    };
    std::map<uint64_t,deferred_io> iomap; ///< map of ios in this batch
    deferred_queue_t txcs;           ///< txcs in this batch
    IOContext ioc;                   ///< our aios
    /// bytes of pending io for each deferred seq (may be 0)
    std::map<uint64_t,int> seq_bytes;

    void _discard(CephContext *cct, uint64_t offset, uint64_t length);
    void _audit(CephContext *cct);

    DeferredBatch(CephContext *cct, OpSequencer *osr)
      : osr(osr), ioc(cct, this) {}

    /// prepare a write
    void prepare_write(CephContext *cct,
		       uint64_t seq, uint64_t offset, uint64_t length,
		       ceph::buffer::list::const_iterator& p);

    void aio_finish(BlueStore *store) override {
      store->_deferred_aio_finish(osr);
    }
  };

  class OpSequencer : public RefCountedObject {
  public:
    ceph::mutex qlock = ceph::make_mutex("BlueStore::OpSequencer::qlock");
    ceph::condition_variable qcond;
    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
        TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    boost::intrusive::list_member_hook<> deferred_osr_queue_item;

    DeferredBatch *deferred_running = nullptr;
    DeferredBatch *deferred_pending = nullptr;

    ceph::mutex deferred_lock = ceph::make_mutex("BlueStore::OpSequencer::deferred_lock");

    BlueStore *store;
    coll_t cid;

    uint64_t last_seq = 0;

    std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io

    std::atomic_int kv_committing_serially = {0};

    std::atomic_int kv_submitted_waiters = {0};

    std::atomic_bool zombie = {false};    ///< in zombie_osr std::set (collection going away)

    const uint32_t sequencer_id;

    uint32_t get_sequencer_id() const {
      return sequencer_id;
    }

    void queue_new(TransContext *txc) {
      std::lock_guard l(qlock);
      txc->seq = ++last_seq;
      q.push_back(*txc);
    }

    void drain() {
      std::unique_lock l(qlock);
      while (!q.empty())
	qcond.wait(l);
    }

    void drain_preceding(TransContext *txc) {
      std::unique_lock l(qlock);
      while (&q.front() != txc)
	qcond.wait(l);
    }

    bool _is_all_kv_submitted() {
      // caller must hold qlock & q.empty() must not empty
      ceph_assert(!q.empty());
      TransContext *txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_KV_SUBMITTED) {
	return true;
      }
      return false;
    }

    void flush() {
      std::unique_lock l(qlock);
      while (true) {
	// std::set flag before the check because the condition
	// may become true outside qlock, and we need to make
	// sure those threads see waiters and signal qcond.
	++kv_submitted_waiters;
	if (q.empty() || _is_all_kv_submitted()) {
	  --kv_submitted_waiters;
	  return;
	}
	qcond.wait(l);
	--kv_submitted_waiters;
      }
    }

    void flush_all_but_last() {
      std::unique_lock l(qlock);
      ceph_assert (q.size() >= 1);
      while (true) {
	// std::set flag before the check because the condition
	// may become true outside qlock, and we need to make
	// sure those threads see waiters and signal qcond.
	++kv_submitted_waiters;
	if (q.size() <= 1) {
	  --kv_submitted_waiters;
	  return;
	} else {
	  auto it = q.rbegin();
	  it++;
	  if (it->get_state() >= TransContext::STATE_KV_SUBMITTED) {
	    --kv_submitted_waiters;
	    return;
          }
	}
	qcond.wait(l);
	--kv_submitted_waiters;
      }
      }

    bool flush_commit(Context *c) {
      std::lock_guard l(qlock);
      if (q.empty()) {
	return true;
      }
      TransContext *txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_KV_DONE) {
	return true;
      }
      txc->oncommits.push_back(c);
      return false;
    }
  private:
    FRIEND_MAKE_REF(OpSequencer);
    OpSequencer(BlueStore *store, uint32_t sequencer_id, const coll_t& c)
      : RefCountedObject(store->cct),
	store(store), cid(c), sequencer_id(sequencer_id) {
    }
    ~OpSequencer() {
      ceph_assert(q.empty());
    }
  };

  typedef boost::intrusive::list<
    OpSequencer,
    boost::intrusive::member_hook<
      OpSequencer,
      boost::intrusive::list_member_hook<>,
      &OpSequencer::deferred_osr_queue_item> > deferred_osr_queue_t;

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

#ifdef HAVE_LIBZBD
  struct ZonedCleanerThread : public Thread {
    BlueStore *store;
    explicit ZonedCleanerThread(BlueStore *s) : store(s) {}
    void *entry() override {
      store->_zoned_cleaner_thread();
      return nullptr;
    }
  };
#endif
  
  struct BigDeferredWriteContext {
    uint64_t off = 0;     // original logical offset
    uint32_t b_off = 0;   // blob relative offset
    uint32_t used = 0;
    uint64_t head_read = 0;
    uint64_t tail_read = 0;
    BlobRef blob_ref;
    uint64_t blob_start = 0;
    PExtentVector res_extents;

    inline uint64_t blob_aligned_len() const {
      return used + head_read + tail_read;
    }

    bool can_defer(BlueStore::extent_map_t::iterator ep,
      uint64_t prefer_deferred_size,
      uint64_t block_size,
      uint64_t offset,
      uint64_t l);
    bool apply_defer();
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

  Allocator *alloc = nullptr;   ///< allocator consumed by BlueStore
  bluefs_shared_alloc_context_t shared_alloc; ///< consumed by BlueFS (may be == alloc)

  uuid_d fsid;
  int path_fd = -1;  ///< open handle to $path
  int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
  bool mounted = false;

  // store open_db options:
  bool db_was_opened_read_only = true;
  bool need_to_destage_allocation_file = false;

  ///< rwlock to protect coll_map/new_coll_map
  ceph::shared_mutex coll_lock = ceph::make_shared_mutex("BlueStore::coll_lock");
  mempool::bluestore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
  bool collections_had_errors = false;
  std::map<coll_t,CollectionRef> new_coll_map;

  mempool::bluestore_cache_buffer::vector<BufferCacheShard*> buffer_cache_shards;
  mempool::bluestore_cache_onode::vector<OnodeCacheShard*> onode_cache_shards;

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
  deferred_osr_queue_t deferred_queue; ///< osr's with deferred io pending
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

#ifdef HAVE_LIBZBD
  ZonedCleanerThread zoned_cleaner_thread;
  ceph::mutex zoned_cleaner_lock = ceph::make_mutex("BlueStore::zoned_cleaner_lock");
  ceph::condition_variable zoned_cleaner_cond;
  bool zoned_cleaner_started = false;
  bool zoned_cleaner_stop = false;
  std::deque<uint64_t> zoned_cleaner_queue;
#endif

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

  // smr-only
  uint64_t zone_size = 0;              ///< number of SMR zones 
  uint64_t first_sequential_zone = 0;  ///< first SMR zone that is sequential-only

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
  CompressorRef compressor;
  std::atomic<uint64_t> comp_min_blob_size = {0};
  std::atomic<uint64_t> comp_max_blob_size = {0};

  std::atomic<uint64_t> max_blob_size = {0};  ///< maximum blob size

  uint64_t kv_ios = 0;
  uint64_t kv_throttle_costs = 0;

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

  typedef std::map<uint64_t, volatile_statfs> osd_pools_map;

  ceph::mutex vstatfs_lock = ceph::make_mutex("BlueStore::vstatfs_lock");
  volatile_statfs vstatfs;
  osd_pools_map osd_pools; // protected by vstatfs_lock as well

  bool per_pool_stat_collection = true;

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

      virtual uint32_t get_bin_count() const {
        return store->onode_cache_shards[0]->get_bin_count();
      }
      virtual void set_bin_count(uint32_t count) {
        for (auto i : store->onode_cache_shards) {
          i->set_bin_count(count);
        }
      }
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
      virtual void shift_bins() {
        for (auto i : store->onode_cache_shards) {
          i->shift_bins();
        }
      }
      virtual uint64_t _sum_bins(uint32_t start, uint32_t end) const {
        uint64_t onodes = 0;
	for (auto i : store->onode_cache_shards) {
	  onodes += i->sum_bins(start, end);
	}
	return onodes*get_bytes_per_onode();
      }
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
  int _open_db_and_around(bool read_only, bool to_repair = false);
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
  int _init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments);
  void _post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments);
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

  static int _write_bdev_label(CephContext* cct,
			       const std::string &path, bluestore_bdev_label_t label);
  static int _read_bdev_label(CephContext* cct, const std::string &path,
			      bluestore_bdev_label_t *label);
private:
  int _check_or_set_bdev_label(std::string path, uint64_t size, std::string desc,
			       bool create);
  int _set_bdev_label_size(const std::string& path, uint64_t size);

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

  template <int LogLevelV>
  friend void _dump_onode(CephContext *cct, const Onode& o);
  template <int LogLevelV>
  friend void _dump_extent_map(CephContext *cct, const ExtentMap& em);
  template <int LogLevelV>
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

#ifdef HAVE_LIBZBD
  void _zoned_cleaner_start();
  void _zoned_cleaner_stop();
  void _zoned_cleaner_thread();
  void _zoned_clean_zone(uint64_t zone_num,
			 class ZonedAllocator *a,
			 class ZonedFreelistManager *f);
  void _clean_some(ghobject_t oid, uint32_t zone_num);
#endif

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

  int _fsck(FSCKDepth depth, bool repair);
  int _fsck_on_open(BlueStore::FSCKDepth depth, bool repair);

  void _buffer_cache_write(
    TransContext *txc,
    BlobRef b,
    uint64_t offset,
    ceph::buffer::list& bl,
    unsigned flags) {
    b->dirty_bc().write(b->shared_blob->get_cache(), txc->seq, offset, bl,
			     flags);
    txc->blobs_written.insert(b);
  }

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
public:
  int mount() override {
    return _mount();
  }
  int umount() override;

  int open_db_environment(KeyValueDB **pdb, bool to_repair);
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
  int quick_fix() override {
    return _fsck(FSCK_SHALLOW, true);
  }

  void set_cache_shards(unsigned num) override;
  void dump_cache_stats(ceph::Formatter *f) override {
    int onode_count = 0, buffers_bytes = 0;
    for (auto i: onode_cache_shards) {
      onode_count += i->_get_num();
    }
    for (auto i: buffer_cache_shards) {
      buffers_bytes += i->_get_bytes();
    }
    f->dump_int("bluestore_onode", onode_count);
    f->dump_int("bluestore_buffers", buffers_bytes);
  }
  void dump_cache_stats(std::ostream& ss) override {
    int onode_count = 0, buffers_bytes = 0;
    for (auto i: onode_cache_shards) {
      onode_count += i->_get_num();
    }
    for (auto i: buffer_cache_shards) {
      buffers_bytes += i->_get_bytes();
    }
    ss << "bluestore_onode: " << onode_count;
    ss << "bluestore_buffers: " << buffers_bytes;
  }

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
    logger->dump_formatted(f, false, false);
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

  int dump_bluefs_sizes(std::ostream& out);

public:
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

  int _do_read(
    Collection *c,
    OnodeRef& o,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0,
    uint64_t retry_count = 0);

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

  /// Get keys defined on oid
  int omap_get_keys(
    CollectionHandle &c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    std::set<std::string> *keys      ///< [out] Keys defined on oid
    ) override;

  /// Get key values
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::set<std::string> &keys,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;

#ifdef WITH_SEASTAR
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::optional<std::string> &start_after,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;
#endif

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle &c,   ///< [in] collection
    const ghobject_t &oid  ///< [in] object
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

  void compact() override {
    ceph_assert(db);
    db->compact();
  }
  bool has_builtin_csum() const override {
    return true;
  }
  // a debug punch_hole function, to use internals of _wctx_finish
  // to remove old_extents from object
  void debug_punch_hole(
    CollectionRef& c,
    OnodeRef& o,
    uint32_t off,
    uint32_t len) {
    BlueStore::TransContext txc(cct, c.get(), nullptr, nullptr);
    BlueStore::WriteContext wctx;
    o->extent_map.punch_hole(c, off, len, &wctx.old_extents);
    _wctx_finish(&txc, c, o, &wctx, nullptr);
  }

  inline void log_latency(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    const char* info = "",
    int idx2 = l_bluestore_first) const;

  inline void log_latency_fn(const char* name,
    int idx,
    const ceph::timespan& lat,
    double lat_threshold,
    std::function<std::string (const ceph::timespan& lat)> fn,
    int idx2 = l_bluestore_first) const;

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
    uint64_t logical_offset) const;
  int _decompress(ceph::buffer::list& source, ceph::buffer::list* result);


  // --------------------------------------------------------
  // write ops

  struct WriteContext {
    bool buffered = false;          ///< buffered write
    bool compress = false;          ///< compressed write
    uint64_t target_blob_size = 0;  ///< target (max) blob size
    unsigned csum_order = 0;        ///< target checksum chunk order

    old_extent_map_t old_extents;   ///< must deref these blobs
    interval_set<uint64_t> extents_to_gc; ///< extents for garbage collection

    struct write_item {
      uint64_t logical_offset;      ///< write logical offset
      BlobRef b;
      uint64_t blob_length;
      uint64_t b_off;
      ceph::buffer::list bl;
      uint64_t b_off0; ///< original offset in a blob prior to padding
      uint64_t length0; ///< original data length prior to padding

      bool mark_unused;
      bool new_blob; ///< whether new blob was created

      bool compressed = false;
      ceph::buffer::list compressed_bl;
      size_t compressed_len = 0;

      write_item(
	uint64_t logical_offs,
        BlobRef b,
        uint64_t blob_len,
        uint64_t o,
        ceph::buffer::list& bl,
        uint64_t o0,
        uint64_t l0,
        bool _mark_unused,
	bool _new_blob)
       :
         logical_offset(logical_offs),
         b(b),
         blob_length(blob_len),
         b_off(o),
         bl(bl),
         b_off0(o0),
         length0(l0),
         mark_unused(_mark_unused),
	 new_blob(_new_blob) {}
    };
    std::vector<write_item> writes;                 ///< blobs we're writing

    /// partial clone of the context
    void fork(const WriteContext& other) {
      buffered = other.buffered;
      compress = other.compress;
      target_blob_size = other.target_blob_size;
      csum_order = other.csum_order;
    }
    void write(
      uint64_t loffs,
      BlobRef b,
      uint64_t blob_len,
      uint64_t o,
      ceph::buffer::list& bl,
      uint64_t o0,
      uint64_t len0,
      bool _mark_unused,
      bool _new_blob) {
      writes.emplace_back(loffs,
                          b,
                          blob_len,
                          o,
                          bl,
                          o0,
                          len0,
                          _mark_unused,
                          _new_blob);
    }
    /// Checks for writes to the same pextent within a blob
    bool has_conflict(
      BlobRef b,
      uint64_t loffs,
      uint64_t loffs_end,
      uint64_t min_alloc_size);
  };
  void _do_write_small(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    ceph::buffer::list::iterator& blp,
    WriteContext *wctx);
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
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       const std::string& name,
	       ceph::buffer::ptr& val);
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
  class ExtentDecoderPartial : public ExtentMap::ExtentDecoder {
    BlueStore& store;
    read_alloc_stats_t& stats;
    SimpleBitmap& sbmap;
    sb_info_space_efficient_map_t& sb_info;
    uint8_t min_alloc_size_order;
    Extent extent;
    ghobject_t oid;
    volatile_statfs* per_pool_statfs = nullptr;
    blob_map_t blobs;
    blob_map_t spanning_blobs;

    void _consume_new_blob(bool spanning,
                           uint64_t extent_no,
                           uint64_t sbid,
                           BlobRef b);
  protected:
    void consume_blobid(Extent*, bool spanning, uint64_t blobid) override;
    void consume_blob(Extent* le,
                      uint64_t extent_no,
                      uint64_t sbid,
                      BlobRef b) override;
    void consume_spanning_blob(uint64_t sbid, BlobRef b) override;
    Extent* get_next_extent() override {
      ++stats.extent_count;
      extent = Extent();
      return &extent;
    }
    void add_extent(Extent*) override {
    }
  public:
    ExtentDecoderPartial(BlueStore& _store,
                         read_alloc_stats_t& _stats,
                         SimpleBitmap& _sbmap,
                         sb_info_space_efficient_map_t& _sb_info,
                         uint8_t _min_alloc_size_order)
      : store(_store), stats(_stats), sbmap(_sbmap), sb_info(_sb_info),
        min_alloc_size_order(_min_alloc_size_order)
    {}
    const ghobject_t& get_oid() const {
      return oid;
    }
    void reset(const ghobject_t _oid,
      volatile_statfs* _per_pool_statfs);
  };

  friend std::ostream& operator<<(std::ostream& out, const read_alloc_stats_t& stats) {
    out << "==========================================================" << std::endl;
    out << "NCB::onode_count             = " ;out.width(10);out << stats.onode_count << std::endl
	<< "NCB::shard_count             = " ;out.width(10);out << stats.shard_count << std::endl
	<< "NCB::shared_blob_count      = " ;out.width(10);out << stats.shared_blob_count << std::endl
	<< "NCB::compressed_blob_count   = " ;out.width(10);out << stats.compressed_blob_count << std::endl
	<< "NCB::spanning_blob_count     = " ;out.width(10);out << stats.spanning_blob_count << std::endl
	<< "NCB::skipped_illegal_extent  = " ;out.width(10);out << stats.skipped_illegal_extent << std::endl
	<< "NCB::extent_count            = " ;out.width(10);out << stats.extent_count << std::endl
	<< "NCB::insert_count            = " ;out.width(10);out << stats.insert_count << std::endl;

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
};

inline std::ostream& operator<<(std::ostream& out, const BlueStore::volatile_statfs& s) {
  return out 
    << " allocated:"
      << s.values[BlueStore::volatile_statfs::STATFS_ALLOCATED]
    << " stored:"
      << s.values[BlueStore::volatile_statfs::STATFS_STORED]
    << " compressed:"
      << s.values[BlueStore::volatile_statfs::STATFS_COMPRESSED]
    << " compressed_orig:"
      << s.values[BlueStore::volatile_statfs::STATFS_COMPRESSED_ORIGINAL]
    << " compressed_alloc:"
      << s.values[BlueStore::volatile_statfs::STATFS_COMPRESSED_ALLOCATED];
}

static inline void intrusive_ptr_add_ref(BlueStore::Onode *o) {
  o->get();
}
static inline void intrusive_ptr_release(BlueStore::Onode *o) {
  o->put();
}

static inline void intrusive_ptr_add_ref(BlueStore::OpSequencer *o) {
  o->get();
}
static inline void intrusive_ptr_release(BlueStore::OpSequencer *o) {
  o->put();
}

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

class RocksDBBlueFSVolumeSelector : public BlueFSVolumeSelector
{
  template <class T, size_t MaxX, size_t MaxY>
  class matrix_2d {
    T values[MaxX][MaxY];
  public:
    matrix_2d() {
      clear();
    }
    T& at(size_t x, size_t y) {
      ceph_assert(x < MaxX);
      ceph_assert(y < MaxY);

      return values[x][y];
    }
    size_t get_max_x() const {
      return MaxX;
    }
    size_t get_max_y() const {
      return MaxY;
    }
    void clear() {
      memset(values, 0, sizeof(values));
    }
  };

  enum {
    // use 0/nullptr as unset indication
    LEVEL_FIRST = 1,
    LEVEL_LOG = LEVEL_FIRST, // BlueFS log
    LEVEL_WAL,
    LEVEL_DB,
    LEVEL_SLOW,
    LEVEL_MAX
  };
  // add +1 row for corresponding per-device totals
  // add +1 column for per-level actual (taken from file size) total
  typedef matrix_2d<std::atomic<uint64_t>, BlueFS::MAX_BDEV + 1, LEVEL_MAX - LEVEL_FIRST + 1> per_level_per_dev_usage_t;

  per_level_per_dev_usage_t per_level_per_dev_usage;
  // file count per level, add +1 to keep total file count
  std::atomic<uint64_t> per_level_files[LEVEL_MAX - LEVEL_FIRST + 1] = { 0 };

  // Note: maximum per-device totals below might be smaller than corresponding
  // perf counters by up to a single alloc unit (1M) due to superblock extent.
  // The later is not accounted here.
  per_level_per_dev_usage_t per_level_per_dev_max;

  uint64_t l_totals[LEVEL_MAX - LEVEL_FIRST];
  uint64_t db_avail4slow = 0;
  uint64_t level0_size = 0;
  uint64_t level_base = 0;
  uint64_t level_multiplier = 0;
  enum {
    OLD_POLICY,
    USE_SOME_EXTRA
  };

public:
  RocksDBBlueFSVolumeSelector(
    uint64_t _wal_total,
    uint64_t _db_total,
    uint64_t _slow_total,
    uint64_t _level0_size,
    uint64_t _level_base,
    uint64_t _level_multiplier,
    double reserved_factor,
    uint64_t reserved,
    bool new_pol)
  {
    l_totals[LEVEL_LOG - LEVEL_FIRST] = 0; // not used at the moment
    l_totals[LEVEL_WAL - LEVEL_FIRST] = _wal_total;
    l_totals[LEVEL_DB - LEVEL_FIRST] = _db_total;
    l_totals[LEVEL_SLOW - LEVEL_FIRST] = _slow_total;

    if (!new_pol) {
      return;
    }
    // Calculating how much extra space is available at DB volume.
    // Depending on the presence of explicit reserved size specification it might be either
    // * DB volume size - reserved
    // or
    // * DB volume size - sum_max_level_size(0, L-1) - max_level_size(L) * reserved_factor
    if (!reserved) {
      level0_size = _level0_size;
      level_base = _level_base;
      level_multiplier = _level_multiplier;
      uint64_t prev_levels = _level0_size;
      uint64_t cur_level = _level_base;
      uint64_t cur_threshold = prev_levels + cur_level;
      do {
	uint64_t next_level = cur_level * _level_multiplier;
        uint64_t next_threshold = prev_levels + cur_level + next_level;
        if (_db_total <= next_threshold) {
	  cur_threshold *= reserved_factor;
          db_avail4slow = cur_threshold < _db_total ? _db_total - cur_threshold : 0;
          break;
        } else {
          prev_levels += cur_level;
          cur_level = next_level;
          cur_threshold = next_threshold;
        }
      } while (true);
    } else {
      db_avail4slow = reserved < _db_total ? _db_total - reserved : 0;
    }
  }

  void* get_hint_for_log() const override {
    return  reinterpret_cast<void*>(LEVEL_LOG);
  }
  void* get_hint_by_dir(std::string_view dirname) const override;

  void add_usage(void* hint, const bluefs_extent_t& extent) override {
    if (hint == nullptr)
      return;
    size_t pos = (size_t)hint - LEVEL_FIRST;
    auto& cur = per_level_per_dev_usage.at(extent.bdev, pos);
    auto& max = per_level_per_dev_max.at(extent.bdev, pos);
    uint64_t v = cur.fetch_add(extent.length) + extent.length;
    while (v > max) {
      max.exchange(v);
    }
    {
      //update per-device totals
      auto& cur = per_level_per_dev_usage.at(extent.bdev, LEVEL_MAX - LEVEL_FIRST);
      auto& max = per_level_per_dev_max.at(extent.bdev, LEVEL_MAX - LEVEL_FIRST);
      uint64_t v = cur.fetch_add(extent.length) + extent.length;
      while (v > max) {
	max.exchange(v);
      }
    }
  }
  void sub_usage(void* hint, const bluefs_extent_t& extent) override {
    if (hint == nullptr)
      return;
    size_t pos = (size_t)hint - LEVEL_FIRST;
    auto& cur = per_level_per_dev_usage.at(extent.bdev, pos);
    ceph_assert(cur >= extent.length);
    cur -= extent.length;

    //update per-device totals
    auto& cur2 = per_level_per_dev_usage.at(extent.bdev, LEVEL_MAX - LEVEL_FIRST);
    ceph_assert(cur2 >= extent.length);
    cur2 -= extent.length;
  }
  void add_usage(void* hint, uint64_t size_more, bool upd_files) override {
    if (hint == nullptr)
      return;
    size_t pos = (size_t)hint - LEVEL_FIRST;
    //update per-level actual totals
    auto& cur = per_level_per_dev_usage.at(BlueFS::MAX_BDEV, pos);
    auto& max = per_level_per_dev_max.at(BlueFS::MAX_BDEV, pos);
    uint64_t v = cur.fetch_add(size_more) + size_more;
    while (v > max) {
      max.exchange(v);
    }
    if (upd_files) {
      ++per_level_files[pos];
      ++per_level_files[LEVEL_MAX - LEVEL_FIRST];
    }
  }
  void sub_usage(void* hint, uint64_t size_less, bool upd_files) override {
    if (hint == nullptr)
      return;
    size_t pos = (size_t)hint - LEVEL_FIRST;
    //update per-level actual totals
    auto& cur = per_level_per_dev_usage.at(BlueFS::MAX_BDEV, pos);
    ceph_assert(cur >= size_less);
    cur -= size_less;
    if (upd_files) {
      ceph_assert(per_level_files[pos] > 0);
      --per_level_files[pos];
      ceph_assert(per_level_files[LEVEL_MAX - LEVEL_FIRST] > 0);
      --per_level_files[LEVEL_MAX - LEVEL_FIRST];
    }
  }

  uint8_t select_prefer_bdev(void* h) override;
  void get_paths(
    const std::string& base,
    BlueFSVolumeSelector::paths& res) const override;

  void dump(std::ostream& sout) override;
  BlueFSVolumeSelector* clone_empty() const override;
  bool compare(BlueFSVolumeSelector* other) override;
};

#endif
