// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_COMPONENTS_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_COMPONENTS_H

#include "BlueStore_objects_impl.h"
#include "BlueStore.h"

namespace bluestore {

  struct OnodeSpace {
    BlueStore::OnodeCacheShard* cache;

  private:
    /// forward lookups
    mempool::bluestore_cache_meta::unordered_map<ghobject_t, OnodeRef> onode_map;

    friend struct Collection;         // for split_cache()
    friend struct Onode;              // for put()
    friend struct LruOnodeCacheShard; // for _remove()
    void _remove(const ghobject_t& oid);
  public:
    OnodeSpace(BlueStore::OnodeCacheShard* c) : cache(c) {}
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
    void dump(CephContext* cct);

    /// return true if f true for any item
    bool map_any(std::function<bool(Onode*)> f);
  };

  std::ostream& operator<<(std::ostream& out, const bluestore::SharedBlob& sb);

  /// a lookup table of SharedBlobs
  struct SharedBlobSet {
    /// protect lookup, insertion, removal
    ceph::mutex lock = ceph::make_mutex("BlueStore::SharedBlobSet::lock");

    // we use a bare pointer because we don't want to affect the ref
    // count
    mempool::bluestore_cache_meta::unordered_map<uint64_t, SharedBlob*> sb_map;

    SharedBlobRef lookup(uint64_t sbid);

    void add(Collection* coll, SharedBlob* sb);

    bool remove(SharedBlob* sb, bool verify_nref_is_zero = false);

    bool empty() {
      std::lock_guard l(lock);
      return sb_map.empty();
    }
    template <int LogLevelV>
    void dump(CephContext * cct) {
      std::lock_guard l(lock);
      for (auto& i : sb_map) {
	lgeneric_subdout(cct, bluestore, LogLevelV) << i.first << " : " << *i.second << dendl;
      }
    }
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
      const bluestore::ExtentMap& extent_map,
      const bluestore::OldExtentMap& old_extents,
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
  struct AioContext {
    virtual void aio_finish(BlueStore* store) = 0;
    virtual ~AioContext() {}
  };

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

  // Declaring through a struct to be able to have forward declarations
  struct OldExtentMap :
    public boost::intrusive::list<
      OldExtent,
      boost::intrusive::member_hook<
	OldExtent,
	boost::intrusive::list_member_hook<>,
	&OldExtent::old_extent_item> > {
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

    const char* get_state_name() {
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
    const char* get_state_latency_name(int state) {
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

    std::set<SharedBlobRef> shared_blobs;  ///< these need to be updated/written

    KeyValueDB::Transaction t; ///< then we will commit this
    std::list<Context*> oncommits;  ///< more commit completions
    std::list<CollectionRef> removed_collections; ///< colls we removed

    boost::intrusive::list_member_hook<> deferred_queue_item;
    bluestore_deferred_transaction_t* deferred_txn = nullptr; ///< if any

    interval_set<uint64_t> allocated, released;
    volatile_statfs statfs_delta;	   ///< overall store statistics delta
    uint64_t osd_pool_id = META_POOL_ID;    ///< osd pool id we're operating on

    IOContext ioc;
    bool had_ios = false;  ///< true if we submitted IOs before our kv txn

    //uint64_t seq = 0;
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

    ceph::mutex writings_lock = ceph::make_mutex("bluestore::TransContextWritings::lock");
    struct WriteObserverEntry {
      Onode* onode;
      uint32_t offset;
      uint32_t length;
      WriteObserverEntry(Onode* _o, uint32_t off, uint32_t len)
	: onode(_o), offset(off), length(len) {
      }
    };
    using write_list_t = mempool::bluestore_writing::list<WriteObserverEntry>;
    write_list_t writings;
    bool were_writings = false;

    bool add_writing(Onode* o, uint32_t off, uint32_t len);
    void finish_writing();

    explicit TransContext(CephContext* cct, BlueStore::Collection* c, OpSequencer* o,
      std::list<Context*>* on_commits)
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

    inline void write_onode(OnodeRef& o) {
      onodes.insert(o);
    }
    inline void write_shared_blob(const SharedBlobRef& sb) {
      shared_blobs.insert(sb);
    }
    inline void unshare_blob(SharedBlobRef sb) {
      shared_blobs.erase(sb);
    }

    /// note we logically modified object (when onode itself is unmodified)
    inline void note_modified_object(OnodeRef& o) {
      // onode itself isn't written, though
      modified_objects.insert(o);
    }
    inline void note_removed_object(OnodeRef& o) {
      modified_objects.insert(o);
      onodes.erase(o);
    }

    void aio_finish(BlueStore* store) override {
      store->txc_aio_finish(this);
    }
  private:
    state_t state = STATE_PREPARE;
  };
  typedef boost::intrusive::list<
    TransContext,
    boost::intrusive::member_hook<
    TransContext,
    boost::intrusive::list_member_hook<>,
    &TransContext::deferred_queue_item> > deferred_queue_t;

  struct DeferredBatch final : public AioContext {
    OpSequencer* osr;
    struct deferred_io {
      ceph::buffer::list bl;    ///< data
      uint64_t seq;     ///< deferred transaction seq
    };
    std::map<uint64_t, deferred_io> iomap; ///< map of ios in this batch
    deferred_queue_t txcs;           ///< txcs in this batch
    IOContext ioc;                   ///< our aios
#if defined(DEBUG_DEFERRED)
    /// bytes of pending io for each deferred seq (may be 0)
    std::map<uint64_t, int> seq_bytes;
    void _audit(CephContext* cct);
#endif
    void _discard(CephContext* cct, uint64_t offset, uint64_t length);

    DeferredBatch(CephContext* cct, OpSequencer* osr)
      : osr(osr), ioc(cct, this) {
    }

    /// prepare a write
    void prepare_write(CephContext* cct,
      uint64_t seq, uint64_t offset, uint64_t length,
      ceph::buffer::list::const_iterator& p);

    void aio_finish(BlueStore* store) override {
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

    DeferredBatch* deferred_running = nullptr;
    DeferredBatch* deferred_pending = nullptr;

    ceph::mutex deferred_lock = ceph::make_mutex("BlueStore::OpSequencer::deferred_lock");

    BlueStore* store;
    coll_t cid;

    std::atomic_int txc_with_unstable_io = { 0 };  ///< num txcs with unstable io

    std::atomic_int kv_committing_serially = { 0 };

    std::atomic_int kv_submitted_waiters = { 0 };

    std::atomic_bool zombie = { false };    ///< in zombie_osr std::set (collection going away)

    const uint32_t sequencer_id;

    uint32_t get_sequencer_id() const {
      return sequencer_id;
    }

    void queue_new(TransContext* txc) {
      std::lock_guard l(qlock);
      q.push_back(*txc);
    }
    void undo_queue(TransContext* txc) {
      std::lock_guard l(qlock);
      ceph_assert(&q.back() == txc);
      q.pop_back();
    }

    void drain() {
      std::unique_lock l(qlock);
      while (!q.empty())
	qcond.wait(l);
    }

    void drain_preceding(TransContext* txc) {
      std::unique_lock l(qlock);
      while (&q.front() != txc)
	qcond.wait(l);
    }

    bool _is_all_kv_submitted() {
      // caller must hold qlock & q.empty() must not empty
      ceph_assert(!q.empty());
      TransContext* txc = &q.back();
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
      ceph_assert(q.size() >= 1);
      while (true) {
	// std::set flag before the check because the condition
	// may become true outside qlock, and we need to make
	// sure those threads see waiters and signal qcond.
	++kv_submitted_waiters;
	if (q.size() <= 1) {
	  --kv_submitted_waiters;
	  return;
	}
	else {
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

    bool flush_commit(Context* c) {
      std::lock_guard l(qlock);
      if (q.empty()) {
	return true;
      }
      TransContext* txc = &q.back();
      if (txc->get_state() >= TransContext::STATE_KV_DONE) {
	return true;
      }
      txc->oncommits.push_back(c);
      return false;
    }
  private:
    FRIEND_MAKE_REF(OpSequencer);
    OpSequencer(BlueStore* store, uint32_t sequencer_id, const coll_t& c)
      : RefCountedObject(store->cct),
      store(store), cid(c), sequencer_id(sequencer_id) {
    }
    ~OpSequencer() {
      ceph_assert(q.empty());
    }
  };

  struct deferred_osr_queue_t : public
    boost::intrusive::list<
    OpSequencer,
    boost::intrusive::member_hook<
    OpSequencer,
    boost::intrusive::list_member_hook<>,
    &OpSequencer::deferred_osr_queue_item> > {
  };

  struct WriteContext {
    bool buffered = false;          ///< buffered write
    bool compress = false;          ///< compressed write
    CompressorRef compressor;       ///< effective compression engine
    double crr = 0.0;               ///< compression required ratio
    uint8_t csum_type = 0;          ///< checksum type for new blobs
    unsigned csum_order = 0;        ///< target checksum chunk order
    uint64_t target_blob_size = 0;  ///< target (max) blob size

    OldExtentMap old_extents;       ///< must deref these blobs
    interval_set<uint64_t> extents_to_gc;      ///< extents for garbage collection

    bool full_write = false;        /// < whether full object is overwritten

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

      inline write_item(uint64_t logical_offs,
			BlobRef b,
			uint64_t blob_len,
			uint64_t o,
			ceph::buffer::list& bl,
			uint64_t o0,
			uint64_t l0,
			bool _mark_unused,
			bool _new_blob) :
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
      csum_type = other.csum_type;
      csum_order = other.csum_order;
    }
    inline void write(uint64_t loffs,
		      BlobRef b,
		      uint64_t blob_len,
		      uint64_t o,
		      ceph::buffer::list& bl,
		      uint64_t o0,
		      uint64_t len0,
		      bool _mark_unused,
		      bool _new_blob) {
      writes.emplace_back(loffs, b, blob_len,
	o, bl, o0, len0, _mark_unused, _new_blob);
    }
    /// Checks for writes to the same pextent within a blob
    bool has_conflict(
      BlobRef b,
      uint64_t loffs,
      uint64_t loffs_end,
      uint64_t min_alloc_size);
  };
  /// A Generic onode Cache Shard
  struct OnodeCacheShard : public BlueStore::CacheShard {
    std::array<std::pair<ghobject_t, ceph::mono_clock::time_point>, 64> dumped_onodes;

  public:
    OnodeCacheShard(CephContext* cct) : BlueStore::CacheShard(cct) {}
    static OnodeCacheShard* create(CephContext* cct, std::string type,
      PerfCounters* logger);

    //The following methods prefixed with '_' to be called under
    // Shard's lock
    virtual void _add(Onode* o, int level) = 0;
    virtual void _rm(Onode* o) = 0;
    virtual void _move_pinned(OnodeCacheShard* to, Onode* o) = 0;

    virtual void maybe_unpin(Onode* o) = 0;
    virtual void add_stats(uint64_t* onodes, uint64_t* pinned_onodes) = 0;
    bool empty() {
      return _get_num() == 0;
    }
  };
  
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

    bool can_defer(bluestore::extent_map_t::iterator ep,
      uint64_t prefer_deferred_size,
      uint64_t block_size,
      uint64_t offset,
      uint64_t l);
    bool apply_defer();
  };
}

#endif