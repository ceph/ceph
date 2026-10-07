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

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_OBJECTS_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_OBJECTS_H

#include "BlueStore_fwd.h"
#include "BlueStore.h"

/*
 * extent map blob encoding
 *
 * we use the low bits of the blobid field to indicate some common scenarios
 * and spanning vs local ids.  See ExtentMap::{encode,decode}_some().
 */
#define BLOBID_FLAG_CONTIGUOUS 0x1  // this extent starts at end of previous
#define BLOBID_FLAG_ZEROOFFSET 0x2  // blob_offset is 0
#define BLOBID_FLAG_SAMELENGTH 0x4  // length matches previous extent
#define BLOBID_FLAG_SPANNING   0x8  // has spanning blob id
#define BLOBID_SHIFT_BITS        4

namespace bluestore {

  struct printer {
    static constexpr uint16_t PTR = 1;   // pointer to Blob
    static constexpr uint16_t NICK = 2;  // a nickname of this Blob
    static constexpr uint16_t DISK = 4;  // disk allocations of Blob
    static constexpr uint16_t SDISK = 8; // shortened version of disk allocaitons
    static constexpr uint16_t USE = 16;  // use tracker
    static constexpr uint16_t SUSE = 32; // shortened use tracker
    static constexpr uint16_t CHK = 64;  // checksum, full dump
    static constexpr uint16_t SCHK = 128; // only base checksum info
    static constexpr uint16_t BUF = 256;  // print Blob's buffers (takes cache lock)
    static constexpr uint16_t SBUF = 512; // short print Blob's buffers (takes cache lock)
    static constexpr uint16_t ATTRS = 1024; // print attrs in onode
    static constexpr uint16_t JUSTID = 2048; // used to suppress printing length, spanning and shared blob
  };

  typedef mempool::bluestore_cache_meta::map<int, BlobRef> blob_map_t;
  /// a logical extent, pointing to (some portion of) a blob
  typedef boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true> > ExtentBase; //making an alias to avoid build warnings

  struct Extent : public ExtentBase {
    MEMPOOL_CLASS_HELPERS();

    uint32_t logical_offset = 0;      ///< logical offset
    uint32_t blob_offset = 0;         ///< blob offset
    uint32_t length = 0;              ///< length
    BlobRef  blob;                    ///< the blob with our data

    /// ctor for lookup only
    explicit Extent(uint32_t lo) : ExtentBase(), logical_offset(lo) {}
    /// ctor for delayed initialization (see decode_some())
    explicit Extent() : ExtentBase() {
    }
    /// ctor for general usage
    Extent(uint32_t lo, uint32_t o, uint32_t l, BlobRef& b)
      : ExtentBase(),
      logical_offset(lo), blob_offset(o), length(l) {
      assign_blob(b);
    }
    ~Extent();
    struct printer : public bluestore::printer {
      const Extent& ext;
      uint16_t mode;
      printer(const Extent& ext, uint16_t mode)
	:ext(ext), mode(mode) {
      }
    };
    friend std::ostream& operator<<(std::ostream& out, const printer& p);
    printer print(uint16_t mode) const {
      return printer(*this, mode);
    }

    void dump(ceph::Formatter* f) const;

    void assign_blob(const BlobRef& b);

    // comparators for intrusive_set
    friend bool operator<(const Extent& a, const Extent& b) {
      return a.logical_offset < b.logical_offset;
    }
    friend bool operator>(const Extent& a, const Extent& b) {
      return a.logical_offset > b.logical_offset;
    }
    friend bool operator==(const Extent& a, const Extent& b) {
      return a.logical_offset == b.logical_offset;
    }

    uint32_t blob_start() const {
      return logical_offset - blob_offset;
    }

    uint32_t blob_end() const;

    uint32_t logical_end() const {
      return logical_offset + length;
    }

    // return true if any piece of the blob is out of
    // the given range [o, o + l].
    bool blob_escapes_range(uint32_t o, uint32_t l) const {
      return blob_start() < o || blob_end() > o + l;
    }
  };

  std::ostream& operator<<(std::ostream& out, const Extent& e);

  typedef boost::intrusive::set<Extent> extent_map_t;

  struct OnodeSpace {
    bluestore::OnodeCacheShard* cache;

  private:
    /// forward lookups
    mempool::bluestore_cache_meta::unordered_map<ghobject_t, OnodeRef> onode_map;

    friend struct Collection;         // for split_cache()
    friend struct Onode;              // for put()
    friend struct LruOnodeCacheShard; // for _remove()
    void _remove(const ghobject_t& oid);
  public:
    OnodeSpace(bluestore::OnodeCacheShard* c) : cache(c) {}
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

  struct Collection : public ObjectStore::CollectionImpl {
    BlueStore* store;
    OpSequencerRef osr;
    BlueStore::BufferCacheShard* cache;       ///< our cache shard
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
    std::optional<int> compression_algorithm;
    std::optional<int> compression_mode;
    std::optional<int> csum_type;
    std::optional<int64_t> comp_min_blob_size;
    std::optional<int64_t> comp_max_blob_size;
    std::optional<double> compression_req_ratio;

    ContextQueue* commit_queue;
    std::unique_ptr<BlueStore::Estimator> estimator;

    std::atomic<uint64_t> runtime_frag_count{ 0 };
    std::atomic<uint64_t> runtime_read_samples{ 0 };
    std::atomic<uint64_t> static_frag_score{ 0 };
    std::atomic<uint64_t> object_read_samples{ 0 };

    OnodeCacheShard* get_onode_cache() const {
      return onode_space.cache;
    }
    OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop = false);

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
    uint64_t make_blob_unshared(SharedBlob* sb);

    inline BlobRef new_blob();

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

    void split_cache(Collection* dest);

    bool flush_commit(Context* c) override;
    void flush() override;
    void flush_all_but_last();

    Collection(BlueStore* ns, OnodeCacheShard* oc, BlueStore::BufferCacheShard* bc, coll_t c);
  };
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  /// in-memory shared blob state (incl cached buffers)
  struct SharedBlob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0}; ///< reference count
    bool loaded = false;

    CollectionRef collection;
    union {
      uint64_t sbid_unloaded;              ///< sbid if persistent isn't loaded
      bluestore_shared_blob_t *persistent; ///< persistent part of the shared blob if any
    };

    SharedBlob(bluestore::Collection *_coll) : collection(_coll), sbid_unloaded(0) {
    }
    SharedBlob(uint64_t i, bluestore::Collection *_coll);
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
    inline BlueStore::BufferCacheShard* get_cache() {
      return collection ? collection->cache : nullptr;
    }
    inline bluestore::SharedBlobSet* get_parent() {
      return collection ? &(collection->shared_blob_set) : nullptr;
    }
    inline bool is_loaded() const {
      return loaded;
    }

  };
  typedef boost::intrusive_ptr<SharedBlob> SharedBlobRef;

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    CollectionRef collection;

    void set_shared_blob(SharedBlobRef sb) {
      ceph_assert((bool)sb);
      ceph_assert(!shared_blob);
      ceph_assert(sb->collection = collection);
      shared_blob = sb;
      ceph_assert(get_cache());
    }
    Blob(CollectionRef collection) : collection(collection) {}
  private:
    SharedBlobRef shared_blob;      ///< shared blob state (if any)
    mutable bluestore_blob_t blob;  ///< decoded blob metadata
    /// refs from this shard.  ephemeral if id<0, persisted if spanning.
    bluestore_blob_use_tracker_t used_in_blob;

  public:

    friend void intrusive_ptr_add_ref(Blob *b) { b->get(); }
    friend void intrusive_ptr_release(Blob *b) { b->put(); }

    void dump(ceph::Formatter* f) const;
    friend std::ostream& operator<<(std::ostream& out, const Blob &b);
    struct printer : public bluestore::printer {
      const Blob& blob;
      uint16_t mode;
      printer(const Blob& blob, uint16_t mode)
      :blob(blob), mode(mode) {}
    };
    friend std::ostream& operator<<(std::ostream& out, const printer &p);
    printer print(uint16_t mode) const {
      return printer(*this, mode);
    }
    const bluestore_blob_use_tracker_t& get_blob_use_tracker() const {
      return used_in_blob;
    }
    bluestore_blob_use_tracker_t& dirty_blob_use_tracker() {
      return used_in_blob;
    }

    const SharedBlobRef& get_shared_blob() const {
      return shared_blob;
    }

    SharedBlobRef& get_dirty_shared_blob() {
      return shared_blob;
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

    bool can_split() {
      // splitting a BufferSpace writing list is too hard; don't try.
      return used_in_blob.can_split() &&
             get_blob().can_split();
    }

    bool can_merge_blob(const Blob* other, uint32_t& blob_end) const;
    uint32_t merge_blob(Blob* blob_to_dissolve);

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
    }

    inline void add_tail(uint32_t new_blob_size,
                         uint32_t min_release_size) {
      ceph_assert(p2phase(new_blob_size, min_release_size) == 0);
      dirty_blob().add_tail(new_blob_size);
      used_in_blob.add_tail(new_blob_size, min_release_size);
    }

    void dup(const Blob& from, bool copy_used_in_blob);
    void copy_from(const Blob& from,
		   uint32_t min_release_size, uint32_t start, uint32_t len);
    void copy_extents(const Blob& from, uint32_t start,
		      uint32_t pre_len, uint32_t main_len, uint32_t post_len);
    void copy_extents_over_empty(const Blob& from, uint32_t start, uint32_t len);

    inline const bluestore_blob_t& get_blob() const {
      return blob;
    }
    inline bluestore_blob_t& dirty_blob() {
      return blob;
    }

    /// get logical references
    void get_ref(bluestore::Collection *coll, uint32_t offset, uint32_t length);
    /// put logical references, and get back any released extents
    bool put_ref(bluestore::Collection *coll, uint32_t offset, uint32_t length,
		 PExtentVector *r);
    uint32_t put_ref_accumulate(
      bluestore::Collection* coll,
      uint32_t offset,
      uint32_t length,
      PExtentVector *released_disk);
    /// split the blob
    void split(bluestore::Collection *coll, uint32_t blob_offset, Blob *o);

    void maybe_prune_tail();

    void get() {
      ++nref;
    }
    void put() {
      if (nref.load(std::memory_order_acquire) == 1) {
        delete this;
        return;
      }
      if (--nref == 0)
	delete this;
    }
    bool is_shared_loaded() const {
      return shared_blob && shared_blob->is_loaded();
    }
    inline BlueStore::BufferCacheShard* get_cache() {
      return collection ? collection->cache : nullptr;
    }
    uint64_t get_sbid() const {
      return shared_blob ? shared_blob->get_sbid() : 0;
    }
    CollectionRef get_collection() const {
      return collection;
    }

    ~Blob();

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
    template <bool decode_csum = true>
    void decode(
      ceph::buffer::ptr::const_iterator& p,
      uint64_t struct_v,
      uint64_t* sbid,
      bool include_ref_map,
      bluestore::Collection *coll) {
      if constexpr (decode_csum)
        blob.decode<true>(p, struct_v);
      else
        blob.decode<false>(p, struct_v);
      if (blob.is_shared()) {
        denc(*sbid, p);
      }
      if (include_ref_map) {
        if (struct_v > 1) {
          used_in_blob.decode(p);
        } else {
          used_in_blob.clear();
          bluestore_extent_ref_map_t legacy_ref_map;
          legacy_ref_map.decode(p);
          if (coll) {
            for (const auto& r : legacy_ref_map.ref_map) {
              get_ref(coll, r.first, r.second.refs * r.second.length);
            }
          }
        }
      }
    }
  };

  /// a sharded extent map, mapping offsets to lextents to blobs
  struct ExtentMap {

    Onode *onode;
    extent_map_t extent_map;        ///< map of Extents to Blobs
    blob_map_t spanning_blob_map;   ///< blobs that span shards

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

    bool encode_some(
      uint32_t offset, uint32_t length, ceph::buffer::list& bl, unsigned *pn,
      bool complain_extent_overlap, //verification; in debug mode assert if extents overlap
      bool complain_shard_spanning  //verification; in debug mode assert if extent spans shards;
                                    //must be used only on encode after reshard
    );

    class ExtentDecoder {
      uint64_t pos = 0;
      uint64_t prev_len = 0;
      uint64_t extent_pos = 0;
    protected:
      // Decodes Blob from bitstream.
      // The returned Blob is then used in \ref consume_blob or \ref consume_spanning_blob
      virtual BlobRef decode_create_blob(
        bptr_c_it_t& p,
        __u8 struct_v,
        uint64_t* sbid,      // shared blobid, is Blob turns out to be shared blob
        bool include_ref_map, // only spanning blobs have references stored
        bluestore::Collection* c) = 0;

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
                         bluestore::Collection* c);
    public:
      virtual ~ExtentDecoder() {
      }

      unsigned decode_some(const ceph::buffer::list& bl, bluestore::Collection* c);
      void decode_spanning_blobs(bptr_c_it_t& p, bluestore::Collection* c);
    };

    class ExtentDecoderFull : public ExtentDecoder {
      ExtentMap& extent_map;
      std::vector<BlobRef> blobs;
      // owns the Extent from get_next_extent() until add_extent() inserts it,
      // so a throw during decode_extent() can't leak it
      std::unique_ptr<Extent> pending_extent;
    protected:
      BlobRef decode_create_blob(
        bptr_c_it_t& p,
        __u8 struct_v,
        uint64_t* sbid,
        bool include_ref_map,
        bluestore::Collection* c) override;

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
    BlobRef& get_spanning_blob(int id) {
      auto p = spanning_blob_map.find(id);
      ceph_assert_decode(p != spanning_blob_map.end());
      return p->second;
    }

    void update(
      KeyValueDB::Transaction t,
      bool just_after_reshard //true to indicate that update should now respect shard boundaries
    );                        //as no further resharding will be done
    decltype(bluestore::Blob::id) allocate_spanning_blob_id();

    struct ReshardPlan {
      std::vector<bluestore_onode_t::shard_info> new_shard_info;
      unsigned shard_index_begin;
      unsigned shard_index_end;
      uint32_t spanning_scan_begin;
      uint32_t spanning_scan_end;
    };

    ReshardPlan reshard_decision(uint32_t segment_size);

    void reshard_action(
      ReshardPlan& plan,
      KeyValueDB *db,
      KeyValueDB::Transaction t);

    void reshard(
      KeyValueDB *db,
      KeyValueDB::Transaction t,
      uint32_t segment_size);

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
    /// ensure that a range of the map is loaded
    /// return range that is encompassed by affected shards
    std::pair<uint32_t, uint32_t> fault_range_ex(
      KeyValueDB *db,
      uint32_t offset,
      uint32_t length);
    void maybe_load_shard(
      KeyValueDB *db,
      int begin_shard,
      int end_shard);

    /// ensure a range of the map is marked dirty
    void dirty_range(uint32_t offset, uint32_t length);

    /// for seek_lextent test
    extent_map_t::iterator find(uint64_t offset);

    /// seek to the first lextent including or after offset
    extent_map_t::iterator seek_lextent(uint64_t offset);
    extent_map_t::const_iterator seek_lextent(uint64_t offset) const;
    /// seek to the exactly the extent, or after offset
    extent_map_t::iterator seek_nextent(uint64_t offset);

    /// split extent
    extent_map_t::iterator split_at(extent_map_t::iterator p, uint32_t offset);
    /// if inside extent split it, if not return extent on right
    extent_map_t::iterator maybe_split_at(uint32_t offset);
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
		    bluestore::OldExtentMap *old_extents);

    /// Empties range [offset~length] of object o that is in collection c.
    /// Collects unused elements:
    /// released - sequence of allocation units that are no longer used
    /// pruned_blobs - set of blobs that are no longer used
    /// shared_changed - set of shared blobs that are modified,
    ///                  including the case of shared blob being empty
    /// statfs_delta - delta of stats
    /// returns: iterator to ExtentMap following last element removed
    extent_map_t::iterator punch_hole_2(
      Collection * c,
      OnodeRef & o,
      uint32_t offset,
      uint32_t length,
      PExtentVector& released,
      std::vector<BlobRef>& pruned_blobs,
      std::set<SharedBlobRef>& shared_changed,
      volatile_statfs& statfs_delta);

    /// put new lextent into lextent_map overwriting existing ones if
    /// any and update references accordingly
    Extent *set_lextent(CollectionRef &c,
			uint64_t logical_offset,
			uint64_t offset, uint64_t length,
                        BlobRef b,
			bluestore::OldExtentMap *old_extents);

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

  /// an in-memory object
  struct Onode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = 0;      ///< reference count
    std::atomic_int pin_nref = 0;  ///< reference count replica to track pinning
    bluestore::Collection *c;
    ghobject_t oid;

    /// key under PREFIX_OBJ where we are stored
    mempool::bluestore_cache_meta::string key;

    boost::intrusive::list_member_hook<> lru_item;

    bluestore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;              ///< true if object logically exists
    bool cached;              ///< Onode is logically in the cache
                              /// (it can be pinned and hence physically out
                              /// of it at the moment though)
    uint16_t prev_spanning_cnt = 0; /// spanning blobs count
    ExtentMap extent_map;
    BlueStore::BufferSpace bc;             ///< buffer cache

    // track txc's that have not been committed to kv store (and whose
    // effects cannot be read via the kvdb read methods)
    std::atomic<int> flushing_count = {0};
    std::atomic<int> waiting_count = {0};
    /// protect flush_txns
    ceph::mutex flush_lock = ceph::make_mutex("BlueStore::Onode::flush_lock");
    ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns
    std::shared_ptr<int64_t> cache_age_bin;  ///< cache age bin

    Onode(bluestore::Collection *c, const ghobject_t& o,
	  const mempool::bluestore_cache_meta::string& k)
      : c(c),
	oid(o),
	key(k),
	exists(false),
        cached(false),
	extent_map(this,
	  c->store->cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size),
	bc(*this) {
    }
    Onode(CephContext* cct)
      : c(nullptr),
        exists(false),
        cached(false),
        extent_map(this,
	  cct->_conf->
	    bluestore_extent_map_inline_shard_prealloc_size),
	bc(*this) {
    }

    ~Onode() {
      if (c) {
        std::lock_guard l(c->cache->lock);
        bc._clear(c->cache);
        if (prev_spanning_cnt > 0) {
          c->store->logger->dec(l_bluestore_spanning_blobs, prev_spanning_cnt);
        }
      }
    }

    static void decode_raw(
      bluestore::Onode* on,
      const bufferlist& v,
      ExtentMap::ExtentDecoder& dencoder,
      bool use_onode_segmentation);

    static Onode* create_decode(
      CollectionRef c,
      const ghobject_t& oid,
      const std::string& key,
      const ceph::buffer::list& v,
      bool allow_empty,
      bool use_onode_segmentation);

    friend void intrusive_ptr_add_ref(Onode* o) { o->get(); }
    friend void intrusive_ptr_release(Onode *o) { o->put(); }

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
    size_t calc_userkey_offset_in_omap_key() const;
    void decode_omap_key(const std::string& key, std::string *user_key);

    void finish_write(TransContext* txc, uint32_t offset, uint32_t length);

    int get_fragmentation_score();

    struct printer : public bluestore::printer {
      const Onode &onode;
      uint16_t mode;
      uint32_t from = 0;
      uint32_t end = BlueStore::OBJECT_MAX_SIZE;
      printer(const Onode &onode, uint16_t mode) : onode(onode), mode(mode) {}
      printer(const Onode &onode, uint16_t mode, uint32_t from, uint32_t end)
          : onode(onode), mode(mode), from(from), end(end) {}
    };
    friend std::ostream &operator<<(std::ostream &out, const printer &p);
    printer print(uint16_t mode) const { return printer(*this, mode); }
    printer print(uint16_t mode, uint32_t from, uint32_t end) const {
      return printer(*this, mode, from, end);
    }
  };
} // namespace bluestore

template <int LogLevelV>
inline void _dump_extent_map(CephContext *cct, const bluestore::ExtentMap &em)
{
  uint64_t pos = 0;
  for (auto& s : em.shards) {
    lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << "  shard " << *s.shard_info
		    << (s.loaded ? " (loaded)" : "")
		    << (s.dirty ? " (dirty)" : "")
		    << dendl;
  }
  for (auto& e : em.extent_map) {
    lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << "  " << e << dendl;
    ceph_assert(e.logical_offset >= pos);
    pos = e.logical_offset + e.length;
    const bluestore_blob_t& blob = e.blob->get_blob();
    if (blob.has_csum()) {
      std::vector<uint64_t> v;
      unsigned n = blob.get_csum_count();
      for (unsigned i = 0; i < n; ++i)
	v.push_back(blob.get_csum_item(i));
      lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << "      csum: "
		      << std::hex << v << std::dec << dendl;
    }
  }
}

template <int LogLevelV>
inline void _dump_onode(CephContext *cct, const bluestore::Onode& o)
{
  if (!cct->_conf->subsys.should_gather<ceph_subsys_bluestore, LogLevelV>())
    return;
  lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << " " << &o << " " << o.oid
		  << " nid " << o.onode.nid
		  << " size 0x" << std::hex << o.onode.size
		  << " (" << std::dec << o.onode.size << ")"
		  << " expected_object_size " << o.onode.expected_object_size
		  << " expected_write_size " << o.onode.expected_write_size
		  << " in " << o.onode.extent_map_shards.size() << " shards"
		  << ", " << o.extent_map.spanning_blob_map.size()
		  << " spanning blobs"
		  << dendl;
  for (auto& [zone, offset] : o.onode.zone_offset_refs) {
    lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << " zone ref 0x"
		    << std::hex << zone << " offset 0x" << offset << std::dec << dendl;
  }
  for (auto p = o.onode.attrs.begin(); p != o.onode.attrs.end(); ++p) {
    lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << "  attr " << p->first
		    << " len " << p->second.length() << dendl;
  }
  _dump_extent_map<LogLevelV>(cct, o.extent_map);

  for (auto& b : o.bc.buffer_map) {
    lgeneric_subdout(cct, bluestore, LogLevelV) << __func__ << "       0x"
                    << std::hex << b.offset << "~" << b.length << std::dec
                    << " " << b << dendl;
  }
}

template<typename S>
void generate_extent_shard_key_and_apply(
  const S& onode_key,
  uint32_t offset,
  std::string *key,
  std::function<void(const std::string& final_key)> apply);

#endif
