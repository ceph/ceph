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

#include <atomic>
#include <bit>
#include <mutex>
#include <condition_variable>
#include <memory_resource>
#include <new>

#include "bluestore_types.h"
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

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    BlueStore::CollectionRef collection;

    void set_shared_blob(BlueStore::SharedBlobRef sb);
    Blob(BlueStore::CollectionRef collection) : collection(collection) {}
  private:
    BlueStore::SharedBlobRef shared_blob;      ///< shared blob state (if any)
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

    const BlueStore::SharedBlobRef& get_shared_blob() const {
      return shared_blob;
    }

    BlueStore::SharedBlobRef& get_dirty_shared_blob() {
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
    }
    void add_tail(uint32_t new_blob_size, uint32_t min_release_size);
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
      return blob;
    }

    /// get logical references
    void get_ref(BlueStore::Collection *coll, uint32_t offset, uint32_t length);
    /// put logical references, and get back any released extents
    bool put_ref(BlueStore::Collection *coll, uint32_t offset, uint32_t length,
		 PExtentVector *r);
    uint32_t put_ref_accumulate(
      BlueStore::Collection *coll,
      uint32_t offset,
      uint32_t length,
      PExtentVector *released_disk);
    /// split the blob
    void split(BlueStore::Collection *coll, uint32_t blob_offset, Blob *o);

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
    bool is_shared_loaded() const;
    BlueStore::BufferCacheShard* get_cache();
    uint64_t get_sbid() const;
    BlueStore::CollectionRef get_collection() const {
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
      BlueStore::Collection *coll);
  };

  /// a sharded extent map, mapping offsets to lextents to blobs
  struct ExtentMap {
    Onode *onode;
    BlueStore::extent_map_t extent_map;        ///< map of Extents to Blobs
    BlueStore::blob_map_t spanning_blob_map;   ///< blobs that span shards

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
			BlueStore::BlobRef from_blob, BlueStore::BlobRef to_blob);
    void make_range_shared_maybe_merge(BlueStore::TransContext* txc, BlueStore::OnodeRef& onode,
				       uint64_t srcoff, uint64_t length);

    void dup(BlueStore* b, BlueStore::TransContext*, BlueStore::CollectionRef&, BlueStore::OnodeRef&, BlueStore::OnodeRef&,
      uint64_t&, uint64_t&, uint64_t&);
    void dup_esb(BlueStore* b, BlueStore::TransContext*, BlueStore::CollectionRef&, BlueStore::OnodeRef&, BlueStore::OnodeRef&,
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
      virtual BlueStore::BlobRef decode_create_blob(
        bptr_c_it_t& p,
        __u8 struct_v,
        uint64_t* sbid,      // shared blobid, is Blob turns out to be shared blob
        bool include_ref_map, // only spanning blobs have references stored
        BlueStore::Collection* c) = 0;

      virtual void consume_blobid(Extent* le,
                                  bool spanning,
                                  uint64_t blobid) = 0;
      virtual void consume_blob(Extent* le,
                                uint64_t extent_no,
                                uint64_t sbid,
                                BlueStore::BlobRef b) = 0;
      virtual void consume_spanning_blob(uint64_t sbid, BlueStore::BlobRef b) = 0;
      virtual Extent* get_next_extent() = 0;
      virtual void add_extent(Extent*) = 0;

      void decode_extent(Extent* le,
                         __u8 struct_v,
                         bptr_c_it_t& p,
                         BlueStore::Collection* c);
    public:
      virtual ~ExtentDecoder() {
      }

      unsigned decode_some(const ceph::buffer::list& bl, BlueStore::Collection* c);
      void decode_spanning_blobs(bptr_c_it_t& p, BlueStore::Collection* c);
    };

    class ExtentDecoderFull : public ExtentDecoder {
      ExtentMap& extent_map;
      std::vector<BlueStore::BlobRef> blobs;
      // owns the Extent from get_next_extent() until add_extent() inserts it,
      // so a throw during decode_extent() can't leak it
      std::unique_ptr<Extent> pending_extent;
    protected:
      BlueStore::BlobRef decode_create_blob(
        bptr_c_it_t& p,
        __u8 struct_v,
        uint64_t* sbid,
        bool include_ref_map,
        BlueStore::Collection* c) override;

      void consume_blobid(Extent* le, bool spanning, uint64_t blobid) override;
      void consume_blob(Extent* le,
                        uint64_t extent_no,
                        uint64_t sbid,
                        BlueStore::BlobRef b) override;
      void consume_spanning_blob(uint64_t sbid, BlueStore::BlobRef b) override;
      Extent* get_next_extent() override;
      void add_extent(Extent* ) override;
    public:
      ExtentDecoderFull (ExtentMap& _extent_map) : extent_map(_extent_map) {
      }
    };

    unsigned decode_some(ceph::buffer::list& bl);

    void bound_encode_spanning_blobs(size_t& p);
    void encode_spanning_blobs(ceph::buffer::list::contiguous_appender& p);
    BlueStore::BlobRef& get_spanning_blob(int id) {
      auto p = spanning_blob_map.find(id);
      ceph_assert_decode(p != spanning_blob_map.end());
      return p->second;
    }

    void update(
      KeyValueDB::Transaction t,
      bool just_after_reshard //true to indicate that update should now respect shard boundaries
    );                        //as no further resharding will be done

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


    int16_t allocate_spanning_blob_id();
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
    BlueStore::extent_map_t::iterator find(uint64_t offset);

    /// seek to the first lextent including or after offset
    BlueStore::extent_map_t::iterator seek_lextent(uint64_t offset);
    BlueStore::extent_map_t::const_iterator seek_lextent(uint64_t offset) const;
    /// seek to the exactly the extent, or after offset
    BlueStore::extent_map_t::iterator seek_nextent(uint64_t offset);

    /// split extent
    BlueStore::extent_map_t::iterator split_at(BlueStore::extent_map_t::iterator p, uint32_t offset);
    /// if inside extent split it, if not return extent on right
    BlueStore::extent_map_t::iterator maybe_split_at(uint32_t offset);
    /// add a new Extent
    void add(uint32_t lo, uint32_t o, uint32_t l, BlueStore::BlobRef& b) {
      extent_map.insert(*new Extent(lo, o, l, b));
    }

    /// remove (and delete) an Extent
    void rm(BlueStore::extent_map_t::iterator p) {
      extent_map.erase_and_dispose(p, DeleteDisposer());
    }

    bool has_any_lextents(uint64_t offset, uint64_t length);

    /// consolidate adjacent lextents in extent_map
    int compress_extent_map(uint64_t offset, uint64_t length);

    /// punch a logical hole.  add lextents to deref to target list.
    void punch_hole(BlueStore::CollectionRef &c,
		    uint64_t offset, uint64_t length,
		    BlueStore::old_extent_map_t *old_extents);

    /// put new lextent into lextent_map overwriting existing ones if
    /// any and update references accordingly
    Extent *set_lextent(BlueStore::CollectionRef &c,
			uint64_t logical_offset,
			uint64_t offset, uint64_t length,
                        BlueStore::BlobRef b,
			BlueStore::old_extent_map_t *old_extents);

    /// split a blob (and referring extents)
    BlueStore::BlobRef split_blob(BlueStore::BlobRef lb, uint32_t blob_offset, uint32_t pos);

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
    BlueStore::Collection *c;
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

    Onode(BlueStore::Collection *c, const ghobject_t& o,
	    const mempool::bluestore_cache_meta::string& k);
    Onode(CephContext* cct);

    ~Onode();

    static void decode_raw(
      BlueStore::Onode* on,
      const bufferlist& v,
      ExtentMap::ExtentDecoder& dencoder,
      bool use_onode_segmentation);

    static Onode* create_decode(
      BlueStore::CollectionRef c,
      const ghobject_t& oid,
      const std::string& key,
      const ceph::buffer::list& v,
      bool allow_empty,
      bool use_onode_segmentation);

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

    void finish_write(BlueStore::TransContext* txc, uint32_t offset, uint32_t length);

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

  static inline void intrusive_ptr_add_ref(bluestore::Onode *o) {
    o->get();
  }
  static inline void intrusive_ptr_release(bluestore::Onode *o) {
    o->put();
  }

  /// in-memory shared blob state (incl cached buffers)
  struct SharedBlob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0}; ///< reference count
    bool loaded = false;

    BlueStore::CollectionRef collection;
    union {
      uint64_t sbid_unloaded;              ///< sbid if persistent isn't loaded
      bluestore_shared_blob_t *persistent; ///< persistent part of the shared blob if any
    };

    SharedBlob(BlueStore::Collection *_coll) : collection(_coll), sbid_unloaded(0) {
    }
    SharedBlob(uint64_t i, BlueStore::Collection *_coll);
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
    inline BlueStore::SharedBlobSet* get_parent() {
      return collection ? &(collection->shared_blob_set) : nullptr;
    }
    inline bool is_loaded() const {
      return loaded;
    }

  };
}

#endif
