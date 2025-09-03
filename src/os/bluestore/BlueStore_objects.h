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

namespace bluestore {

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    bluestore::Onode* onode;

    void set_shared_blob(BlueStore::SharedBlobRef sb);
    Blob(bluestore::Onode* onode) : onode(onode) {}
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
    struct printer : public BlueStore::printer {
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
      if (--nref == 0)
	delete this;
    }
    bool is_shared_loaded() const;
    BlueStore::BufferCacheShard* get_cache();
    uint64_t get_sbid() const;

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
    void decode(
      ceph::buffer::ptr::const_iterator& p,
      uint64_t struct_v,
      uint64_t* sbid,
      bool include_ref_map,
      BlueStore::Collection *coll);
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
    BlueStore::ExtentMap extent_map;
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
      BlueStore::ExtentMap::ExtentDecoder& dencoder,
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

    BlueStore::BlobRef new_blob();

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

    struct printer : public BlueStore::printer {
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

}

#endif
