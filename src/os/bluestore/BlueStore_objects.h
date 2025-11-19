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
#include <map>
#include <set>

#include "bluestore_types.h"
#include "BlueStore.h"

class FixedPoolMemoryResource : public std::pmr::memory_resource {
  struct Slab {
    Slab* next;
    size_t size;
    size_t offset;
    void* data() { return reinterpret_cast<char*>(this + 1); }
  };

  struct FreeBlock {
    uintptr_t ptr;
    size_t size;
    bool operator<(const FreeBlock& o) const {
      return size < o.size || (size == o.size && ptr < o.ptr);
    }
  };

  Slab* head = nullptr;
  Slab* current_slab = nullptr;
  size_t default_slab_size;
  std::set<FreeBlock> size_freelist;
  std::map<uintptr_t, size_t> ptr_map;

public:
  FixedPoolMemoryResource(void* start, size_t size) : default_slab_size(size) {
    head = init_slab(start, size);
    current_slab = head;
  }

  ~FixedPoolMemoryResource() {
    Slab* slab = head->next;
    while (slab) {
      Slab* next = slab->next;
      free(slab);
      slab = next;
    }
  }

protected:
  void* do_allocate(size_t bytes, size_t alignment) override {
    assert((alignment & (alignment - 1)) == 0);
    if (bytes == 0) return static_cast<char*>(current_slab->data()) + current_slab->offset;

    FreeBlock key{0, bytes};
    auto it = size_freelist.lower_bound(key);
    while (it != size_freelist.end()) {
      void* p = reinterpret_cast<void*>(it->ptr);
      size_t block_size = it->size;
      size_t padding = (alignment - (reinterpret_cast<uintptr_t>(p) & (alignment - 1))) & (alignment - 1);
      size_t used = bytes + padding;
      if (block_size >= used) {
        ptr_map.erase(it->ptr);
        auto to_erase = it++;
        size_freelist.erase(to_erase);
        void* aligned_ptr = static_cast<char*>(p) + padding;
        if (block_size > used) {
          void* rem_ptr = static_cast<char*>(p) + used;
          size_t rem_size = block_size - used;
          size_freelist.insert(FreeBlock{reinterpret_cast<uintptr_t>(rem_ptr), rem_size});
          ptr_map[reinterpret_cast<uintptr_t>(rem_ptr)] = rem_size;
        }
        return aligned_ptr;
      } else {
        ++it;
      }
    }

    size_t aligned_offset = (current_slab->offset + alignment - 1) & ~(alignment - 1);

    if (aligned_offset + bytes <= current_slab->size) {
      void* result = static_cast<char*>(current_slab->data()) + aligned_offset;
      current_slab->offset = aligned_offset + bytes;
      return result;
    }

    size_t new_slab_size = std::max(bytes, default_slab_size);
    size_t slab_bytes = sizeof(Slab) + new_slab_size;
    constexpr size_t SLAB_ALIGN = alignof(Slab);
    void* mem = std::aligned_alloc(SLAB_ALIGN, slab_bytes);
    if (!mem) throw std::bad_alloc();
    Slab* new_slab = new (mem) Slab{nullptr, new_slab_size, 0};
    current_slab->next = new_slab;
    current_slab = new_slab;

    aligned_offset = (current_slab->offset + alignment - 1) & ~(alignment - 1);
    void* result = static_cast<char*>(current_slab->data()) + aligned_offset;
    current_slab->offset = aligned_offset + bytes;
    return result;
  }

  void do_deallocate(void* p, size_t bytes, size_t) override {
    if (bytes == 0 || p == nullptr) return;
    uintptr_t addr = reinterpret_cast<uintptr_t>(p);
    if (ptr_map.find(addr) != ptr_map.end()) throw std::runtime_error("Double deallocation detected");
    coalesce(addr, bytes);
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  void reset() {
    for (Slab* s = head; s; s = s->next) s->offset = 0;
    size_freelist.clear();
    ptr_map.clear();
  }

private:
  static Slab* init_slab(void* start, size_t size) {
    constexpr size_t SLAB_ALIGN = alignof(Slab);
    uintptr_t p = reinterpret_cast<uintptr_t>(start);
    uintptr_t aligned = (p + (SLAB_ALIGN - 1)) & ~(SLAB_ALIGN - 1);

    size_t adjust = aligned - p;
    if (size < adjust + sizeof(Slab))
      throw std::bad_alloc();

    size -= adjust;
    start = reinterpret_cast<void*>(aligned);
    return new (start) Slab{nullptr, size - sizeof(Slab), 0};
  }

  void coalesce(uintptr_t p, size_t sz) {
    uintptr_t addr = p;
    size_t size = sz;
    auto left_it = ptr_map.lower_bound(addr);
    if (left_it != ptr_map.begin()) {
      --left_it;
      uintptr_t left_addr = left_it->first;
      size_t left_size = left_it->second;
      if (left_addr + left_size == addr) {
        size += left_size;
        size_freelist.erase(FreeBlock{left_addr, left_size});
        ptr_map.erase(left_it);
        addr = left_addr;
      }
    }

    auto right_it = ptr_map.upper_bound(addr);
    if (right_it != ptr_map.end()) {
      uintptr_t right_addr = right_it->first;
      size_t right_size = right_it->second;
      if (addr + size == right_addr) {
        size += right_size;
        size_freelist.erase(FreeBlock{right_addr, right_size});
        ptr_map.erase(right_it);
      }
    }
    ptr_map[addr] = size;
    size_freelist.insert(FreeBlock{addr, size});
  }
};


namespace bluestore {

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    bluestore::Onode* onode;

    void set_shared_blob(BlueStore::SharedBlobRef sb);
    Blob(bluestore::Onode* onode)
     : onode(onode),
     used_in_blob(onode) {}
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

    // track txc's that have not been committed to kv store (and whose
    // effects cannot be read via the kvdb read methods)
    std::atomic<int> flushing_count = {0};
    std::atomic<int> waiting_count = {0};
    /// protect flush_txns
    ceph::mutex flush_lock = ceph::make_mutex("BlueStore::Onode::flush_lock");
    ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns
    std::shared_ptr<int64_t> cache_age_bin;  ///< cache age bin

    static constexpr size_t pool_size = sizeof(uint32_t) * 128;
    alignas(uint32_t) std::byte pool[pool_size];
    FixedPoolMemoryResource mem_resource;
    std::pmr::polymorphic_allocator<uint32_t> LocalBytesPerAuAllocator;
    BlueStore::ExtentMap extent_map;
    BlueStore::BufferSpace bc;             ///< buffer cache

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
