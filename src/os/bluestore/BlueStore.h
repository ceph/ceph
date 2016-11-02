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
#include <mutex>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>

#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "include/mempool.h"
#include "common/Finisher.h"
#include "compressor/Compressor.h"
#include "os/ObjectStore.h"

#include "bluestore_types.h"
#include "BlockDevice.h"
class Allocator;
class FreelistManager;
class BlueFS;

//#define DEBUG_CACHE

enum {
  l_bluestore_first = 732430,
  l_bluestore_state_prepare_lat,
  l_bluestore_state_aio_wait_lat,
  l_bluestore_state_io_done_lat,
  l_bluestore_state_kv_queued_lat,
  l_bluestore_state_kv_committing_lat,
  l_bluestore_state_kv_done_lat,
  l_bluestore_state_wal_queued_lat,
  l_bluestore_state_wal_applying_lat,
  l_bluestore_state_wal_aio_wait_lat,
  l_bluestore_state_wal_cleanup_lat,
  l_bluestore_state_finishing_lat,
  l_bluestore_state_done_lat,
  l_bluestore_compress_lat,
  l_bluestore_decompress_lat,
  l_bluestore_csum_lat,
  l_bluestore_compress_success_count,
  l_bluestore_compress_rejected_count,
  l_bluestore_write_pad_bytes,
  l_bluestore_wal_write_ops,
  l_bluestore_wal_write_bytes,
  l_bluestore_write_penalty_read_ops,
  l_bluestore_allocated,
  l_bluestore_stored,
  l_bluestore_compressed,
  l_bluestore_compressed_allocated,
  l_bluestore_compressed_original,
  l_bluestore_onodes,
  l_bluestore_onode_hits,
  l_bluestore_onode_misses,
  l_bluestore_extents,
  l_bluestore_blobs,
  l_bluestore_buffers,
  l_bluestore_buffer_bytes,
  l_bluestore_buffer_hit_bytes,
  l_bluestore_buffer_miss_bytes,
  l_bluestore_write_big,
  l_bluestore_write_big_bytes,
  l_bluestore_write_big_blobs,
  l_bluestore_write_small,
  l_bluestore_write_small_bytes,
  l_bluestore_write_small_unused,
  l_bluestore_write_small_wal,
  l_bluestore_write_small_pre_read,
  l_bluestore_write_small_new,
  l_bluestore_txc,
  l_bluestore_onode_reshard,
  l_bluestore_gc,
  l_bluestore_gc_bytes,
  l_bluestore_blob_split,
  l_bluestore_last
};

class BlueStore : public ObjectStore,
		  public md_config_obs_t {
  // -----------------------------------------------------
  // types
public:

  // config observer
  virtual const char** get_tracked_conf_keys() const override;
  virtual void handle_conf_change(const struct md_config_t *conf,
                                  const std::set<std::string> &changed) override;

  void _set_csum();
  void _set_compression();

  class TransContext;

  typedef map<uint64_t, bufferlist> ready_regions_t;

  struct BufferSpace;

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
    bufferlist data;

    boost::intrusive::list_member_hook<> lru_item;
    boost::intrusive::list_member_hook<> state_item;

    Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, uint32_t l,
	   unsigned f = 0)
      : space(space), state(s), flags(f), seq(q), offset(o), length(l) {}
    Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, bufferlist& b,
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
      assert(newlen < length);
      if (data.length()) {
	bufferlist t;
	t.substr_of(data, 0, newlen);
	data.claim(t);
      }
      length = newlen;
    }

    void dump(Formatter *f) const {
      f->dump_string("state", get_state_name(state));
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("offset", offset);
      f->dump_unsigned("length", length);
      f->dump_unsigned("data_length", data.length());
    }
  };

  struct Cache;

  /// map logical extent range (object) onto buffers
  struct BufferSpace {
    typedef boost::intrusive::list<
      Buffer,
      boost::intrusive::member_hook<
        Buffer,
	boost::intrusive::list_member_hook<>,
	&Buffer::state_item> > state_list_t;

    mempool::bluestore_meta_other::map<uint64_t, std::unique_ptr<Buffer>>
      buffer_map;
    Cache *cache;

    // we use a bare intrusive list here instead of std::map because
    // it uses less memory and we expect this to be very small (very
    // few IOs in flight to the same Blob at the same time).
    state_list_t writing;   ///< writing buffers, sorted by seq, ascending

    BufferSpace(Cache *c) : cache(c) {
      if (cache) {
	cache->add_blob();
      }
    }
    ~BufferSpace() {
      assert(buffer_map.empty());
      assert(writing.empty());
      if (cache) {
	cache->rm_blob();
      }
    }

    void _add_buffer(Buffer *b, int level, Buffer *near) {
      cache->_audit("_add_buffer start");
      buffer_map[b->offset].reset(b);
      if (b->is_writing()) {
        writing.push_back(*b);
      } else {
	cache->_add_buffer(b, level, near);
      }
      cache->_audit("_add_buffer end");
    }
    void _rm_buffer(Buffer *b) {
      _rm_buffer(buffer_map.find(b->offset));
    }
    void _rm_buffer(map<uint64_t,std::unique_ptr<Buffer>>::iterator p) {
      assert(p != buffer_map.end());
      cache->_audit("_rm_buffer start");
      if (p->second->is_writing()) {
        writing.erase(writing.iterator_to(*p->second));
      } else {
	cache->_rm_buffer(p->second.get());
      }
      buffer_map.erase(p);
      cache->_audit("_rm_buffer end");
    }

    map<uint64_t,std::unique_ptr<Buffer>>::iterator _data_lower_bound(
      uint64_t offset) {
      auto i = buffer_map.lower_bound(offset);
      if (i != buffer_map.begin()) {
	--i;
	if (i->first + i->second->length <= offset)
	  ++i;
      }
      return i;
    }

    bool empty() const {
      return buffer_map.empty();
    }

    // must be called under protection of the Cache lock
    void _clear();

    // return value is the highest cache_private of a trimmed buffer, or 0.
    int discard(uint64_t offset, uint64_t length) {
      std::lock_guard<std::recursive_mutex> l(cache->lock);
      return _discard(offset, length);
    }
    int _discard(uint64_t offset, uint64_t length);

    void write(uint64_t seq, uint64_t offset, bufferlist& bl, unsigned flags) {
      std::lock_guard<std::recursive_mutex> l(cache->lock);
      Buffer *b = new Buffer(this, Buffer::STATE_WRITING, seq, offset, bl,
			     flags);
      b->cache_private = _discard(offset, bl.length());
      _add_buffer(b, (flags & Buffer::FLAG_NOCACHE) ? 0 : 1, nullptr);
    }
    void finish_write(uint64_t seq);
    void did_read(uint64_t offset, bufferlist& bl) {
      std::lock_guard<std::recursive_mutex> l(cache->lock);
      Buffer *b = new Buffer(this, Buffer::STATE_CLEAN, 0, offset, bl);
      b->cache_private = _discard(offset, bl.length());
      _add_buffer(b, 1, nullptr);
    }

    void read(uint64_t offset, uint64_t length,
	      BlueStore::ready_regions_t& res,
	      interval_set<uint64_t>& res_intervals);

    void truncate(uint64_t offset) {
      discard(offset, (uint64_t)-1 - offset);
    }

    void split(size_t pos, BufferSpace &r);

    void dump(Formatter *f) const {
      std::lock_guard<std::recursive_mutex> l(cache->lock);
      f->open_array_section("buffers");
      for (auto& i : buffer_map) {
	f->open_object_section("buffer");
	assert(i.first == i.second->offset);
	i.second->dump(f);
	f->close_section();
      }
      f->close_section();
    }
  };

  struct SharedBlobSet;

  /// in-memory shared blob state (incl cached buffers)
  struct SharedBlob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0}; ///< reference count

    // these are defined/set if the shared_blob is 'loaded'
    bool loaded = false;        ///< whether shared_blob is loaded
    bluestore_shared_blob_t shared_blob; ///< the actual shared state

    // these are defined/set if the blob is marked 'shared'
    uint64_t sbid = 0;          ///< shared blob id
    string key;                 ///< key in kv store
    SharedBlobSet *parent_set = 0;  ///< containing SharedBlobSet

    BufferSpace bc;             ///< buffer cache

    SharedBlob(uint64_t i, const string& k, Cache *c);
    ~SharedBlob();

    friend void intrusive_ptr_add_ref(SharedBlob *b) { b->get(); }
    friend void intrusive_ptr_release(SharedBlob *b) { b->put(); }

    friend ostream& operator<<(ostream& out, const SharedBlob& sb);

    void get() {
      ++nref;
    }
    void put();

    friend bool operator==(const SharedBlob &l, const SharedBlob &r) {
      return l.sbid == r.sbid;
    }
    friend std::size_t hash_value(const SharedBlob &e) {
      rjhash<uint32_t> h;
      return h(e.sbid);
    }
  };
  typedef boost::intrusive_ptr<SharedBlob> SharedBlobRef;

  /// a lookup table of SharedBlobs
  struct SharedBlobSet {
    std::mutex lock;   ///< protect lookup, insertion, removal

    // we use a bare pointer because we don't want to affect the ref
    // count
    mempool::bluestore_meta_other::unordered_map<uint64_t,SharedBlob*> sb_map;

    SharedBlobRef lookup(uint64_t sbid) {
      std::lock_guard<std::mutex> l(lock);
      auto p = sb_map.find(sbid);
      if (p == sb_map.end()) {
        return nullptr;
      }
      return p->second;
    }

    void add(SharedBlob *sb) {
      std::lock_guard<std::mutex> l(lock);
      sb_map[sb->sbid] = sb;
      sb->parent_set = this;
    }

    bool remove(SharedBlob *sb) {
      std::lock_guard<std::mutex> l(lock);
      if (sb->nref == 0) {
	assert(sb->parent_set == this);
	sb_map.erase(sb->sbid);
	return true;
      }
      return false;
    }

    bool empty() {
      std::lock_guard<std::mutex> l(lock);
      return sb_map.empty();
    }
  };

  /// in-memory blob metadata and associated cached buffers (if any)
  struct Blob {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref = {0};     ///< reference count
    int16_t id = -1;                ///< id, for spanning blobs only, >= 0
    int16_t last_encoded_id = -1;   ///< (ephemeral) used during encoding only
    SharedBlobRef shared_blob;      ///< shared blob state (if any)

    /// refs from this shard.  ephemeral if id<0, persisted if spanning.
    bluestore_extent_ref_map_t ref_map;

  private:
    mutable bluestore_blob_t blob;  ///< decoded blob metadata
    mutable bool dirty = true;      ///< true if blob is newer than blob_bl
    mutable bufferlist blob_bl;     ///< cached encoded blob

  public:
    Blob() {}
    ~Blob() {
    }

    friend void intrusive_ptr_add_ref(Blob *b) { b->get(); }
    friend void intrusive_ptr_release(Blob *b) { b->put(); }

    friend ostream& operator<<(ostream& out, const Blob &b);

    bool is_spanning() const {
      return id >= 0;
    }

    bool can_split() const {
      std::lock_guard<std::recursive_mutex> l(shared_blob->bc.cache->lock);
      // splitting a BufferSpace writing list is too hard; don't try.
      return shared_blob->bc.writing.empty() && get_blob().can_split();
    }

    void dup(Blob& o) {
      o.shared_blob = shared_blob;
      o.blob = blob;
      o.dirty = dirty;
      o.blob_bl = blob_bl;
    }

    const bluestore_blob_t& get_blob() const {
      return blob;
    }
    bluestore_blob_t& dirty_blob() {
      if (!dirty) {
	dirty = true;
	blob_bl.clear();
      }
      return blob;
    }
    bool is_dirty() const {
      return dirty;
    }

    bool is_unreferenced(uint64_t offset, uint64_t length) const {
      return !ref_map.intersects(offset, length);
    }

    /// discard buffers for unallocated regions
    void discard_unallocated();

    /// get logical references
    void get_ref(uint64_t offset, uint64_t length);
    /// put logical references, and get back any released extents
    bool put_ref(uint64_t offset, uint64_t length,  uint64_t min_alloc_size,
		 vector<bluestore_pextent_t> *r);

    /// split the blob
    void split(size_t blob_offset, Blob *o);

    void get() {
      ++nref;
    }
    void put() {
      if (--nref == 0)
	delete this;
    }

//#define CACHE_BLOB_BL  // not sure if this is a win yet or not... :/

#ifdef CACHE_BLOB_BL
    void _encode() const {
      if (dirty) {
	blob_bl.clear();
	::encode(blob, blob_bl);
	dirty = false;
      } else {
	assert(blob_bl.length());
      }
    }
    void bound_encode(size_t& p) const {
      _encode();
      p += blob_bl.length();
    }
    void encode(bufferlist::contiguous_appender& p) const {
      _encode();
      p.append(blob_bl);
    }
    void decode(bufferptr::iterator& p) {
      const char *start = p.get_pos();
      denc(blob, p);
      const char *end = p.get_pos();
      blob_bl.clear();
      blob_bl.append(start, end - start);
      dirty = false;
    }
#else
    void bound_encode(size_t& p) const {
      denc(blob, p);
    }
    void encode(bufferlist::contiguous_appender& p) const {
      denc(blob, p);
    }
    void decode(bufferptr::iterator& p) {
      denc(blob, p);
    }
#endif
  };
  typedef boost::intrusive_ptr<Blob> BlobRef;
  typedef mempool::bluestore_meta_other::map<int,BlobRef> blob_map_t;

  /// a logical extent, pointing to (some portion of) a blob
  struct Extent : public boost::intrusive::set_base_hook<boost::intrusive::optimize_size<true>> {
    MEMPOOL_CLASS_HELPERS();

    uint32_t logical_offset = 0;      ///< logical offset
    uint32_t blob_offset = 0;         ///< blob offset
    uint32_t length = 0;              ///< length
    uint8_t  blob_depth = 0;          ///< blob overlapping count
    BlobRef blob;                     ///< the blob with our data

    /// ctor for lookup only
    explicit Extent(uint32_t lo) : logical_offset(lo) { }
    /// ctor for delayed initialization (see decode_some())
    explicit Extent() {
    }
    /// ctor for general usage
    Extent(uint32_t lo, uint32_t o, uint32_t l, uint8_t bd, BlobRef& b)
      : logical_offset(lo), blob_offset(o), length(l), blob_depth(bd) {
      assign_blob(b);
    }
    ~Extent() {
      if (blob) {
	blob->shared_blob->bc.cache->rm_extent();
      }
    }

    void assign_blob(const BlobRef& b) {
      assert(!blob);
      blob = b;
      blob->shared_blob->bc.cache->add_extent();
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

    uint32_t blob_end() {
      return logical_offset + blob->get_blob().get_logical_length() -
	blob_offset;
    }

    uint32_t end() const {
      return logical_offset + length;
    }

    bool blob_escapes_range(uint32_t o, uint32_t l) {
      uint32_t bstart = logical_offset - blob_offset;
      return (bstart < o ||
	      bstart + blob->get_blob().get_logical_length() > o + l);
    }
  };
  typedef boost::intrusive::set<Extent> extent_map_t;

  friend ostream& operator<<(ostream& out, const Extent& e);

  struct Collection;
  struct Onode;

  /// a sharded extent map, mapping offsets to lextents to blobs
  struct ExtentMap {
    Onode *onode;
    extent_map_t extent_map;        ///< map of Extents to Blobs
    blob_map_t spanning_blob_map;   ///< blobs that span shards

    struct Shard {
      string key;            ///< kv key
      uint32_t offset;       ///< starting logical offset
      bluestore_onode_t::shard_info *shard_info;
      bool loaded = false;   ///< true if shard is loaded
      bool dirty = false;    ///< true if shard is dirty and needs reencoding
    };
    mempool::bluestore_meta_other::vector<Shard> shards;    ///< shards

    bufferlist inline_bl;    ///< cached encoded map, if unsharded; empty=>dirty

    bool needs_reshard = false;   ///< true if we must reshard

    ExtentMap(Onode *o);
    ~ExtentMap() {
      extent_map.clear_and_dispose([&](Extent *e) { delete e; });
    }

    void clear() {
      extent_map.clear();
      extent_map.clear_and_dispose([&](Extent *e) { delete e; });
      shards.clear();
      inline_bl.clear();
      needs_reshard = false;
    }

    bool encode_some(uint32_t offset, uint32_t length, bufferlist& bl,
		     unsigned *pn);
    void decode_some(bufferlist& bl);

    void bound_encode_spanning_blobs(size_t& p);
    void encode_spanning_blobs(bufferlist::contiguous_appender& p);
    void decode_spanning_blobs(Collection *c, bufferptr::iterator& p);

    BlobRef get_spanning_blob(int id) {
      auto p = spanning_blob_map.find(id);
      assert(p != spanning_blob_map.end());
      return p->second;
    }

    bool update(Onode *on, KeyValueDB::Transaction t, bool force);
    void reshard(Onode *on, uint64_t min_alloc_size);

    /// initialize Shards from the onode
    void init_shards(Onode *on, bool loaded, bool dirty);

    /// return index of shard containing offset
    /// or -1 if not found
    int seek_shard(uint32_t offset) {
      size_t end = shards.size();
      size_t mid, left = 0;
      size_t right = end; // one passed the right end

      while (left < right) {
        mid = left + (right - left) / 2;
        if (offset >= shards[mid].offset) {
          size_t next = mid + 1;
          if (next >= end || offset < shards[next].offset)
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
      assert(s >= 0);
      if (s == (int)shards.size() - 1) {
	return false; // last shard
      }
      if (offset + length <= shards[s+1].offset) {
	return false;
      }
      return true;
    }

    /// ensure that a range of the map is loaded
    void fault_range(KeyValueDB *db,
		     uint32_t offset, uint32_t length);

    /// ensure a range of the map is marked dirty
    void dirty_range(KeyValueDB::Transaction t,
		     uint32_t offset, uint32_t length);

    extent_map_t::iterator find(uint64_t offset);

    /// find a lextent that includes offset
    extent_map_t::iterator find_lextent(uint64_t offset);

    /// seek to the first lextent including or after offset
    extent_map_t::iterator seek_lextent(uint64_t offset);

    /// add a new Extent
    void add(uint32_t lo, uint32_t o, uint32_t l, uint8_t bd, BlobRef& b) {
      extent_map.insert(*new Extent(lo, o, l, bd, b));
    }

    /// remove (and delete) an Extent
    void rm(extent_map_t::iterator p) {
      Extent *e = &*p;
      extent_map.erase(p);
      delete e;
    }

    bool has_any_lextents(uint64_t offset, uint64_t length);

    /// consolidate adjacent lextents in extent_map
    int compress_extent_map(uint64_t offset, uint64_t length);

    /// punch a logical hole.  add lextents to deref to target list.
    void punch_hole(uint64_t offset, uint64_t length,
		    extent_map_t *old_extents);

    /// put new lextent into lextent_map overwriting existing ones if
    /// any and update references accordingly
    Extent *set_lextent(uint64_t logical_offset,
			uint64_t offset, uint64_t length, uint8_t blob_depth,
                        BlobRef b, extent_map_t *old_extents);

    /// split a blob (and referring extents)
    BlobRef split_blob(BlobRef lb, uint32_t blob_offset, uint32_t pos);

    bool do_write_check_depth(
      uint64_t onode_size,
      uint64_t start_offset,
      uint64_t end_offset,
      uint8_t  *blob_depth,
      uint64_t *gc_start_offset,
      uint64_t *gc_end_offset);
  };

  struct OnodeSpace;

  /// an in-memory object
  struct Onode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref;  ///< reference count
    Collection *c;

    ghobject_t oid;
    string key;     ///< key under PREFIX_OBJ where we are stored

    OnodeSpace *space;    ///< containing OnodeSpace
    boost::intrusive::list_member_hook<> lru_item;

    bluestore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;              ///< true if object logically exists

    ExtentMap extent_map;

    std::mutex flush_lock;  ///< protect flush_txns
    std::condition_variable flush_cond;   ///< wait here for unapplied txns
    set<TransContext*> flush_txns;   ///< committing or wal txns

    Onode(OnodeSpace *s, Collection *c, const ghobject_t& o, const string& k)
      : nref(0),
	c(c),
	oid(o),
	key(k),
	space(s),
	exists(false),
	extent_map(this) {
    }

    void flush();
    void get() {
      ++nref;
    }
    void put() {
      if (--nref == 0)
	delete this;
    }
  };
  typedef boost::intrusive_ptr<Onode> OnodeRef;


  /// a cache (shard) of onodes and buffers
  struct Cache {
    PerfCounters *logger;
    std::recursive_mutex lock;          ///< protect lru and other structures

    std::atomic<uint64_t> num_extents = {0};
    std::atomic<uint64_t> num_blobs = {0};

    size_t last_trim_seq = 0;

    static Cache *create(string type, PerfCounters *logger);

    virtual ~Cache() {}

    virtual void _add_onode(OnodeRef& o, int level) = 0;
    virtual void _rm_onode(OnodeRef& o) = 0;
    virtual void _touch_onode(OnodeRef& o) = 0;

    virtual void _add_buffer(Buffer *b, int level, Buffer *near) = 0;
    virtual void _rm_buffer(Buffer *b) = 0;
    virtual void _adjust_buffer_size(Buffer *b, int64_t delta) = 0;
    virtual void _touch_buffer(Buffer *b) = 0;

    virtual uint64_t _get_num_onodes() = 0;
    virtual uint64_t _get_buffer_bytes() = 0;

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

    void trim(uint64_t target_bytes, float target_meta_ratio,
	      float bytes_per_onode);

    virtual void _trim(uint64_t onode_max, uint64_t buffer_max) = 0;

    virtual void add_stats(uint64_t *onodes, uint64_t *extents,
			   uint64_t *blobs,
			   uint64_t *buffers,
			   uint64_t *bytes) = 0;

#ifdef DEBUG_CACHE
    virtual void _audit(const char *s) = 0;
#else
    void _audit(const char *s) { /* no-op */ }
#endif
  };

  /// simple LRU cache for onodes and buffers
  struct LRUCache : public Cache {
  private:
    typedef boost::intrusive::list<
      Onode,
      boost::intrusive::member_hook<
        Onode,
	boost::intrusive::list_member_hook<>,
	&Onode::lru_item> > onode_lru_list_t;
    typedef boost::intrusive::list<
      Buffer,
      boost::intrusive::member_hook<
	Buffer,
	boost::intrusive::list_member_hook<>,
	&Buffer::lru_item> > buffer_lru_list_t;

    onode_lru_list_t onode_lru;

    buffer_lru_list_t buffer_lru;
    uint64_t buffer_size = 0;

  public:
    uint64_t _get_num_onodes() override {
      return onode_lru.size();
    }
    void _add_onode(OnodeRef& o, int level) override {
      if (level > 0)
	onode_lru.push_front(*o);
      else
	onode_lru.push_back(*o);
    }
    void _rm_onode(OnodeRef& o) override {
      auto q = onode_lru.iterator_to(*o);
      onode_lru.erase(q);
    }
    void _touch_onode(OnodeRef& o) override;

    uint64_t _get_buffer_bytes() override {
      return buffer_size;
    }
    void _add_buffer(Buffer *b, int level, Buffer *near) override {
      if (near) {
	auto q = buffer_lru.iterator_to(*near);
	buffer_lru.insert(q, *b);
      } else if (level > 0) {
	buffer_lru.push_front(*b);
      } else {
	buffer_lru.push_back(*b);
      }
      buffer_size += b->length;
    }
    void _rm_buffer(Buffer *b) override {
      assert(buffer_size >= b->length);
      buffer_size -= b->length;
      auto q = buffer_lru.iterator_to(*b);
      buffer_lru.erase(q);
    }
    void _adjust_buffer_size(Buffer *b, int64_t delta) override {
      assert((int64_t)buffer_size + delta >= 0);
      buffer_size += delta;
    }
    void _touch_buffer(Buffer *b) override {
      auto p = buffer_lru.iterator_to(*b);
      buffer_lru.erase(p);
      buffer_lru.push_front(*b);
      _audit("_touch_buffer end");
    }

    void _trim(uint64_t onode_max, uint64_t buffer_max) override;

    void add_stats(uint64_t *onodes, uint64_t *extents,
		   uint64_t *blobs,
		   uint64_t *buffers,
		   uint64_t *bytes) override {
      std::lock_guard<std::recursive_mutex> l(lock);
      *onodes += onode_lru.size();
      *extents += num_extents;
      *blobs += num_blobs;
      *buffers += buffer_lru.size();
      *bytes += buffer_size;
    }

#ifdef DEBUG_CACHE
    void _audit(const char *s) override;
#endif
  };

  // 2Q cache for buffers, LRU for onodes
  struct TwoQCache : public Cache {
  private:
    // stick with LRU for onodes for now (fixme?)
    typedef boost::intrusive::list<
      Onode,
      boost::intrusive::member_hook<
        Onode,
	boost::intrusive::list_member_hook<>,
	&Onode::lru_item> > onode_lru_list_t;
    typedef boost::intrusive::list<
      Buffer,
      boost::intrusive::member_hook<
	Buffer,
	boost::intrusive::list_member_hook<>,
	&Buffer::lru_item> > buffer_list_t;

    onode_lru_list_t onode_lru;

    buffer_list_t buffer_hot;      //< "Am" hot buffers
    buffer_list_t buffer_warm_in;  //< "A1in" newly warm buffers
    buffer_list_t buffer_warm_out; //< "A1out" empty buffers we've evicted
    uint64_t buffer_bytes = 0;     //< bytes

    enum {
      BUFFER_NEW = 0,
      BUFFER_WARM_IN,   ///< in buffer_warm_in
      BUFFER_WARM_OUT,  ///< in buffer_warm_out
      BUFFER_HOT,       ///< in buffer_hot
      BUFFER_TYPE_MAX
    };

    uint64_t buffer_list_bytes[BUFFER_TYPE_MAX] = {0}; ///< bytes per type

  public:
    uint64_t _get_num_onodes() override {
      return onode_lru.size();
    }
    void _add_onode(OnodeRef& o, int level) override {
      if (level > 0)
	onode_lru.push_front(*o);
      else
	onode_lru.push_back(*o);
    }
    void _rm_onode(OnodeRef& o) override {
      auto q = onode_lru.iterator_to(*o);
      onode_lru.erase(q);
    }
    void _touch_onode(OnodeRef& o) override;

    uint64_t _get_buffer_bytes() override {
      return buffer_bytes;
    }
    void _add_buffer(Buffer *b, int level, Buffer *near) override;
    void _rm_buffer(Buffer *b) override;
    void _adjust_buffer_size(Buffer *b, int64_t delta) override;
    void _touch_buffer(Buffer *b) override {
      switch (b->cache_private) {
      case BUFFER_WARM_IN:
	// do nothing (somewhat counter-intuitively!)
	break;
      case BUFFER_WARM_OUT:
	// move from warm_out to hot LRU
	assert(0 == "this happens via discard hint");
	break;
      case BUFFER_HOT:
	// move to front of hot LRU
	buffer_hot.erase(buffer_hot.iterator_to(*b));
	buffer_hot.push_front(*b);
	break;
      }
      _audit("_touch_buffer end");
    }

    void _trim(uint64_t onode_max, uint64_t buffer_max) override;

    void add_stats(uint64_t *onodes, uint64_t *extents,
		   uint64_t *blobs,
		   uint64_t *buffers,
		   uint64_t *bytes) override {
      std::lock_guard<std::recursive_mutex> l(lock);
      *onodes += onode_lru.size();
      *extents += num_extents;
      *blobs += num_blobs;
      *buffers += buffer_hot.size() + buffer_warm_in.size();
      *bytes += buffer_bytes;
    }

#ifdef DEBUG_CACHE
    void _audit(const char *s) override;
#endif
  };

  struct OnodeSpace {
    Cache *cache;

    /// forward lookups
    mempool::bluestore_meta_other::unordered_map<ghobject_t,OnodeRef> onode_map;

    OnodeSpace(Cache *c) : cache(c) {}
    ~OnodeSpace() {
      clear();
    }

    OnodeRef add(const ghobject_t& oid, OnodeRef o);
    OnodeRef lookup(const ghobject_t& o);
    void rename(OnodeRef& o, const ghobject_t& old_oid,
		const ghobject_t& new_oid,
		const string& new_okey);
    void clear();

    /// return true if f true for any item
    bool map_any(std::function<bool(OnodeRef)> f);
  };

  struct Cache;

  struct Collection : public CollectionImpl {
    BlueStore *store;
    Cache *cache;       ///< our cache shard
    coll_t cid;
    bluestore_cnode_t cnode;
    RWLock lock;

    bool exists;

    SharedBlobSet shared_blob_set;      ///< open SharedBlobs

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    OnodeSpace onode_map;

    //pool options
    pool_opts_t pool_opts;

    OnodeRef get_onode(const ghobject_t& oid, bool create);

    // the terminology is confusing here, sorry!
    //
    //  blob_t     shared_blob_t
    //  !shared    unused                -> open
    //  shared     !loaded               -> open + shared
    //  shared     loaded                -> open + shared + loaded
    //
    // i.e.,
    //  open = SharedBlob is instantiated
    //  shared = blob_t shared flag is set; SharedBlob is hashed.
    //  loaded = SharedBlob::shared_blob_t is loaded from kv store
    void open_shared_blob(BlobRef b);
    void load_shared_blob(SharedBlobRef sb);
    void make_blob_shared(BlobRef b);

    BlobRef new_blob() {
      BlobRef b = new Blob;
      b->shared_blob = new SharedBlob(0, string(), cache);
      return b;
    }

    const coll_t &get_cid() override {
      return cid;
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

    void trim_cache();

    Collection(BlueStore *ns, Cache *ca, coll_t c);
  };
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    OnodeRef o;
    KeyValueDB::Iterator it;
    string head, tail;
  public:
    OmapIteratorImpl(CollectionRef c, OnodeRef o, KeyValueDB::Iterator it);
    int seek_to_first();
    int upper_bound(const string &after);
    int lower_bound(const string &to);
    bool valid();
    int next(bool validate=true);
    string key();
    bufferlist value();
    int status() {
      return 0;
    }
  };

  class OpSequencer;
  typedef boost::intrusive_ptr<OpSequencer> OpSequencerRef;

  struct TransContext {
    typedef enum {
      STATE_PREPARE,
      STATE_AIO_WAIT,
      STATE_IO_DONE,
      STATE_KV_QUEUED,
      STATE_KV_COMMITTING,
      STATE_KV_DONE,
      STATE_WAL_QUEUED,
      STATE_WAL_APPLYING,
      STATE_WAL_AIO_WAIT,
      STATE_WAL_CLEANUP,   // remove wal kv record
      STATE_WAL_DONE,
      STATE_FINISHING,
      STATE_DONE,
    } state_t;

    state_t state;

    const char *get_state_name() {
      switch (state) {
      case STATE_PREPARE: return "prepare";
      case STATE_AIO_WAIT: return "aio_wait";
      case STATE_IO_DONE: return "io_done";
      case STATE_KV_QUEUED: return "kv_queued";
      case STATE_KV_COMMITTING: return "kv_committing";
      case STATE_KV_DONE: return "kv_done";
      case STATE_WAL_QUEUED: return "wal_queued";
      case STATE_WAL_APPLYING: return "wal_applying";
      case STATE_WAL_AIO_WAIT: return "wal_aio_wait";
      case STATE_WAL_CLEANUP: return "wal_cleanup";
      case STATE_WAL_DONE: return "wal_done";
      case STATE_FINISHING: return "finishing";
      case STATE_DONE: return "done";
      }
      return "???";
    }

    void log_state_latency(PerfCounters *logger, int state) {
      utime_t lat, now = ceph_clock_now(g_ceph_context);
      lat = now - start;
      logger->tinc(state, lat);
      start = now;
    }

    OpSequencerRef osr;
    boost::intrusive::list_member_hook<> sequencer_item;

    uint64_t ops, bytes;

    set<OnodeRef> onodes;     ///< these need to be updated/written
    set<SharedBlobRef> shared_blobs;  ///< these need to be updated/written
    set<SharedBlobRef> shared_blobs_written; ///< update these on io completion

    KeyValueDB::Transaction t; ///< then we will commit this
    Context *oncommit;         ///< signal on commit
    Context *onreadable;         ///< signal on readable
    Context *onreadable_sync;         ///< signal on readable
    list<Context*> oncommits;  ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed

    boost::intrusive::list_member_hook<> wal_queue_item;
    bluestore_wal_transaction_t *wal_txn; ///< wal transaction (if any)
    vector<OnodeRef> wal_op_onodes;

    bool kv_submitted = false; ///< true when we've been submitted to kv db

    interval_set<uint64_t> allocated, released;
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
      bool is_empty() {
        return values[STATFS_ALLOCATED] == 0 &&
          values[STATFS_STORED] == 0 &&
          values[STATFS_COMPRESSED] == 0 &&
          values[STATFS_COMPRESSED_ORIGINAL] == 0 &&
          values[STATFS_COMPRESSED_ALLOCATED] == 0;
      }
      void decode(bufferlist::iterator& it) {
        for (size_t i = 0; i < STATFS_LAST; i++) {
          ::decode(values[i], it);
        }
      }

      void encode(bufferlist& bl) {
        for (size_t i = 0; i < STATFS_LAST; i++) {
          ::encode(values[i], bl);
        }
      }
    } statfs_delta;


    IOContext ioc;

    CollectionRef first_collection;  ///< first referenced collection

    uint64_t seq = 0;
    utime_t start;

    uint64_t last_nid = 0;     ///< if non-zero, highest new nid we allocated
    uint64_t last_blobid = 0;  ///< if non-zero, highest new blobid we allocated

    explicit TransContext(OpSequencer *o)
      : state(STATE_PREPARE),
	osr(o),
	ops(0),
	bytes(0),
	oncommit(NULL),
	onreadable(NULL),
	onreadable_sync(NULL),
	wal_txn(NULL),
	ioc(this),
	start(ceph_clock_now(g_ceph_context)) {
    }
    ~TransContext() {
      delete wal_txn;
    }

    void write_onode(OnodeRef &o) {
      onodes.insert(o);
    }
    void write_shared_blob(SharedBlobRef &sb) {
      shared_blobs.insert(sb);
    }
  };

  class OpSequencer : public Sequencer_impl {
  public:
    std::mutex qlock;
    std::condition_variable qcond;
    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
        TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
	TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::wal_queue_item> > wal_queue_t;
    wal_queue_t wal_q; ///< transactions

    boost::intrusive::list_member_hook<> wal_osr_queue_item;

    Sequencer *parent;

    std::mutex wal_apply_mutex;

    uint64_t last_seq = 0;

    std::atomic_int kv_committing_serially = {0};

    OpSequencer()
	//set the qlock to PTHREAD_MUTEX_RECURSIVE mode
      : parent(NULL) {
    }
    ~OpSequencer() {
      assert(q.empty());
    }

    void queue_new(TransContext *txc) {
      std::lock_guard<std::mutex> l(qlock);
      txc->seq = ++last_seq;
      q.push_back(*txc);
    }

    void flush() {
      std::unique_lock<std::mutex> l(qlock);
      while (!q.empty())
	qcond.wait(l);
    }

    bool flush_commit(Context *c) {
      std::lock_guard<std::mutex> l(qlock);
      if (q.empty()) {
	return true;
      }
      TransContext *txc = &q.back();
      if (txc->state >= TransContext::STATE_KV_DONE) {
	return true;
      }
      assert(txc->state < TransContext::STATE_KV_DONE);
      txc->oncommits.push_back(c);
      return false;
    }

    /// if there is a wal on @seq, wait for it to apply
    void wait_for_wal_on_seq(uint64_t seq) {
      std::unique_lock<std::mutex> l(qlock);
      restart:
      for (OpSequencer::q_list_t::reverse_iterator p = q.rbegin();
	   p != q.rend();
	   ++p) {
	if (p->seq == seq) {
	  TransContext *txc = &(*p);
	  if (txc->wal_txn) {
	    while (txc->state < TransContext::STATE_WAL_CLEANUP) {
	      txc->osr->qcond.wait(l);
	      goto restart;  // txc may have gone away
	    }
	  }
	  break;
	}
	if (p->seq < seq)
	  break;
      }
    }
  };

  class WALWQ : public ThreadPool::WorkQueue<TransContext> {
    // We need to order WAL items within each Sequencer.  To do that,
    // queue each txc under osr, and queue the osr's here.  When we
    // dequeue an txc, requeue the osr if there are more pending, and
    // do it at the end of the list so that the next thread does not
    // get a conflicted txc.  Hold an osr mutex while doing the wal to
    // preserve the ordering.
  public:
    typedef boost::intrusive::list<
      OpSequencer,
      boost::intrusive::member_hook<
	OpSequencer,
	boost::intrusive::list_member_hook<>,
	&OpSequencer::wal_osr_queue_item> > wal_osr_queue_t;

  private:
    BlueStore *store;
    wal_osr_queue_t wal_queue;

  public:
    WALWQ(BlueStore *s, time_t ti, time_t sti, ThreadPool *tp)
      : ThreadPool::WorkQueue<TransContext>("BlueStore::WALWQ", ti, sti, tp),
	store(s) {
    }
    bool _empty() {
      return wal_queue.empty();
    }
    bool _enqueue(TransContext *i) {
      if (i->osr->wal_q.empty()) {
	wal_queue.push_back(*i->osr);
      }
      i->osr->wal_q.push_back(*i);
      return true;
    }
    void _dequeue(TransContext *p) {
      assert(0 == "not needed, not implemented");
    }
    TransContext *_dequeue() {
      if (wal_queue.empty())
	return NULL;
      OpSequencer *osr = &wal_queue.front();
      TransContext *i = &osr->wal_q.front();
      osr->wal_q.pop_front();
      wal_queue.pop_front();
      if (!osr->wal_q.empty()) {
	// requeue at the end to minimize contention
	wal_queue.push_back(*i->osr);
      }

      // preserve wal ordering for this sequencer by taking the lock
      // while still holding the queue lock
      i->osr->wal_apply_mutex.lock();
      return i;
    }
    void _process(TransContext *i, ThreadPool::TPHandle &) override {
      store->_wal_apply(i);
      i->osr->wal_apply_mutex.unlock();
    }
    void _clear() {
      assert(wal_queue.empty());
    }

    void flush() {
      drain();
    }
  };

  struct KVSyncThread : public Thread {
    BlueStore *store;
    explicit KVSyncThread(BlueStore *s) : store(s) {}
    void *entry() {
      store->_kv_sync_thread();
      return NULL;
    }
  };

  // --------------------------------------------------------
  // members
private:
  CephContext *cct;
  BlueFS *bluefs;
  unsigned bluefs_shared_bdev;  ///< which bluefs bdev we are sharing
  KeyValueDB *db;
  BlockDevice *bdev;
  std::string freelist_type;
  FreelistManager *fm;
  Allocator *alloc;
  uuid_d fsid;
  int path_fd;  ///< open handle to $path
  int fsid_fd;  ///< open handle (locked) to $path/fsid
  bool mounted;

  RWLock coll_lock;    ///< rwlock to protect coll_map
  mempool::bluestore_meta_other::unordered_map<coll_t, CollectionRef> coll_map;

  vector<Cache*> cache_shards;

  std::atomic<uint64_t> nid_last = {0};
  std::atomic<uint64_t> nid_max = {0};
  std::atomic<uint64_t> blobid_last = {0};
  std::atomic<uint64_t> blobid_max = {0};

  Throttle throttle_ops, throttle_bytes;          ///< submit to commit
  Throttle throttle_wal_ops, throttle_wal_bytes;  ///< submit to wal complete

  interval_set<uint64_t> bluefs_extents;  ///< block extents owned by bluefs

  std::mutex wal_lock;
  std::atomic<uint64_t> wal_seq = {0};
  ThreadPool wal_tp;
  WALWQ wal_wq;

  int m_finisher_num;
  vector<Finisher*> finishers;

  KVSyncThread kv_sync_thread;
  std::mutex kv_lock;
  std::condition_variable kv_cond, kv_sync_cond;
  bool kv_stop;
  deque<TransContext*> kv_queue;             ///< ready, already submitted
  deque<TransContext*> kv_queue_unsubmitted; ///< ready, need submit by kv thread
  deque<TransContext*> kv_committing;        ///< currently syncing
  deque<TransContext*> wal_cleanup_queue;    ///< wal done, ready for cleanup

  PerfCounters *logger;

  std::mutex reap_lock;
  list<CollectionRef> removed_collections;

  RWLock debug_read_error_lock;
  set<ghobject_t, ghobject_t::BitwiseComparator> debug_data_error_objects;
  set<ghobject_t, ghobject_t::BitwiseComparator> debug_mdata_error_objects;

  std::atomic<int> csum_type;

  uint64_t block_size;     ///< block size of block device (power of 2)
  uint64_t block_mask;     ///< mask to get just the block offset
  size_t block_size_order; ///< bits to shift to get block size

  uint64_t min_alloc_size = 0; ///< minimum allocation unit (power of 2)
  uint64_t min_min_alloc_size = 0; /// < minimum seen min_alloc_size
  size_t min_alloc_size_order = 0; ///< bits for min_alloc_size

  uint64_t max_alloc_size; ///< maximum allocation unit (power of 2)

  bool sync_wal_apply;	  ///< see config option bluestore_sync_wal_apply

  std::atomic<Compressor::CompressionMode> comp_mode = {Compressor::COMP_NONE}; ///< compression mode
  CompressorRef compressor;
  std::atomic<uint64_t> comp_min_blob_size = {0};
  std::atomic<uint64_t> comp_max_blob_size = {0};

  // cache trim control

  // note that these update in a racy way, but we don't *really* care if
  // they're perfectly accurate.  they are all word sized so they will
  // individually update atomically, but may not be coherent with each other.
  size_t mempool_seq = 0;
  size_t mempool_bytes = 0;
  size_t mempool_onodes = 0;

  void get_mempool_stats(size_t *seq, uint64_t *bytes, uint64_t *onodes) {
    *seq = mempool_seq;
    *bytes = mempool_bytes;
    *onodes = mempool_onodes;
  }

  struct MempoolThread : public Thread {
    BlueStore *store;
    Cond cond;
    Mutex lock;
    bool stop = false;
  public:
    explicit MempoolThread(BlueStore *s)
      : store(s),
	lock("BlueStore::MempoolThread::lock") {}
    void *entry();
    void init() {
      assert(stop == false);
      create("bstore_mempool");
    }
    void shutdown() {
      lock.Lock();
      stop = true;
      cond.Signal();
      lock.Unlock();
      join();
    }
  } mempool_thread;

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
  int _open_bdev(bool create);
  void _close_bdev();
  int _open_db(bool create);
  void _close_db();
  int _open_fm(bool create);
  void _close_fm();
  int _open_alloc();
  void _close_alloc();
  int _open_collections(int *errors=0);
  void _close_collections();

  int _setup_block_symlink_or_file(string name, string path, uint64_t size,
				   bool create);

  int _write_bdev_label(string path, bluestore_bdev_label_t label);
  static int _read_bdev_label(string path, bluestore_bdev_label_t *label);
  int _check_or_set_bdev_label(string path, uint64_t size, string desc,
			       bool create);

  void _save_min_min_alloc_size(uint64_t new_val);
  int _open_super_meta();

  int _reconcile_bluefs_freespace();
  int _balance_bluefs_freespace(vector<bluestore_pextent_t> *extents);
  void _commit_bluefs_freespace(const vector<bluestore_pextent_t>& extents);

  CollectionRef _get_collection(const coll_t& cid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();
  void _update_cache_logger();

  void _assign_nid(TransContext *txc, OnodeRef o);
  uint64_t _assign_blobid(TransContext *txc);

  void _dump_onode(OnodeRef o, int log_level=30);
  void _dump_extent_map(ExtentMap& em, int log_level=30);
  void _dump_transaction(Transaction *t, int log_level = 30);

  TransContext *_txc_create(OpSequencer *osr);
  void _txc_update_store_statfs(TransContext *txc);
  void _txc_add_transaction(TransContext *txc, Transaction *t);
  void _txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t);
  void _txc_state_proc(TransContext *txc);
  void _txc_aio_submit(TransContext *txc);
  void _txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t);
public:
  void _txc_aio_finish(void *p) {
    _txc_state_proc(static_cast<TransContext*>(p));
  }
private:
  void _txc_finish_io(TransContext *txc);
  void _txc_finish_kv(TransContext *txc);
  void _txc_finish(TransContext *txc);

  void _osr_reap_done(OpSequencer *osr);

  void _kv_sync_thread();
  void _kv_stop() {
    {
      std::lock_guard<std::mutex> l(kv_lock);
      kv_stop = true;
      kv_cond.notify_all();
    }
    kv_sync_thread.join();
    kv_stop = false;
  }

  bluestore_wal_op_t *_get_wal_op(TransContext *txc, OnodeRef o);
  int _wal_apply(TransContext *txc);
  int _wal_finish(TransContext *txc);
  int _do_wal_op(TransContext *txc, bluestore_wal_op_t& wo);
  int _wal_replay();

  int _fsck_check_extents(
    const ghobject_t& oid,
    const vector<bluestore_pextent_t>& extents,
    bool compressed,
    boost::dynamic_bitset<> &used_blocks,
    store_statfs_t& expected_statfs);

  void _buffer_cache_write(
    TransContext *txc,
    BlobRef b,
    uint64_t offset,
    bufferlist& bl,
    unsigned flags) {
    b->shared_blob->bc.write(txc->seq, offset, bl, flags);
    txc->shared_blobs_written.insert(b->shared_blob);
  }

  int _collection_list(Collection *c, ghobject_t start, ghobject_t end,
    bool sort_bitwise, int max, vector<ghobject_t> *ls, ghobject_t *next);

  template <typename T, typename F>
  T select_option(const std::string& opt_name, T val1, F f) {
    //NB: opt_name reserved for future use
    boost::optional<T> val2 = f();
    if (val2) {
      return *val2;
    }
    return val1;
  }
public:
  BlueStore(CephContext *cct, const string& path);
  ~BlueStore();

  string get_type() override {
    return "bluestore";
  }

  bool needs_journal() override { return false; };
  bool wants_journal() override { return false; };
  bool allows_journal() override { return false; };

  static int get_block_device_fsid(const string& path, uuid_d *fsid);

  bool test_mount_in_use() override;

  int mount() override;
  int umount() override;
  void _sync();

  int fsck(bool deep) override;

  void set_cache_shards(unsigned num) override;

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

public:
  int statfs(struct store_statfs_t *buf) override;

  bool exists(const coll_t& cid, const ghobject_t& oid) override;
  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int set_collection_opts(
    const coll_t& cid,
    const pool_opts_t& opts) override;
  int stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int stat(
    CollectionHandle &c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  int _do_read(
    Collection *c,
    OnodeRef o,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0);

  int fiemap(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, bufferlist& bl) override;
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, bufferlist& bl) override;

  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;

  int getattrs(const coll_t& cid, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;

  int list_collections(vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t &c) override;

  bool collection_exists(const coll_t& c) override;
  int collection_empty(const coll_t& c, bool *empty) override;
  int collection_bits(const coll_t& c) override;

  int collection_list(const coll_t& cid, ghobject_t start, ghobject_t end,
		      bool sort_bitwise, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;
  int collection_list(CollectionHandle &c, ghobject_t start, ghobject_t end,
		      bool sort_bitwise, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;

  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;
  int omap_get(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;

  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;
  int omap_get_header(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;
  int omap_get_keys(
    CollectionHandle &c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;

  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;
  int omap_check_keys(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
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

  objectstore_perf_stat_t get_cur_stats() override {
    return objectstore_perf_stat_t();
  }

  int queue_transactions(
    Sequencer *osr,
    vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;

  // error injection
  void inject_data_error(const ghobject_t& o) override {
    RWLock::WLocker l(debug_read_error_lock);
    debug_data_error_objects.insert(o);
  }
  void inject_mdata_error(const ghobject_t& o) override {
    RWLock::WLocker l(debug_read_error_lock);
    debug_mdata_error_objects.insert(o);
  }
private:
  bool _debug_data_eio(const ghobject_t& o) {
    if (!g_conf->bluestore_debug_inject_read_err) {
      return false;
    }
    RWLock::RLocker l(debug_read_error_lock);
    return debug_data_error_objects.count(o);
  }
  bool _debug_mdata_eio(const ghobject_t& o) {
    if (!g_conf->bluestore_debug_inject_read_err) {
      return false;
    }
    RWLock::RLocker l(debug_read_error_lock);
    return debug_mdata_error_objects.count(o);
  }
  void _debug_obj_on_delete(const ghobject_t& o) {
    if (g_conf->bluestore_debug_inject_read_err) {
      RWLock::WLocker l(debug_read_error_lock);
      debug_data_error_objects.erase(o);
      debug_mdata_error_objects.erase(o);
    }
  }

private:

  // --------------------------------------------------------
  // read processing internal methods
  int _verify_csum(OnodeRef& o,
		   const bluestore_blob_t* blob,
		   uint64_t blob_xoffset,
		   const bufferlist& bl) const;
  int _decompress(bufferlist& source, bufferlist* result);


  // --------------------------------------------------------
  // write ops

  struct WriteContext {
    bool buffered = false;          ///< buffered write
    bool compress = false;          ///< compressed write
    uint64_t target_blob_size = 0;  ///< target (max) blob size
    uint8_t blob_depth = 0;         ///< depth of the logical extent
    unsigned csum_order = 0;        ///< target checksum chunk order

    extent_map_t old_extents;       ///< must deref these blobs

    struct write_item {
      BlobRef b;
      uint64_t blob_length;
      uint64_t b_off;
      bufferlist bl;
      bool mark_unused;

      write_item(BlobRef b, uint64_t blob_len, uint64_t o, bufferlist& bl, bool _mark_unused)
       : b(b), blob_length(blob_len), b_off(o), bl(bl), mark_unused(_mark_unused) {}
    };
    vector<write_item> writes;                 ///< blobs we're writing

    void write(BlobRef b, uint64_t blob_len, uint64_t o, bufferlist& bl, bool _mark_unused) {
      writes.emplace_back(write_item(b, blob_len, o, bl, _mark_unused));
    }
  };

  void _do_write_small(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx);
  void _do_write_big(
    TransContext *txc,
    CollectionRef &c,
    OnodeRef o,
    uint64_t offset, uint64_t length,
    bufferlist::iterator& blp,
    WriteContext *wctx);
  int _do_alloc_write(
    TransContext *txc,
    CollectionRef c,
    WriteContext *wctx);
  void _wctx_finish(
    TransContext *txc,
    CollectionRef& c,
    OnodeRef o,
    WriteContext *wctx);

  int _do_transaction(Transaction *t,
		      TransContext *txc,
		      ThreadPool::TPHandle *handle);

  int _write(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o,
	     uint64_t offset, size_t len,
	     bufferlist& bl,
	     uint32_t fadvise_flags);
  void _pad_zeros(bufferlist *bl, uint64_t *offset,
		  uint64_t chunk_size);

  int _do_write(TransContext *txc,
		CollectionRef &c,
		OnodeRef o,
		uint64_t offset, uint64_t length,
		bufferlist& bl,
		uint32_t fadvise_flags);
  void _do_write_data(TransContext *txc,
                      CollectionRef& c,
                      OnodeRef o,
                      uint64_t offset,
                      uint64_t length,
                      bufferlist& bl,
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
  int _do_truncate(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef o,
		   uint64_t offset);
  int _truncate(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		uint64_t offset);
  int _remove(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o);
  int _do_remove(TransContext *txc,
		 CollectionRef& c,
		 OnodeRef o);
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       const string& name,
	       bufferptr& val);
  int _setattrs(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		const map<string,bufferptr>& aset);
  int _rmattr(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o,
	      const string& name);
  int _rmattrs(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o);
  void _do_omap_clear(TransContext *txc, uint64_t id);
  int _omap_clear(TransContext *txc,
		  CollectionRef& c,
		  OnodeRef& o);
  int _omap_setkeys(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    bufferlist& bl);
  int _omap_setheader(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      bufferlist& header);
  int _omap_rmkeys(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   bufferlist& bl);
  int _omap_rmkey_range(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const string& first, const string& last);
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
  int _move_ranges_destroy_src(
    TransContext *txc,
    CollectionRef& c,
    OnodeRef& oldo,
    OnodeRef& newo,
    const vector<std::pair<uint64_t,uint64_t>> move_info);
  int _rename(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& oldo,
	      OnodeRef& newo,
	      const ghobject_t& new_oid);
  int _create_collection(TransContext *txc, coll_t cid, unsigned bits,
			 CollectionRef *c);
  int _remove_collection(TransContext *txc, coll_t cid, CollectionRef *c);
  int _split_collection(TransContext *txc,
			CollectionRef& c,
			CollectionRef& d,
			unsigned bits, int rem);

};

inline ostream& operator<<(ostream& out, const BlueStore::OpSequencer& s) {
  return out << *s.parent;
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

#endif
