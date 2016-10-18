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

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H

#include <ostream>
#include <bitset>
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
#include "include/small_encoding.h"
#include "common/hobject.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"
#include "include/mempool.h"

namespace ceph {
  class Formatter;
}

/// label for block device
struct bluestore_bdev_label_t {
  uuid_d osd_uuid;     ///< osd uuid
  uint64_t size;       ///< device size
  utime_t btime;       ///< birth time
  string description;  ///< device description

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_bdev_label_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_bdev_label_t)

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l);

/// collection metadata
struct bluestore_cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit bluestore_cnode_t(int b=0) : bits(b) {}

  DENC(bluestore_cnode_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.bits, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_cnode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_cnode_t)

class AllocExtent {
public:
  uint64_t offset;
  uint32_t length;

  AllocExtent() { 
    offset = 0;
    length = 0;
  }

  AllocExtent(int64_t off, int32_t len) : offset(off), length(len) { }
  uint64_t end() const {
    return offset + length;
  }
};

class ExtentList {
  std::vector<AllocExtent> *m_extents;
  int64_t m_num_extents;
  int64_t m_block_size;
  uint64_t m_max_alloc_size;

public:
  void init(std::vector<AllocExtent> *extents, int64_t block_size, uint64_t max_alloc_size) {
    m_extents = extents;
    m_num_extents = 0;
    m_block_size = block_size;
    m_max_alloc_size = max_alloc_size;
  }

  ExtentList(std::vector<AllocExtent> *extents, int64_t block_size) {
    init(extents, block_size, 0);
  }

  ExtentList(std::vector<AllocExtent> *extents, int64_t block_size, uint64_t max_alloc_size) {
    init(extents, block_size, max_alloc_size);
  }

  void reset() {
    m_num_extents = 0;
  }

  void add_extents(int64_t start, int64_t count);

  std::vector<AllocExtent> *get_extents() {
    return m_extents;
  }

  std::pair<int64_t, int64_t> get_nth_extent(int index) {
      return std::make_pair
            ((*m_extents)[index].offset / m_block_size,
             (*m_extents)[index].length / m_block_size);
  }

  int64_t get_extent_count() {
    return m_num_extents;
  }
};


/// pextent: physical extent
struct bluestore_pextent_t : public AllocExtent{
  const static uint64_t INVALID_OFFSET = ~0ull;

  bluestore_pextent_t() : AllocExtent() {}
  bluestore_pextent_t(uint64_t o, uint64_t l) : AllocExtent(o, l) {}
  bluestore_pextent_t(AllocExtent &ext) : AllocExtent(ext.offset, ext.length) { }

  bool is_valid() const {
    return offset != INVALID_OFFSET;
  }

  DENC(bluestore_pextent_t, v, p) {
    denc(v.offset, p);
    denc(v.length, p);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_pextent_t*>& ls);
};
WRITE_CLASS_DENC(bluestore_pextent_t)

ostream& operator<<(ostream& out, const bluestore_pextent_t& o);

template<>
struct denc_traits<vector<bluestore_pextent_t>> {
  enum { supported = true };
  enum { bounded = false };
  enum { featured = false };
  static void bound_encode(const vector<bluestore_pextent_t>& v, size_t& p) {
    p += sizeof(uint32_t);
    size_t per = 0;
    denc(*(bluestore_pextent_t*)nullptr, per);
    p += per * v.size();
  }
  static void encode(const vector<bluestore_pextent_t>& v,
		     bufferlist::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(vector<bluestore_pextent_t>& v, bufferptr::iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.resize(num);
    for (unsigned i=0; i<num; ++i) {
      denc(v[i], p);
    }
  }
};


/// extent_map: a map of reference counted extents
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;
    uint32_t refs;
    record_t(uint32_t l=0, uint32_t r=0) : length(l), refs(r) {}
    DENC(bluestore_extent_ref_map_t::record_t, v, p) {
      denc_varint_lowz(v.length, p);
      denc_varint(v.refs, p);
    }
  };

  typedef mempool::bluestore_meta_other::map<uint64_t,record_t> map_t;
  map_t ref_map;

  void _check() const;
  void _maybe_merge_left(map_t::iterator& p);

  void clear() {
    ref_map.clear();
  }
  bool empty() const {
    return ref_map.empty();
  }

  void get(uint64_t offset, uint32_t len);
  void put(uint64_t offset, uint32_t len, vector<bluestore_pextent_t> *release);

  bool contains(uint64_t offset, uint32_t len) const;
  bool intersects(uint64_t offset, uint32_t len) const;

  void bound_encode(size_t& p) const {
    denc((uint32_t)0, p);
    size_t elem_size = 0;
    denc_varint_lowz((uint32_t)0, p);
    ((const record_t*)nullptr)->bound_encode(elem_size);
    p += elem_size * ref_map.size();
  }
  void encode(bufferlist::contiguous_appender& p) const {
    uint32_t n = ref_map.size();
    denc_varint(n, p);
    if (n) {
      auto i = ref_map.begin();
      denc_varint_lowz(i->first, p);
      i->second.encode(p);
      int64_t pos = i->first;
      while (--n) {
	++i;
	denc_varint_lowz((int64_t)i->first - pos, p);
	i->second.encode(p);
	pos = i->first;
      }
    }
  }
  void decode(bufferptr::iterator& p) {
    uint32_t n;
    denc_varint(n, p);
    if (n) {
      int64_t pos;
      denc_varint_lowz(pos, p);
      ref_map[pos].decode(p);
      while (--n) {
	int64_t delta;
	denc_varint_lowz(delta, p);
	pos += delta;
	ref_map[pos].decode(p);
      }
    }
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_extent_ref_map_t*>& o);
};
WRITE_CLASS_DENC(bluestore_extent_ref_map_t)


ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& rm);
static inline bool operator==(const bluestore_extent_ref_map_t::record_t& l,
			      const bluestore_extent_ref_map_t::record_t& r) {
  return l.length == r.length && l.refs == r.refs;
}
static inline bool operator==(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return l.ref_map == r.ref_map;
}
static inline bool operator!=(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return !(l == r);
}

/// blob: a piece of data on disk
struct bluestore_blob_t {
  enum {
    FLAG_MUTABLE = 1,         ///< blob can be overwritten or split
    FLAG_COMPRESSED = 2,      ///< blob is compressed
    FLAG_CSUM = 4,            ///< blob has checksums
    FLAG_HAS_UNUSED = 8,      ///< blob has unused map
    FLAG_SHARED = 16,         ///< blob is shared; see external SharedBlob
  };
  static string get_flags_string(unsigned flags);

  vector<bluestore_pextent_t> extents;///< raw data position on device
  uint64_t sbid = 0;                  ///< shared blob id (if shared)
  uint32_t compressed_length_orig = 0;///< original length of compressed blob if any
  uint32_t compressed_length = 0;     ///< compressed length if any
  uint32_t flags = 0;                 ///< FLAG_*

  uint8_t csum_type = Checksummer::CSUM_NONE;      ///< CSUM_*
  uint8_t csum_chunk_order = 0;       ///< csum block size is 1<<block_order bytes

  bufferptr csum_data;                ///< opaque vector of csum data

  typedef uint16_t unused_uint_t;
  typedef std::bitset<sizeof(unused_uint_t) * 8> unused_t;
  unused_t unused;                    ///< portion that has never been written to

  bluestore_blob_t(uint32_t f = 0) : flags(f) {}

  DENC_HELPERS;
  void bound_encode(size_t& p) const {
    p += 2 + 4;
    denc(extents, p);
    denc_varint(flags, p);
    denc_varint(sbid, p);
    denc_varint_lowz(compressed_length_orig, p);
    denc_varint_lowz(compressed_length, p);
    denc(csum_type, p);
    denc(csum_chunk_order, p);
    denc(csum_data, p);
    p += sizeof(unsigned long long);
  }
  void encode(bufferlist::contiguous_appender& p) const {
    DENC_START(1, 1, p);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_shared()) {
      denc_varint(sbid, p);
    }
    if (is_compressed()) {
      denc_varint_lowz(compressed_length_orig, p);
      denc_varint_lowz(compressed_length, p);
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      denc(csum_data, p);
    }
    if (has_unused()) {
      denc(unused_uint_t(unused.to_ullong()), p);
    }
    DENC_FINISH(p);
  }
  void decode(bufferptr::iterator& p) {
    DENC_START(1, 1, p);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_shared()) {
      denc_varint(sbid, p);
    }
    if (is_compressed()) {
      denc_varint_lowz(compressed_length_orig, p);
      denc_varint_lowz(compressed_length, p);
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      denc(csum_data, p);
    }
    if (has_unused()) {
      unused_uint_t val;
      denc(val, p);
      unused = unused_t(val);
    }
    DENC_FINISH(p);
  }

  bool can_split() const {
    return
      !has_flag(FLAG_SHARED) &&
      !has_flag(FLAG_COMPRESSED) &&
      !has_flag(FLAG_HAS_UNUSED);     // splitting unused set is complex
  }
  bool can_split_at(uint32_t blob_offset) const {
    return !has_csum() || blob_offset % get_csum_chunk_size() == 0;
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_blob_t*>& ls);

  bool has_flag(unsigned f) const {
    return flags & f;
  }
  void set_flag(unsigned f) {
    flags |= f;
  }
  void clear_flag(unsigned f) {
    flags &= ~f;
  }
  string get_flags_string() const {
    return get_flags_string(flags);
  }

  void set_compressed(uint64_t clen_orig, uint64_t clen) {
    set_flag(FLAG_COMPRESSED);
    compressed_length_orig = clen_orig;
    compressed_length = clen;
  }
  bool is_mutable() const {
    return has_flag(FLAG_MUTABLE);
  }
  bool is_compressed() const {
    return has_flag(FLAG_COMPRESSED);
  }
  bool has_csum() const {
    return has_flag(FLAG_CSUM);
  }
  bool has_unused() const {
    return has_flag(FLAG_HAS_UNUSED);
  }
  bool is_shared() const {
    return has_flag(FLAG_SHARED);
  }

  /// return chunk (i.e. min readable block) size for the blob
  uint64_t get_chunk_size(uint64_t dev_block_size) const {
    return has_csum() ?
      MAX(dev_block_size, get_csum_chunk_size()) : dev_block_size;
  }
  uint32_t get_csum_chunk_size() const {
    return 1 << csum_chunk_order;
  }
  uint32_t get_compressed_payload_length() const {
    return is_compressed() ? compressed_length : 0;
  }
  uint32_t get_compressed_payload_original_length() const {
    return is_compressed() ? compressed_length_orig : 0;
  }
  uint64_t calc_offset(uint64_t x_off, uint64_t *plen) const {
    auto p = extents.begin();
    assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    if (plen)
      *plen = p->length - x_off;
    return p->offset + x_off;
  }

  /// return true if the entire range is allocated (mapped to extents on disk)
  bool is_allocated(uint64_t b_off, uint64_t b_len) const {
    auto p = extents.begin();
    assert(p != extents.end());
    while (b_off >= p->length) {
      b_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    b_len += b_off;
    while (b_len) {
      assert(p != extents.end());
      if (!p->is_valid()) {
	return false;
      }
      if (p->length >= b_len) {
	return true;
      }
      b_len -= p->length;
      ++p;
    }
    assert(0 == "we should not get here");
  }

  /// return true if the logical range has never been used
  bool is_unused(uint64_t offset, uint64_t length) const {
    if (!has_unused()) {
      return false;
    }
    uint64_t blob_len = get_logical_length();
    assert((blob_len % unused.size()) == 0);
    assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / unused.size();
    uint64_t start = offset / chunk_size;
    uint64_t end = ROUND_UP_TO(offset + length, chunk_size) / chunk_size;
    assert(end <= unused.size());
    auto i = start;
    while (i < end && unused[i]) {
      i++;
    }
    return i >= end;
  }

  /// mark a range that has never been used
  void add_unused(uint64_t offset, uint64_t length) {
    uint64_t blob_len = get_logical_length();
    assert((blob_len % unused.size()) == 0);
    assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / unused.size();
    uint64_t start = ROUND_UP_TO(offset, chunk_size) / chunk_size;
    uint64_t end = (offset + length) / chunk_size;
    assert(end <= unused.size());
    for (auto i = start; i < end; ++i) {
      unused[i] = 1;
    }
    if (start != end) {
      set_flag(FLAG_HAS_UNUSED);
    }
  }

  /// indicate that a range has (now) been used.
  void mark_used(uint64_t offset, uint64_t length) {
    if (has_unused()) {
      uint64_t blob_len = get_logical_length();
      assert((blob_len % unused.size()) == 0);
      assert(offset + length <= blob_len);
      uint64_t chunk_size = blob_len / unused.size();
      uint64_t start = offset / chunk_size;
      uint64_t end = ROUND_UP_TO(offset + length, chunk_size) / chunk_size;
      assert(end <= unused.size());
      for (auto i = start; i < end; ++i) {
        unused[i] = 0;
      }
      if (unused.none()) {
        clear_flag(FLAG_HAS_UNUSED);
      }
    }
  }

  int map(uint64_t x_off, uint64_t x_len,
	   std::function<int(uint64_t,uint64_t)> f) const {
    auto p = extents.begin();
    assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    while (x_len > 0) {
      assert(p != extents.end());
      uint64_t l = MIN(p->length - x_off, x_len);
      int r = f(p->offset + x_off, l);
      if (r < 0)
        return r;
      x_off = 0;
      x_len -= l;
      ++p;
    }
    return 0;
  }
  void map_bl(uint64_t x_off,
	      bufferlist& bl,
	      std::function<void(uint64_t,uint64_t,bufferlist&)> f) const {
    auto p = extents.begin();
    assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    bufferlist::iterator it = bl.begin();
    uint64_t x_len = bl.length();
    while (x_len > 0) {
      assert(p != extents.end());
      uint64_t l = MIN(p->length - x_off, x_len);
      bufferlist t;
      it.copy(l, t);
      f(p->offset + x_off, l, t);
      x_off = 0;
      x_len -= l;
      ++p;
    }
  }

  uint32_t get_ondisk_length() const {
    uint32_t len = 0;
    for (auto &p : extents) {
      len += p.length;
    }
    return len;
  }

  uint32_t get_logical_length() const {
    if (is_compressed()) {
      return compressed_length_orig;
    } else {
      return get_ondisk_length();
    }
  }
  size_t get_csum_value_size() const;

  size_t get_csum_count() const {
    size_t vs = get_csum_value_size();
    if (!vs)
      return 0;
    return csum_data.length() / vs;
  }
  uint64_t get_csum_item(unsigned i) const {
    size_t cs = get_csum_value_size();
    const char *p = csum_data.c_str();
    switch (cs) {
    case 0:
      assert(0 == "no csum data, bad index");
    case 1:
      return reinterpret_cast<const uint8_t*>(p)[i];
    case 2:
      return reinterpret_cast<const __le16*>(p)[i];
    case 4:
      return reinterpret_cast<const __le32*>(p)[i];
    case 8:
      return reinterpret_cast<const __le64*>(p)[i];
    default:
      assert(0 == "unrecognized csum word size");
    }
  }
  const char *get_csum_item_ptr(unsigned i) const {
    size_t cs = get_csum_value_size();
    return csum_data.c_str() + (cs * i);
  }
  char *get_csum_item_ptr(unsigned i) {
    size_t cs = get_csum_value_size();
    return csum_data.c_str() + (cs * i);
  }

  void init_csum(unsigned type, unsigned order, unsigned len) {
    flags |= FLAG_CSUM;
    csum_type = type;
    csum_chunk_order = order;
    csum_data = buffer::create(get_csum_value_size() * len / get_csum_chunk_size());
    csum_data.zero();
  }

  /// calculate csum for the buffer at the given b_off
  void calc_csum(uint64_t b_off, const bufferlist& bl);

  /// verify csum: return -EOPNOTSUPP for unsupported checksum type;
  /// return -1 and valid(nonnegative) b_bad_off for checksum error;
  /// return 0 if all is well.
  int verify_csum(uint64_t b_off, const bufferlist& bl, int* b_bad_off,
		  uint64_t *bad_csum) const;

  bool can_prune_tail() const {
    return
      extents.size() > 1 &&  // if it's all invalid it's not pruning.
      !extents.back().is_valid() &&
      !has_unused();
  }
  void prune_tail() {
    extents.pop_back();
    if (has_csum()) {
      bufferptr t;
      t.swap(csum_data);
      csum_data = bufferptr(t.c_str(),
			    get_logical_length() / get_csum_chunk_size() *
			    get_csum_value_size());
    }
  }
};
WRITE_CLASS_DENC(bluestore_blob_t)

ostream& operator<<(ostream& out, const bluestore_blob_t& o);


/// shared blob state
struct bluestore_shared_blob_t {
  bluestore_extent_ref_map_t ref_map;  ///< shared blob extents

  DENC(bluestore_shared_blob_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.ref_map, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_shared_blob_t*>& ls);

  bool empty() const {
    return ref_map.empty();
  }
};
WRITE_CLASS_DENC(bluestore_shared_blob_t)

ostream& operator<<(ostream& out, const bluestore_shared_blob_t& o);

/// onode: per-object metadata
struct bluestore_onode_t {
  uint64_t nid = 0;                    ///< numeric id (locally unique)
  uint64_t size = 0;                   ///< object size
  map<string, bufferptr> attrs;        ///< attrs
  uint64_t omap_head = 0;              ///< id for omap root node

  struct shard_info {
    uint32_t offset = 0;  ///< logical offset for start of shard
    uint32_t bytes = 0;   ///< encoded bytes
    uint32_t extents = 0; ///< extents
    DENC(shard_info, v, p) {
      denc(v.offset, p);
      denc(v.bytes, p);
      denc(v.extents, p);
    }
    void dump(Formatter *f) const;
  };
  vector<shard_info> extent_map_shards; ///< extent map shards (if any)

  uint32_t expected_object_size = 0;
  uint32_t expected_write_size = 0;
  uint32_t alloc_hint_flags = 0;

  /// get preferred csum chunk size
  size_t get_preferred_csum_order() const;

  DENC(bluestore_onode_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.nid, p);
    denc(v.size, p);
    denc(v.attrs, p);
    denc(v.omap_head, p);
    denc(v.extent_map_shards, p);
    denc(v.expected_object_size, p);
    denc(v.expected_write_size, p);
    denc(v.alloc_hint_flags, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_onode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_onode_t::shard_info)
WRITE_CLASS_DENC(bluestore_onode_t)

ostream& operator<<(ostream& out, const bluestore_onode_t::shard_info& si);

/// writeahead-logged op
struct bluestore_wal_op_t {
  typedef enum {
    OP_WRITE = 1,
  } type_t;
  __u8 op = 0;

  vector<bluestore_pextent_t> extents;
  bufferlist data;

  DENC(bluestore_wal_op_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.extents, p);
    denc(v.data, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_op_t*>& o);
};
WRITE_CLASS_DENC(bluestore_wal_op_t)


/// writeahead-logged transaction
struct bluestore_wal_transaction_t {
  uint64_t seq = 0;
  list<bluestore_wal_op_t> ops;
  interval_set<uint64_t> released;  ///< allocations to release after wal

  bluestore_wal_transaction_t() : seq(0) {}

  DENC(bluestore_wal_transaction_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seq, p);
    denc(v.ops, p);
    denc(v.released, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_transaction_t*>& o);
};
WRITE_CLASS_DENC(bluestore_wal_transaction_t)

struct bluestore_compression_header_t {
  uint8_t type = Compressor::COMP_ALG_NONE;
  uint32_t length = 0;

  bluestore_compression_header_t() {}
  bluestore_compression_header_t(uint8_t _type)
    : type(_type) {}

  DENC(bluestore_compression_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.length, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_compression_header_t*>& o);
};
WRITE_CLASS_DENC(bluestore_compression_header_t)


#endif
