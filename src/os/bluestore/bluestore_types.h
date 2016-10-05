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

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_cnode_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_cnode_t)

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

  void encode(bufferlist& bl) const {
    small_encode_lba(offset, bl);
    small_encode_varint_lowz(length, bl);
  }
  void decode(bufferlist::iterator& p) {
    small_decode_lba(offset, p);
    small_decode_varint_lowz(length, p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_pextent_t*>& ls);
};
WRITE_CLASS_ENCODER(bluestore_pextent_t)

ostream& operator<<(ostream& out, const bluestore_pextent_t& o);

void small_encode(const vector<bluestore_pextent_t>& v, bufferlist& bl);
void small_decode(vector<bluestore_pextent_t>& v, bufferlist::iterator& p);

/// extent_map: a map of reference counted extents
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;
    uint32_t refs;
    record_t(uint64_t l=0, uint32_t r=0) : length(l), refs(r) {}
    void encode(bufferlist& bl) const {
      small_encode_varint_lowz(length, bl);
      small_encode_varint(refs, bl);
    }
    void decode(bufferlist::iterator& p) {
      small_decode_varint_lowz(length, p);
      small_decode_varint(refs, p);
    }
  };
  WRITE_CLASS_ENCODER(record_t)

  map<uint64_t,record_t> ref_map;

  void _check() const;
  void _maybe_merge_left(map<uint64_t,record_t>::iterator& p);

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

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_extent_ref_map_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_extent_ref_map_t::record_t)
WRITE_CLASS_ENCODER(bluestore_extent_ref_map_t)

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

  enum CSumType {
    CSUM_NONE = 0,
    CSUM_XXHASH32 = 1,
    CSUM_XXHASH64 = 2,
    CSUM_CRC32C = 3,
    CSUM_CRC32C_16 = 4, // low 16 bits of crc32c
    CSUM_CRC32C_8 = 5,  // low 8 bits of crc32c
    CSUM_MAX,
  };
  static const char *get_csum_type_string(unsigned t) {
    switch (t) {
    case CSUM_NONE: return "none";
    case CSUM_XXHASH32: return "xxhash32";
    case CSUM_XXHASH64: return "xxhash64";
    case CSUM_CRC32C: return "crc32c";
    case CSUM_CRC32C_16: return "crc32c_16";
    case CSUM_CRC32C_8: return "crc32c_8";
    default: return "???";
    }
  }
  static int get_csum_string_type(const std::string &s) {
    if (s == "none")
      return CSUM_NONE;
    if (s == "xxhash32")
      return CSUM_XXHASH32;
    if (s == "xxhash64")
      return CSUM_XXHASH64;
    if (s == "crc32c")
      return CSUM_CRC32C;
    if (s == "crc32c_16")
      return CSUM_CRC32C_16;
    if (s == "crc32c_8")
      return CSUM_CRC32C_8;
    return -EINVAL;
  }

  enum CompressionAlgorithm {
    COMP_ALG_NONE = 0,
    COMP_ALG_SNAPPY = 1,
    COMP_ALG_ZLIB = 2,
  };

  static const char * get_comp_alg_name(int a) {
    switch (a) {
    case COMP_ALG_NONE: return "none";
    case COMP_ALG_SNAPPY: return "snappy";
    case COMP_ALG_ZLIB: return "zlib";
    default: return "???";
    }
  }

  static int get_comp_alg_type(const std::string &s) {
    if (s == "none")
      return COMP_ALG_NONE;
    if (s == "snappy")
      return COMP_ALG_SNAPPY;
    if (s == "zlib")
      return COMP_ALG_ZLIB;

    assert(0 == "invalid compression algorithm");
    return COMP_ALG_NONE;
  }

  vector<bluestore_pextent_t> extents;///< raw data position on device
  uint64_t sbid = 0;                  ///< shared blob id (if shared)
  uint32_t compressed_length_orig = 0;///< original length of compressed blob if any
  uint32_t compressed_length = 0;     ///< compressed length if any
  uint32_t flags = 0;                 ///< FLAG_*

  uint8_t csum_type = CSUM_NONE;      ///< CSUM_*
  uint8_t csum_chunk_order = 0;       ///< csum block size is 1<<block_order bytes

  bufferptr csum_data;                ///< opaque vector of csum data

  typedef uint16_t unused_uint_t;
  typedef std::bitset<sizeof(unused_uint_t) * 8> unused_t;
  unused_t unused;                    ///< portion that has never been written to

  bluestore_blob_t(uint32_t f = 0) : flags(f) {}

  int estimate_encoded_size() const {
    // conservative upper bound... fixme
    return csum_data.length() + extents.size() * 16 + 48;
  }

  bool can_split() const {
    return
      !has_flag(FLAG_SHARED) &&
      !has_flag(FLAG_COMPRESSED) &&
      !has_flag(FLAG_HAS_UNUSED);     // splitting unused set is complex
  }
  bool can_split_at(uint32_t blob_offset) const {
    if (has_csum() &&
	blob_offset % get_csum_chunk_size() != 0)
      return false;
    return true;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
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

  size_t get_csum_value_size() const {
    switch (csum_type) {
    case CSUM_NONE: return 0;
    case CSUM_XXHASH32: return 4;
    case CSUM_XXHASH64: return 8;
    case CSUM_CRC32C: return 4;
    case CSUM_CRC32C_16: return 2;
    case CSUM_CRC32C_8: return 1;
    default: return 0;
    }
  }
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
WRITE_CLASS_ENCODER(bluestore_blob_t)

ostream& operator<<(ostream& out, const bluestore_blob_t& o);


/// shared blob state
struct bluestore_shared_blob_t {
  bluestore_extent_ref_map_t ref_map;  ///< shared blob extents
  //set<ghobject_t> objects;  ///< objects referencing these shared blocks (debug)

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_shared_blob_t*>& ls);

  bool empty() const {
    return ref_map.empty();
  }
};
WRITE_CLASS_ENCODER(bluestore_shared_blob_t)

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
    void encode(bufferlist& bl) const {
      ::encode(offset, bl);
      ::encode(bytes, bl);
      ::encode(extents, bl);
    }
    void decode(bufferlist::iterator& p) {
      ::decode(offset, p);
      ::decode(bytes, p);
      ::decode(extents, p);
    }
    void dump(Formatter *f) const;
  };
  WRITE_CLASS_ENCODER(shard_info)
  vector<shard_info> extent_map_shards; ///< extent map shards (if any)

  uint32_t expected_object_size = 0;
  uint32_t expected_write_size = 0;
  uint32_t alloc_hint_flags = 0;

  /// get preferred csum chunk size
  size_t get_preferred_csum_order() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_onode_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_onode_t::shard_info)
WRITE_CLASS_ENCODER(bluestore_onode_t)

ostream& operator<<(ostream& out, const bluestore_onode_t::shard_info& si);

/// writeahead-logged op
struct bluestore_wal_op_t {
  typedef enum {
    OP_WRITE = 1,
  } type_t;
  __u8 op = 0;

  vector<bluestore_pextent_t> extents;
  bufferlist data;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_op_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_wal_op_t)


/// writeahead-logged transaction
struct bluestore_wal_transaction_t {
  uint64_t seq = 0;
  list<bluestore_wal_op_t> ops;
  interval_set<uint64_t> released;  ///< allocations to release after wal

  bluestore_wal_transaction_t() : seq(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_transaction_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_wal_transaction_t)

struct bluestore_compression_header_t {
  uint8_t type = bluestore_blob_t::COMP_ALG_NONE;
  uint32_t length = 0;

  bluestore_compression_header_t() {}
  bluestore_compression_header_t(uint8_t _type)
    : type(_type) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_compression_header_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_compression_header_t)


#endif
