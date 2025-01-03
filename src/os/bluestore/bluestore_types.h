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

#include <bit>
#include <limits>
#include <ostream>
#include <type_traits>
#include <vector>
#include <array>
#include "include/mempool.h"
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
#include "common/hobject.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"
#include "include/ceph_hash.h"

namespace ceph {
  class Formatter;
}

/// label for block device
struct bluestore_bdev_label_t {
  uuid_d osd_uuid;     ///< osd uuid
  uint64_t size = 0;   ///< device size
  utime_t btime;       ///< birth time
  std::string description;  ///< device description

  std::map<std::string,std::string> meta; ///< {read,write}_meta() content from ObjectStore

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_bdev_label_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_bdev_label_t)

std::ostream& operator<<(std::ostream& out, const bluestore_bdev_label_t& l);

/// collection metadata
struct bluestore_cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit bluestore_cnode_t(int b=0) : bits(b) {}

  DENC(bluestore_cnode_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.bits, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_cnode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_cnode_t)

std::ostream& operator<<(std::ostream& out, const bluestore_cnode_t& l);

template <typename OFFS_TYPE, typename LEN_TYPE>
struct bluestore_interval_t
{
  static constexpr uint64_t INVALID_OFFSET = ~0ull;

  OFFS_TYPE offset = 0;
  LEN_TYPE length = 0;

  bluestore_interval_t(){}
  bluestore_interval_t(uint64_t o, uint64_t l) : offset(o), length(l) {}

  bool is_valid() const {
    return offset != INVALID_OFFSET;
  }
  uint64_t end() const {
    return offset != INVALID_OFFSET ? offset + length : INVALID_OFFSET;
  }

  bool operator==(const bluestore_interval_t& other) const {
    return offset == other.offset && length == other.length;
  }

};

/// pextent: physical extent
struct bluestore_pextent_t : public bluestore_interval_t<uint64_t, uint32_t> 
{
  bluestore_pextent_t() {}
  bluestore_pextent_t(uint64_t o, uint64_t l) : bluestore_interval_t(o, l) {}
  bluestore_pextent_t(const bluestore_interval_t &ext) :
    bluestore_interval_t(ext.offset, ext.length) {}

  DENC(bluestore_pextent_t, v, p) {
    denc_lba(v.offset, p);
    denc_varint_lowz(v.length, p);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_pextent_t*>& ls);
};
WRITE_CLASS_DENC(bluestore_pextent_t)

std::ostream& operator<<(std::ostream& out, const bluestore_pextent_t& o);

typedef mempool::bluestore_cache_other::vector<bluestore_pextent_t> PExtentVector;

template<>
struct denc_traits<PExtentVector> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const PExtentVector& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const PExtentVector& v,
		     ceph::buffer::list::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(PExtentVector& v, ceph::buffer::ptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.resize(num);
    for (unsigned i=0; i<num; ++i) {
      denc(v[i], p);
    }
  }
};

/// extent_map: a std::map of reference counted extents
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;
    uint32_t refs;
    record_t(uint32_t l=0, uint32_t r=0) : length(l), refs(r) {}
    DENC(bluestore_extent_ref_map_t::record_t, v, p) {
      denc_varint_lowz(v.length, p);
      denc_varint(v.refs, p);
    }
    void dump(ceph::Formatter *f) const {
      f->dump_unsigned("length", length);
      f->dump_unsigned("refs", refs);
    }
    static void generate_test_instances(std::list<record_t*>& o) {
      o.push_back(new record_t);
      o.push_back(new record_t(123, 456));
    }
  };

  typedef mempool::bluestore_cache_other::map<uint64_t,record_t> map_t;
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
  void put(uint64_t offset, uint32_t len, PExtentVector *release,
	   bool *maybe_unshared);
  struct debug_len_cnt {
    uint32_t len; // length for which cnt is valid
    uint32_t cnt; // reference count for the region
  };
  debug_len_cnt debug_peek(uint64_t offset) const;

  bool contains(uint64_t offset, uint32_t len) const;
  bool intersects(uint64_t offset, uint32_t len) const;

  void bound_encode(size_t& p) const {
    denc_varint((uint32_t)0, p);
    if (!ref_map.empty()) {
      size_t elem_size = 0;
      denc_varint_lowz((uint64_t)0, elem_size);
      ref_map.begin()->second.bound_encode(elem_size);
      p += elem_size * ref_map.size();
    }
  }
  void encode(ceph::buffer::list::contiguous_appender& p) const {
    const uint32_t n = ref_map.size();
    denc_varint(n, p);
    if (n) {
      auto i = ref_map.begin();
      denc_varint_lowz(i->first, p);
      i->second.encode(p);
      int64_t pos = i->first;
      while (++i != ref_map.end()) {
	denc_varint_lowz((int64_t)i->first - pos, p);
	i->second.encode(p);
	pos = i->first;
      }
    }
  }
  void decode(ceph::buffer::ptr::const_iterator& p) {
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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_extent_ref_map_t*>& o);
};
WRITE_CLASS_DENC(bluestore_extent_ref_map_t)
WRITE_CLASS_DENC(bluestore_extent_ref_map_t::record_t)

std::ostream& operator<<(std::ostream& out, const bluestore_extent_ref_map_t& rm);
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

/// blob_use_tracker: a set of per-alloc unit ref buckets to track blob usage
struct bluestore_blob_use_tracker_t {
  // N.B.: There is no need to minimize au_size/num_au
  //   as much as possible (e.g. have just a single byte for au_size) since:
  //   1) Struct isn't packed hence it's padded. And even if it's packed see 2)
  //   2) Mem manager has its own granularity, most probably >= 8 bytes
  //
  uint32_t au_size;  // Allocation (=tracking) unit size,
                     // == 0 if uninitialized
  uint32_t num_au;   // Amount of allocation units tracked
                     // == 0 if single unit or the whole blob is tracked
  uint32_t alloc_au; // Amount of allocation units allocated
                       
  union {
    uint32_t* bytes_per_au;
    uint32_t total_bytes;
  };
  
  bluestore_blob_use_tracker_t()
    : au_size(0), num_au(0), alloc_au(0), bytes_per_au(nullptr) {
  }
  bluestore_blob_use_tracker_t(const bluestore_blob_use_tracker_t& tracker);
  bluestore_blob_use_tracker_t& operator=(const bluestore_blob_use_tracker_t& rhs);
  ~bluestore_blob_use_tracker_t() {
    clear();
  }

  void clear() {
    release(alloc_au, bytes_per_au);
    num_au = 0;
    alloc_au = 0;
    bytes_per_au = 0;
    au_size = 0;
  }

  uint32_t get_referenced_bytes() const {
    uint32_t total = 0;
    if (!num_au) {
      total = total_bytes;
    } else {
      for (size_t i = 0; i < num_au; ++i) {
	total += bytes_per_au[i];
      }
    }
    return total;
  }
  bool is_not_empty() const {
    if (!num_au) {
      return total_bytes != 0;
    } else {
      for (size_t i = 0; i < num_au; ++i) {
	if (bytes_per_au[i]) {
	  return true;
	}
      }
    }
    return false;
  }
  bool is_empty() const {
    return !is_not_empty();
  }
  // Returns how many allocation units are currently tracked.
  // Simplifies logic when num_au = 0, but in reality we track just one
  uint32_t get_num_au() const {
    return num_au == 0 ? 1 : num_au;
  }
  // Returns array of used sizes per au.
  // It has at least get_num_au() elements.
  const uint32_t* get_au_array() const {
    if (num_au > 0) {
      return bytes_per_au;
    } else {
      return &total_bytes;
    }
  }
  // Returns array of used sizes per au.
  // It has at least get_num_au() elements.
  uint32_t* dirty_au_array() {
    if (num_au > 0) {
      return bytes_per_au;
    } else {
      return &total_bytes;
    }
  }
  void prune_tail(uint32_t new_len) {
    if (num_au) {
      new_len = round_up_to(new_len, au_size);
      uint32_t _num_au = new_len / au_size;
      ceph_assert(_num_au <= num_au);
      if (_num_au) {
        num_au = _num_au; // bytes_per_au array is left unmodified
      } else {
        clear();
      }
    }
  }
  void add_tail(uint32_t new_len, uint32_t _au_size) {
    auto full_size = au_size * (num_au ? num_au : 1);
    ceph_assert(new_len >= full_size);
    if (new_len == full_size) {
      return;
    }
    if (!num_au) {
      uint32_t old_total = total_bytes;
      total_bytes = 0;
      init(new_len, _au_size);
      ceph_assert(num_au);
      bytes_per_au[0] = old_total;
    } else {
      ceph_assert(_au_size == au_size);
      new_len = round_up_to(new_len, au_size);
      uint32_t _num_au = new_len / au_size;
      ceph_assert(_num_au >= num_au);
      if (_num_au > num_au) {
	auto old_bytes = bytes_per_au;
	auto old_num_au = num_au;
	auto old_alloc_au = alloc_au;
	alloc_au = num_au = 0; // to bypass an assertion in allocate()
	bytes_per_au = nullptr;
	allocate(_num_au);
	for (size_t i = 0; i < old_num_au; i++) {
	  bytes_per_au[i] = old_bytes[i];
	}
	for (size_t i = old_num_au; i < num_au; i++) {
	  bytes_per_au[i] = 0;
	}
	release(old_alloc_au, old_bytes);
      }
    }
  }

  void init(
    uint32_t full_length,
    uint32_t _au_size);

  inline void init_and_ref(
    uint32_t full_length,
    uint32_t tracked_chunk);

  void get(
    uint32_t offset,
    uint32_t len);

  /// put: return true if the blob has no references any more after the call,
  /// no release_units is filled for the sake of performance.
  /// return false if there are some references to the blob,
  /// in this case release_units contains pextents
  /// (identified by their offsets relative to the blob start)
  ///  that are not used any more and can be safely deallocated.
  bool put(
    uint32_t offset,
    uint32_t len,
    PExtentVector *release);

  /// Puts back references in region [offset~length].
  /// It is different, simpler version of put,
  /// as it does not allow for overprovisioning.
  /// Releasing off=0x500 len=0x2000 from {0x1000,0x1004,0x1000} will fail,
  /// while the other one behaves properly
  std::pair<uint32_t, uint32_t> put_simple(
    uint32_t offset,
    uint32_t length);

  bool can_split() const;
  bool can_split_at(uint32_t blob_offset) const;
  void split(
    uint32_t blob_offset,
    bluestore_blob_use_tracker_t* r);
  void dup(const bluestore_blob_use_tracker_t& from,
	   uint32_t start, uint32_t len);
  bool equal(
    const bluestore_blob_use_tracker_t& other) const;
    
  void bound_encode(size_t& p) const {
    denc_varint(au_size, p);
    if (au_size) {
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        size_t elem_size = 0;
        denc_varint((uint32_t)0, elem_size);
        p += elem_size * num_au;
      }
    }
  }
  void encode(ceph::buffer::list::contiguous_appender& p) const {
    denc_varint(au_size, p);
    if (au_size) {
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        size_t elem_size = 0;
        denc_varint((uint32_t)0, elem_size);
        for (size_t i = 0; i < num_au; ++i) {
          denc_varint(bytes_per_au[i], p);
        }
      }
    }
  }
  void decode(ceph::buffer::ptr::const_iterator& p) {
    clear();
    denc_varint(au_size, p);
    if (au_size) {
      uint32_t _num_au;
      denc_varint(_num_au, p);
      if (!_num_au) {
        num_au = 0;
        denc_varint(total_bytes, p);
      } else {
        allocate(_num_au);
        for (size_t i = 0; i < _num_au; ++i) {
	  denc_varint(bytes_per_au[i], p);
        }
      }
    }
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_blob_use_tracker_t*>& o);
private:
  void allocate(uint32_t _num_au);
  void release(uint32_t _num_au, uint32_t* ptr);
};
WRITE_CLASS_DENC(bluestore_blob_use_tracker_t)
std::ostream& operator<<(std::ostream& out, const bluestore_blob_use_tracker_t& rm);

/// blob: a piece of data on disk
struct bluestore_blob_t {
private:
  PExtentVector extents;              ///< raw data position on device
  uint32_t logical_length = 0;        ///< original length of data stored in the blob
  uint32_t compressed_length = 0;     ///< compressed length if any

public:
  enum {
    LEGACY_FLAG_MUTABLE = 1,  ///< [legacy] blob can be overwritten or split
    FLAG_COMPRESSED = 2,      ///< blob is compressed
    FLAG_CSUM = 4,            ///< blob has checksums
    FLAG_HAS_UNUSED = 8,      ///< blob has unused std::map
    FLAG_SHARED = 16,         ///< blob is shared; see external SharedBlob
  };
  static std::string get_flags_string(unsigned flags);

  uint32_t flags = 0;                 ///< FLAG_*

  typedef uint16_t unused_t;
  unused_t unused = 0;     ///< portion that has never been written to (bitmap)

  uint8_t csum_type = Checksummer::CSUM_NONE;      ///< CSUM_*
  uint8_t csum_chunk_order = 0;       ///< csum block size is 1<<block_order bytes

  ceph::buffer::ptr csum_data;                ///< opaque std::vector of csum data

  bluestore_blob_t(uint32_t f = 0) : flags(f) {}

  void dup(const bluestore_blob_t& from);

  // initialize blob to accomodate data from other blob, but do not copy yet
  void adjust_to(const bluestore_blob_t& other, uint32_t new_logical_length);

  const PExtentVector& get_extents() const {
    return extents;
  }
  PExtentVector& dirty_extents() {
    return extents;
  }

  DENC_HELPERS;
  void bound_encode(size_t& p, uint64_t struct_v) const {
    ceph_assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    denc_varint_lowz(logical_length, p);
    denc_varint_lowz(compressed_length, p);
    denc(csum_type, p);
    denc(csum_chunk_order, p);
    denc_varint(csum_data.length(), p);
    p += csum_data.length();
    p += sizeof(unused_t);
  }

  void encode(ceph::buffer::list::contiguous_appender& p, uint64_t struct_v) const {
    ceph_assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_compressed()) {
      denc_varint_lowz(logical_length, p);
      denc_varint_lowz(compressed_length, p);
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      denc_varint(csum_data.length(), p);
      memcpy(p.get_pos_add(csum_data.length()), csum_data.c_str(),
	     csum_data.length());
    }
    if (has_unused()) {
      denc(unused, p);
    }
  }

  void decode(ceph::buffer::ptr::const_iterator& p, uint64_t struct_v) {
    ceph_assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_compressed()) {
      denc_varint_lowz(logical_length, p);
      denc_varint_lowz(compressed_length, p);
    } else {
      logical_length = get_ondisk_length();
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      int len;
      denc_varint(len, p);
      csum_data = p.get_ptr(len);
      csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
    }
    if (has_unused()) {
      denc(unused, p);
    }
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

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_blob_t*>& ls);

  bool has_flag(unsigned f) const {
    return flags & f;
  }
  void set_flag(unsigned f) {
    flags |= f;
  }
  void clear_flag(unsigned f) {
    flags &= ~f;
  }
  std::string get_flags_string() const {
    return get_flags_string(flags);
  }

  void set_compressed(uint64_t clen_orig, uint64_t clen) {
    set_flag(FLAG_COMPRESSED);
    logical_length = clen_orig;
    compressed_length = clen;
  }
  bool is_mutable() const {
    return !is_compressed() && !is_shared();
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
  bool has_disk() const {
    return extents.size() > 1 || extents.begin()->is_valid();
  }
  /// return chunk (i.e. min readable block) size for the blob
  uint64_t get_chunk_size(uint64_t dev_block_size) const {
    return has_csum() ?
      std::max<uint64_t>(dev_block_size, get_csum_chunk_size()) : dev_block_size;
  }
  uint32_t get_csum_chunk_size() const {
    return 1 << csum_chunk_order;
  }
  uint32_t get_compressed_payload_length() const {
    return is_compressed() ? compressed_length : 0;
  }
  uint64_t calc_offset(uint64_t x_off, uint64_t *plen) const {
    auto p = extents.begin();
    ceph_assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      ceph_assert(p != extents.end());
    }
    if (plen)
      *plen = p->length - x_off;
    return p->offset + x_off;
  }

  // validate whether or not the status of pextents within the given range
  // meets the requirement(allocated or unallocated).
  bool _validate_range(uint64_t b_off, uint64_t b_len,
                       bool require_allocated) const {
    auto p = extents.begin();
    ceph_assert(p != extents.end());
    while (b_off >= p->length) {
      b_off -= p->length;
      if (++p == extents.end())
        return false;
    }
    b_len += b_off;
    while (b_len) {
      if (require_allocated != p->is_valid()) {
        return false;
      }
      if (p->length >= b_len) {
        return true;
      }
      b_len -= p->length;
      if (++p == extents.end())
        return false;
    }
    ceph_abort_msg("we should not get here");
    return false;
  }

  /// return true if the entire range is allocated
  /// (mapped to extents on disk)
  bool is_allocated(uint64_t b_off, uint64_t b_len) const {
    return _validate_range(b_off, b_len, true);
  }

  /// return true if the entire range is unallocated
  /// (not mapped to extents on disk)
  bool is_unallocated(uint64_t b_off, uint64_t b_len) const {
    return _validate_range(b_off, b_len, false);
  }

  /// return true if the logical range has never been used
  bool is_unused(uint64_t offset, uint64_t length) const {
    if (!has_unused()) {
      return false;
    }
    ceph_assert(!is_compressed());
    uint64_t blob_len = get_logical_length();
    ceph_assert((blob_len % (sizeof(unused)*8)) == 0);
    ceph_assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / (sizeof(unused)*8);
    uint64_t start = offset / chunk_size;
    uint64_t end = round_up_to(offset + length, chunk_size) / chunk_size;
    auto i = start;
    while (i < end && (unused & (1u << i))) {
      i++;
    }
    return i >= end;
  }

  /// mark a range that has never been used
  void add_unused(uint64_t offset, uint64_t length) {
    ceph_assert(!is_compressed());
    uint64_t blob_len = get_logical_length();
    ceph_assert((blob_len % (sizeof(unused)*8)) == 0);
    ceph_assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / (sizeof(unused)*8);
    uint64_t start = round_up_to(offset, chunk_size) / chunk_size;
    uint64_t end = (offset + length) / chunk_size;
    for (auto i = start; i < end; ++i) {
      unused |= (1u << i);
    }
    if (start != end) {
      set_flag(FLAG_HAS_UNUSED);
    }
  }

  /// indicate that a range has (now) been used.
  void mark_used(uint64_t offset, uint64_t length) {
    if (has_unused()) {
      ceph_assert(!is_compressed());
      uint64_t blob_len = get_logical_length();
      ceph_assert((blob_len % (sizeof(unused)*8)) == 0);
      ceph_assert(offset + length <= blob_len);
      uint64_t chunk_size = blob_len / (sizeof(unused)*8);
      uint64_t start = offset / chunk_size;
      uint64_t end = round_up_to(offset + length, chunk_size) / chunk_size;
      for (auto i = start; i < end; ++i) {
        unused &= ~(1u << i);
      }
      if (unused == 0) {
        clear_flag(FLAG_HAS_UNUSED);
      }
    }
  }
  /// todo implement me!
  unused_t get_unused_mask(uint32_t offset, uint32_t length, uint32_t chunk_size) {
    if (has_unused()) {
      return 0;
    } else {
      return 0;
    }
  }
  // map_f_invoke templates intended to mask parameters which are not expected
  // by the provided callback
  template<class F, typename std::enable_if<std::is_invocable_r_v<
    int,
    F,
    uint64_t,
    uint64_t>>::type* = nullptr>
  int map_f_invoke(uint64_t lo,
    const bluestore_pextent_t& p,
    uint64_t o,
    uint64_t l, F&& f) const{
    return f(o, l);
  }

  template<class F, typename std::enable_if<std::is_invocable_r_v<
    int,
    F,
    uint64_t,
    uint64_t,
    uint64_t>>::type * = nullptr>
  int map_f_invoke(uint64_t lo,
    const bluestore_pextent_t& p,
    uint64_t o,
    uint64_t l, F&& f) const {
    return f(lo, o, l);
  }

  template<class F, typename std::enable_if<std::is_invocable_r_v<
    int,
    F,
    const bluestore_pextent_t&,
    uint64_t,
    uint64_t>>::type * = nullptr>
    int map_f_invoke(uint64_t lo,
      const bluestore_pextent_t& p,
      uint64_t o,
      uint64_t l, F&& f) const {
    return f(p, o, l);
  }

  template<class F>
  int map(uint64_t x_off, uint64_t x_len, F&& f) const {
    auto x_off0 = x_off;
    auto p = extents.begin();
    ceph_assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      ceph_assert(p != extents.end());
    }
    while (x_len > 0 && p != extents.end()) {
      uint64_t l = std::min(p->length - x_off, x_len);
      int r = map_f_invoke(x_off0, *p, p->offset + x_off, l, f);
      if (r < 0)
        return r;
      x_off = 0;
      x_len -= l;
      x_off0 += l;
      ++p;
    }
    return 0;
  }

  template<class F>
  void map_bl(uint64_t x_off,
	      ceph::buffer::list& bl,
	      F&& f) const {
    static_assert(std::is_invocable_v<F, uint64_t, ceph::buffer::list&>);

    auto p = extents.begin();
    ceph_assert(p != extents.end());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      ceph_assert(p != extents.end());
    }
    ceph::buffer::list::iterator it = bl.begin();
    uint64_t x_len = bl.length();
    while (x_len > 0) {
      ceph_assert(p != extents.end());
      uint64_t l = std::min(p->length - x_off, x_len);
      ceph::buffer::list t;
      it.copy(l, t);
      f(p->offset + x_off, t);
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
    return logical_length;
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
      ceph_abort_msg("no csum data, bad index");
    case 1:
      return reinterpret_cast<const uint8_t*>(p)[i];
    case 2:
      return reinterpret_cast<const ceph_le16*>(p)[i];
    case 4:
      return reinterpret_cast<const ceph_le32*>(p)[i];
    case 8:
      return reinterpret_cast<const ceph_le64*>(p)[i];
    default:
      ceph_abort_msg("unrecognized csum word size");
    }
  }
  void set_csum_item(unsigned i, uint64_t val)  {
    size_t cs = get_csum_value_size();
    char *p = csum_data.c_str();
    switch (cs) {
    case 0:
      ceph_abort_msg("no csum data, bad index");
    case 1:
      reinterpret_cast<uint8_t*>(p)[i] = val;
      break;
    case 2:
      reinterpret_cast<ceph_le16*>(p)[i] = val;
      break;
    case 4:
      reinterpret_cast<ceph_le32*>(p)[i] = val;
      break;
    case 8:
      reinterpret_cast<ceph_le64*>(p)[i] = val;
      break;
    default:
      ceph_abort_msg("unrecognized csum word size");
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
    csum_data = ceph::buffer::create(get_csum_value_size() * len / get_csum_chunk_size());
    csum_data.zero();
    csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
  }

  /// calculate csum for the buffer at the given b_off
  void calc_csum(uint64_t b_off, const ceph::buffer::list& bl);

  /// verify csum: return -EOPNOTSUPP for unsupported checksum type;
  /// return -1 and valid(nonnegative) b_bad_off for checksum error;
  /// return 0 if all is well.
  int verify_csum(uint64_t b_off, const ceph::buffer::list& bl, int* b_bad_off,
		  uint64_t *bad_csum) const;

  bool can_prune_tail() const {
    return
      !is_shared() &&
      !is_compressed() &&
      extents.size() > 1 &&  // if it's all invalid it's not pruning.
      !extents.back().is_valid() &&
      !has_unused();
  }
  void prune_tail() {
    const auto &p = extents.back();
    logical_length -= p.length;
    extents.pop_back();
    if (has_csum()) {
      ceph::buffer::ptr t;
      t.swap(csum_data);
      csum_data = ceph::buffer::ptr(t.c_str(),
			    get_logical_length() / get_csum_chunk_size() *
			    get_csum_value_size());
      csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
    }
  }
  void add_tail(uint32_t new_len) {
    ceph_assert(!has_unused());
    ceph_assert(new_len > logical_length);
    extents.emplace_back(
      bluestore_pextent_t(
        bluestore_pextent_t::INVALID_OFFSET,
        new_len - logical_length));
    logical_length = new_len;
    if (has_csum()) {
      ceph::buffer::ptr t;
      t.swap(csum_data);
      csum_data = ceph::buffer::create(
	get_csum_value_size() * logical_length / get_csum_chunk_size());
      csum_data.copy_in(0, t.length(), t.c_str());
      csum_data.zero(t.length(), csum_data.length() - t.length());
      csum_data.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
    }
  }
  uint32_t get_release_size(uint32_t min_alloc_size) const {
    if (is_compressed()) {
      return get_logical_length();
    }
    uint32_t res = get_csum_chunk_size();
    if (!has_csum() || res < min_alloc_size) {
      res = min_alloc_size;
    }
    return res;
  }

  void split(uint32_t blob_offset, bluestore_blob_t& rb);
  void allocated(uint32_t b_off, uint32_t length, const PExtentVector& allocs);
  void allocated_full(uint32_t length, PExtentVector&& allocs);
  void allocated_test(const bluestore_pextent_t& alloc); // intended for UT only
  static constexpr uint64_t NO_ALLOCATION = std::numeric_limits<uint64_t>::max();
  uint64_t get_allocation_at(uint32_t in_blob_offset) {
    uint32_t loc = in_blob_offset;
    for (auto e : extents) {
      if (loc < e.length) {
        //ceph_assert(e.is_valid());
        if (e.is_valid()) {
          return e.offset + loc;
        } else {
          return NO_ALLOCATION;
        }
      }
      loc -= e.length;
    }
    ceph_assert(false);
  };

  /// updates blob's pextents container and return unused pextents eligible
  /// for release.
  /// all - indicates that the whole blob to be released.
  /// logical - specifies set of logical extents within blob's
  /// to be released
  /// Returns true if blob has no more valid pextents
  bool release_extents(
    bool all,
    const PExtentVector& logical,
    PExtentVector* r);

  /// Remove blob's pextents.
  /// [offset~length] - range to remove, in local blob space
  /// released_disk   - a vector of disk allocation units that are no longer in use;
  ///                   appends to it
  /// returns:
  ///   size of released disk
  uint32_t release_extents(
    uint32_t offset,
    uint32_t length,
    PExtentVector* released_disk
  );
};
WRITE_CLASS_DENC_FEATURED(bluestore_blob_t)

std::ostream& operator<<(std::ostream& out, const bluestore_blob_t& o);


/// shared blob state
struct bluestore_shared_blob_t {
  MEMPOOL_CLASS_HELPERS();
  uint64_t sbid;                       ///> shared blob id
  bluestore_extent_ref_map_t ref_map;  ///< shared blob extents

  bluestore_shared_blob_t() : sbid(0) {}
  bluestore_shared_blob_t(uint64_t _sbid) : sbid(_sbid) {}
  bluestore_shared_blob_t(uint64_t _sbid,
			  bluestore_extent_ref_map_t&& _ref_map ) 
    : sbid(_sbid), ref_map(std::move(_ref_map)) {}

  DENC(bluestore_shared_blob_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.ref_map, p);
    DENC_FINISH(p);
  }


  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_shared_blob_t*>& ls);

  bool empty() const {
    return ref_map.empty();
  }
};
WRITE_CLASS_DENC(bluestore_shared_blob_t)

std::ostream& operator<<(std::ostream& out, const bluestore_shared_blob_t& o);

/// onode: per-object metadata
struct bluestore_onode_t {
  uint64_t nid = 0;                    ///< numeric id (locally unique)
  uint64_t size = 0;                   ///< object size
  // mempool to be assigned to buffer::ptr manually
  std::map<mempool::bluestore_cache_meta::string, ceph::buffer::ptr> attrs;

  struct shard_info {
    uint32_t offset = 0;  ///< logical offset for start of shard
    uint32_t bytes = 0;   ///< encoded bytes
    DENC(shard_info, v, p) {
      denc_varint(v.offset, p);
      denc_varint(v.bytes, p);
    }
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<shard_info*>& ls);
  };
  std::vector<shard_info> extent_map_shards; ///< extent std::map shards (if any)

  uint32_t expected_object_size = 0;
  uint32_t expected_write_size = 0;
  uint32_t alloc_hint_flags = 0;

  uint8_t flags = 0;

  std::map<uint32_t, uint64_t> zone_offset_refs;  ///< (zone, offset) refs to this onode

  enum {
    FLAG_OMAP = 1,         ///< object may have omap data
    FLAG_PGMETA_OMAP = 2,  ///< omap data is in meta omap prefix
    FLAG_PERPOOL_OMAP = 4, ///< omap data is in per-pool prefix; per-pool keys
    FLAG_PERPG_OMAP = 8,   ///< omap data is in per-pg prefix; per-pg keys
  };

  std::string get_flags_string() const {
    std::string s;
    if (flags & FLAG_OMAP) {
      s = "omap";
    }
    if (flags & FLAG_PGMETA_OMAP) {
      s += "+pgmeta_omap";
    }
    if (flags & FLAG_PERPOOL_OMAP) {
      s += "+per_pool_omap";
    }
    if (flags & FLAG_PERPG_OMAP) {
      s += "+per_pg_omap";
    }
    return s;
  }

  bool has_flag(unsigned f) const {
    return flags & f;
  }

  void set_flag(unsigned f) {
    flags |= f;
  }

  void clear_flag(unsigned f) {
    flags &= ~f;
  }

  bool has_omap() const {
    return has_flag(FLAG_OMAP);
  }

  static bool is_pgmeta_omap(uint8_t flags) {
    return flags & FLAG_PGMETA_OMAP;
  }
  static bool is_perpool_omap(uint8_t flags) {
    return flags & FLAG_PERPOOL_OMAP;
  }
  static bool is_perpg_omap(uint8_t flags) {
    return flags & FLAG_PERPG_OMAP;
  }
  bool is_pgmeta_omap() const {
    return has_flag(FLAG_PGMETA_OMAP);
  }
  bool is_perpool_omap() const {
    return has_flag(FLAG_PERPOOL_OMAP);
  }
  bool is_perpg_omap() const {
    return has_flag(FLAG_PERPG_OMAP);
  }

  void set_omap_flags(bool legacy) {
    set_flag(FLAG_OMAP | (legacy ? 0 : (FLAG_PERPOOL_OMAP | FLAG_PERPG_OMAP)));
  }
  void set_omap_flags_pgmeta() {
    set_flag(FLAG_OMAP | FLAG_PGMETA_OMAP);
  }

  void clear_omap_flag() {
    clear_flag(FLAG_OMAP |
	       FLAG_PGMETA_OMAP |
	       FLAG_PERPOOL_OMAP |
	       FLAG_PERPG_OMAP);
  }

  DENC(bluestore_onode_t, v, p) {
    DENC_START(2, 1, p);
    denc_varint(v.nid, p);
    denc_varint(v.size, p);
    denc(v.attrs, p);
    denc(v.flags, p);
    denc(v.extent_map_shards, p);
    denc_varint(v.expected_object_size, p);
    denc_varint(v.expected_write_size, p);
    denc_varint(v.alloc_hint_flags, p);
    if (struct_v >= 2) {
      denc(v.zone_offset_refs, p);
    }
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_onode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_onode_t::shard_info)
WRITE_CLASS_DENC(bluestore_onode_t)

std::ostream& operator<<(std::ostream& out, const bluestore_onode_t::shard_info& si);

/// writeahead-logged op
struct bluestore_deferred_op_t {
  typedef enum {
    OP_WRITE = 1,
  } type_t;
  __u8 op = 0;

  PExtentVector extents;
  ceph::buffer::list data;

  DENC(bluestore_deferred_op_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.extents, p);
    denc(v.data, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_deferred_op_t*>& o);
};
WRITE_CLASS_DENC(bluestore_deferred_op_t)


/// writeahead-logged transaction
struct bluestore_deferred_transaction_t {
  uint64_t seq = 0;
  std::list<bluestore_deferred_op_t> ops;
  interval_set<uint64_t> released;  ///< allocations to release after tx

  bluestore_deferred_transaction_t() : seq(0) {}

  DENC(bluestore_deferred_transaction_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seq, p);
    denc(v.ops, p);
    denc(v.released, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_deferred_transaction_t*>& o);
};
WRITE_CLASS_DENC(bluestore_deferred_transaction_t)

struct bluestore_compression_header_t {
  uint8_t type = Compressor::COMP_ALG_NONE;
  uint32_t length = 0;
  std::optional<int32_t> compressor_message;

  bluestore_compression_header_t() {}
  bluestore_compression_header_t(uint8_t _type)
    : type(_type) {}

  DENC(bluestore_compression_header_t, v, p) {
    DENC_START(2, 1, p);
    denc(v.type, p);
    denc(v.length, p);
    if (struct_v >= 2) {
      denc(v.compressor_message, p);
    }
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_compression_header_t*>& o);
};
WRITE_CLASS_DENC(bluestore_compression_header_t)

template <template <typename> typename V, class COUNTER_TYPE = int32_t>
class ref_counter_2hash_tracker_t {
  size_t num_non_zero = 0;
  size_t num_buckets = 0;
  V<COUNTER_TYPE> buckets1;
  V<COUNTER_TYPE> buckets2;

public:
  ref_counter_2hash_tracker_t(uint64_t mem_cap) {
    num_buckets = mem_cap / sizeof(COUNTER_TYPE) / 2;
    ceph_assert(num_buckets);
    buckets1.resize(num_buckets);
    buckets2.resize(num_buckets);
    reset();
  }

  size_t get_num_buckets() const {
    return num_buckets;
  }

  void inc(const char* hash_val, size_t hash_val_len, int n) {
    auto h = ceph_str_hash_rjenkins((const char*)hash_val, hash_val_len) %
      num_buckets;
    if (buckets1[h] == 0 && n) {
      ++num_non_zero;
    } else if (buckets1[h] == -n) {
      --num_non_zero;
    }
    buckets1[h] += n;
    h = ceph_str_hash_linux((const char*)hash_val, hash_val_len) % num_buckets;
    if (buckets2[h] == 0 && n) {
      ++num_non_zero;
    } else if (buckets2[h] == -n) {
      --num_non_zero;
    }
    buckets2[h] += n;
  }

  bool test_hash_conflict(
    const char* hash_val1,
    const char* hash_val2,
    size_t hash_val_len) const {

    auto h1 = ceph_str_hash_rjenkins((const char*)hash_val1, hash_val_len);
    auto h2 = ceph_str_hash_rjenkins((const char*)hash_val2, hash_val_len);
    auto h3 = ceph_str_hash_linux((const char*)hash_val1, hash_val_len);
    auto h4 = ceph_str_hash_linux((const char*)hash_val2, hash_val_len);
    return ((h1 % num_buckets) == (h2 % num_buckets)) &&
      ((h3 % num_buckets) == (h4 % num_buckets));
  }

  bool test_all_zero(const char* hash_val, size_t hash_val_len) const {
    auto h = ceph_str_hash_rjenkins((const char*)hash_val, hash_val_len);
    if (buckets1[h % num_buckets] != 0) {
      return false;
    }
    h = ceph_str_hash_linux((const char*)hash_val, hash_val_len);
    return buckets2[h % num_buckets] == 0;
  }

  // returns number of mismatching buckets
  size_t count_non_zero() const {
    return num_non_zero;
  }
  void reset() {
    for (size_t i = 0; i < num_buckets; i++) {
      buckets1[i] = 0;
      buckets2[i] = 0;
    }
    num_non_zero = 0;
  }
};

class shared_blob_2hash_tracker_t
  : public ref_counter_2hash_tracker_t<mempool::bluestore_fsck::vector> {

  static const size_t hash_input_len = 3;

  typedef std::array<uint64_t, hash_input_len> hash_input_t;

  static size_t get_hash_input_size() {
    return hash_input_len * sizeof(hash_input_t::value_type);
  }

  inline hash_input_t build_hash_input(uint64_t sbid, uint64_t offset) const;

  size_t au_void_bits = 0;


public:
  shared_blob_2hash_tracker_t(uint64_t mem_cap, size_t alloc_unit)
    : ref_counter_2hash_tracker_t(mem_cap) {
    ceph_assert(alloc_unit);
    ceph_assert(std::has_single_bit(alloc_unit));
    au_void_bits = std::countr_zero(alloc_unit);
  }
  void inc(uint64_t sbid, uint64_t offset, int n);
  void inc_range(uint64_t sbid, uint64_t offset, uint32_t len, int n);

  bool test_hash_conflict(
    uint64_t sbid,
    uint64_t offset,
    uint64_t sbid2,
    uint64_t offset2) const;
  bool test_all_zero(
    uint64_t sbid,
    uint64_t offset) const;
  bool test_all_zero_range(
    uint64_t sbid,
    uint64_t offset,
    uint32_t len) const;
};

class sb_info_t {
  // subzero value indicates (potentially) stray blob,
  // i.e. blob that has got no real references from onodes
  int64_t sbid = 0;

public:
  enum {
    INVALID_POOL_ID = INT64_MIN
  };

  int64_t pool_id = INVALID_POOL_ID;
  // subzero value indicates compressed_allocated as well
  int32_t allocated_chunks = 0;

  sb_info_t(int64_t _sbid = 0) : sbid(_sbid)
  {
  }
  bool operator< (const sb_info_t& other) const {
    return std::abs(sbid) < std::abs(other.sbid);
  }
  bool operator< (const uint64_t& other_sbid) const {
    return uint64_t(std::abs(sbid)) < other_sbid;
  }
  bool is_stray() const {
    return sbid < 0;
  }
  uint64_t get_sbid() const {
    return uint64_t(std::abs(sbid));
  }
  void adopt() {
    sbid = std::abs(sbid);
  }
} __attribute__((packed));

// Space-efficient container to keep a set of sb_info structures
// given that the majority of entries are appended in a proper id-sorted
// order. Hence one can keep them in a regular vector and apply binary search
// whenever specific entry to be found.
// For the rare occasions when out-of-order append takes place - an auxilliary
// regular map is used.
struct sb_info_space_efficient_map_t {
  // large array sorted by the user
  mempool::bluestore_fsck::vector<sb_info_t> items;
  // small additional set of items we maintain sorting ourselves
  // this would never keep an entry with id > items.back().id
  mempool::bluestore_fsck::vector<sb_info_t> aux_items;

  sb_info_t& add_maybe_stray(uint64_t sbid) {
    return _add(-int64_t(sbid));
  }
  sb_info_t& add_or_adopt(uint64_t sbid) {
    auto& r = _add(sbid);
    r.adopt();
    return r;
  }
  auto find(uint64_t id) {
    if (items.size() != 0) {
      auto it = std::lower_bound(
	items.begin(),
	items.end() - 1,
	id,
	[](const sb_info_t& a, const uint64_t& b) {
	  return a < b;
	});
      if (it->get_sbid() == id) {
	return it;
      }
      if (aux_items.size() != 0) {
	auto it = std::lower_bound(
	  aux_items.begin(),
	  aux_items.end() - 1,
	  id,
	  [](const sb_info_t& a, const uint64_t& b) {
	    return a < b;
	  });
        if (it->get_sbid() == id) {
	  return it;
	}
      }
    }
    return items.end();
  }
  // enumerates strays, order isn't guaranteed.
  void foreach_stray(std::function<void(const sb_info_t&)> cb) {
    for (auto& sbi : items) {
      if (sbi.is_stray()) {
	cb(sbi);
      }
    }
    for (auto& sbi : aux_items) {
      if (sbi.is_stray()) {
	cb(sbi);
      }
    }
  }
  auto end() {
    return items.end();
  }

  void shrink() {
    items.shrink_to_fit();
    aux_items.shrink_to_fit();
  }
  void clear() {
    items.clear();
    aux_items.clear();
    shrink();
  }
private:
  sb_info_t& _add(int64_t id) {
    uint64_t n_id = uint64_t(std::abs(id));
    if (items.size() == 0 || n_id > items.back().get_sbid()) {
      return items.emplace_back(id);
    }
    auto it = find(n_id);
    if (it != items.end()) {
      return *it;
    }
    if (aux_items.size() == 0 || n_id > aux_items.back().get_sbid()) {
      return aux_items.emplace_back(id);
    }
    // do sorted insertion, may be expensive!
    it = std::upper_bound(
      aux_items.begin(),
      aux_items.end(),
      n_id,
      [](const uint64_t& a, const sb_info_t& b) {
	return a < b.get_sbid();
      });
    return *aux_items.emplace(it, id);
  }
};

#endif
