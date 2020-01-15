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
#include <type_traits>
#include "include/mempool.h"
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
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

static const uint64_t INVALID_OFFSET = ~0ull;

template <typename OFFS_TYPE, typename LEN_TYPE>
struct bluestore_interval_t
{
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

static const size_t MAX_SQUEEZED = sizeof(PExtentVector) / sizeof(uint64_t);
static const uint64_t MIN_ALLOC_SIZE_ORDER = 12; // 4096
static const uint64_t MIN_ALLOC_SIZE_MASK = (1ull << MIN_ALLOC_SIZE_ORDER) - 1;
static const size_t MAX_SQUEEZED_LEN = 1ull << (2 * MIN_ALLOC_SIZE_ORDER);
static const uint64_t INVALID_OFFSET_SQUEEZED =
  INVALID_OFFSET & ~MIN_ALLOC_SIZE_MASK;
typedef std::array<uint64_t, MAX_SQUEEZED> PExtentArray;

extern size_t get_squeezed_size(const PExtentArray& a);
extern uint64_t squeeze_extent(const bluestore_pextent_t& e);
extern bluestore_pextent_t unsqueeze_extent(uint64_t v);

// IMPORTANT!!
// PExtentVector and PExtentArray encoding should be compatible
// (given array size is enough to fit all the elements)
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

template<>
struct denc_traits<PExtentArray> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const PExtentArray& v, size_t& p) {
    p += sizeof(uint32_t);
    auto size = get_squeezed_size(v);
    if (size) {
      size_t per = 0;
      bluestore_pextent_t dummy;
      denc(dummy, per);
      p += per * size;
    }
  }
  static void encode(const PExtentArray& v,
    bufferlist::contiguous_appender& p) {

    // cut off tailing invalid extents
    size_t size = get_squeezed_size(v);
    denc_varint(size, p);
    size_t pos = 0;
    while (pos < size) {
      bluestore_pextent_t ext = unsqueeze_extent(v[pos]);
      denc(ext, p);
      ++pos;
    }
  }
  static void decode(PExtentArray& v, bufferptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    ceph_assert(num < v.size());
    v.fill(INVALID_OFFSET);

    for (unsigned i = 0; i < num; ++i) {
      bluestore_pextent_t dummy;
      denc(dummy, p);
      v[i] = squeeze_extent(dummy);
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

/// blob_use_tracker: a set of per-alloc unit ref counters to track blob usage
struct bluestore_blob_use_tracker_t {
  // N.B.: There is no need to minimize au_size/num_au
  //   as much as possible (e.g. have just a single byte for au_size) since:
  //   1) Struct isn't packed hence it's padded. And even if it's packed see 2)
  //   2) Mem manager has its own granularity, most probably >= 8 bytes
  //
  uint32_t au_size; // Allocation (=tracking) unit size,
                    // == 0 if uninitialized
  uint32_t num_au;  // Amount of allocation units tracked
                    // == 0 if single unit or the whole blob is tracked
                       
  union {
    uint32_t* bytes_per_au;
    uint32_t total_bytes;
  };
  
  bluestore_blob_use_tracker_t()
    : au_size(0), num_au(0), bytes_per_au(nullptr) {
  }
  bluestore_blob_use_tracker_t(const bluestore_blob_use_tracker_t& tracker);
  bluestore_blob_use_tracker_t& operator=(const bluestore_blob_use_tracker_t& rhs);
  ~bluestore_blob_use_tracker_t() {
    clear();
  }

  void clear() {
    if (num_au != 0) {
      delete[] bytes_per_au;
      mempool::get_pool(
        mempool::pool_index_t(mempool::mempool_bluestore_cache_other)).
          adjust_count(-1, -sizeof(uint32_t) * num_au);
    }
    bytes_per_au = 0;
    au_size = 0;
    num_au = 0;
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
	num_au = _num_au;
	allocate();
	for (size_t i = 0; i < old_num_au; i++) {
	  bytes_per_au[i] = old_bytes[i];
	}
	for (size_t i = old_num_au; i < num_au; i++) {
	  bytes_per_au[i] = 0;
	}
	delete[] old_bytes;
      }
    }
  }

  void init(
    uint32_t full_length,
    uint32_t _au_size);

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

  bool can_split() const;
  bool can_split_at(uint32_t blob_offset) const;
  void split(
    uint32_t blob_offset,
    bluestore_blob_use_tracker_t* r);

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
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        allocate();
        for (size_t i = 0; i < num_au; ++i) {
	  denc_varint(bytes_per_au[i], p);
        }
      }
    }
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_blob_use_tracker_t*>& o);
private:
  void allocate();
};
WRITE_CLASS_DENC(bluestore_blob_use_tracker_t)
std::ostream& operator<<(std::ostream& out, const bluestore_blob_use_tracker_t& rm);

/// blob: a piece of data on disk
struct bluestore_blob_t {
private:
  union {
    PExtentVector _extents;              ///< raw data position on device
    PExtentArray _squeezed_extents;
  };

  uint32_t logical_length = 0;        ///< original length of data stored in the blob
  uint32_t compressed_length = 0;     ///< compressed length if any

  static void unsqueeze_extents(const PExtentArray& a,
                                PExtentVector& res) {
    res.clear();
    for (auto i : a) {
      if (i == INVALID_OFFSET) {
        break;
      }
      res.emplace_back(unsqueeze_extent(i));
    }
  }
  void unsqueeze_extents() {
    PExtentVector extents;
    unsqueeze_extents(_squeezed_extents, extents);
    new (&_extents) PExtentVector;
    _extents.swap(extents);
    clear_flag(FLAG_SHORT_EXTENTS_LIST);
  }
  void _update_extent(size_t idx, const bluestore_pextent_t& p) {
    if (has_short_extents_list()) {
      _squeezed_extents[idx] = squeeze_extent(p);
    } else {
      _extents[idx] = p;
    }
  }
  void _prune_tailing_extents(size_t new_size) {
    if (has_short_extents_list()) {
      for (size_t i = new_size; i < _squeezed_extents.size(); i++) {
        _squeezed_extents[i] = INVALID_OFFSET;
      }
    } else if (new_size < _extents.size()) {
      _extents.resize(new_size);
    }
  }

  void reset_extents(bool use_short) {
    if (use_short) {
      if (!has_short_extents_list()) {
        _extents.~PExtentVector();
        set_flag(FLAG_SHORT_EXTENTS_LIST);
      }
      _squeezed_extents.fill(INVALID_OFFSET);
    } else {
      if (has_short_extents_list()) {
        clear_flag(FLAG_SHORT_EXTENTS_LIST);
      }
      new (&_extents) PExtentVector;
    }
  }

public:
  enum {
    LEGACY_FLAG_MUTABLE = 1,  ///< [legacy] blob can be overwritten or split
    FLAG_COMPRESSED = 2,      ///< blob is compressed
    FLAG_CSUM = 4,            ///< blob has checksums
    FLAG_HAS_UNUSED = 8,      ///< blob has unused std::map
    FLAG_SHARED = 16,         ///< blob is shared; see external SharedBlob
    FLAG_SHORT_EXTENTS_LIST = 32, ///< blob has pextents which fit into _squeezed_extents
  };
  static std::string get_flags_string(unsigned flags);

  uint32_t flags = 0;                 ///< FLAG_*

  typedef uint16_t unused_t;
  unused_t unused = 0;     ///< portion that has never been written to (bitmap)

  uint8_t csum_type = Checksummer::CSUM_NONE;      ///< CSUM_*
  uint8_t csum_chunk_order = 0;       ///< csum block size is 1<<block_order bytes

  ceph::buffer::ptr csum_data;                ///< opaque std::vector of csum data

  bluestore_blob_t(uint32_t f = 0);
  ~bluestore_blob_t();

  bluestore_blob_t& operator=(const bluestore_blob_t& other) {
    if (other.has_short_extents_list()) {
      if (!has_short_extents_list()) {
        _extents.~PExtentVector();
      }
      _squeezed_extents = other._squeezed_extents;
    } else {
      if (has_short_extents_list()) {
        new (&_extents) PExtentVector;
      }
      _extents = other._extents;
    }
    logical_length = other.logical_length;
    compressed_length = other.compressed_length;
    flags = other.flags;
    unused = other.unused;
    csum_type = other.csum_type;
    csum_chunk_order = other.csum_chunk_order;

    csum_data = other.csum_data;
    return *this;
  }
  // ineffective extent vector accessor intended mainly for testing purposed
  PExtentVector get_extents() const {
    PExtentVector res;
    if (has_short_extents_list()) {
      unsqueeze_extents(_squeezed_extents, res);
    } else {
      res = _extents;
    }
    return res;
  }

  DENC_HELPERS;
  void bound_encode(size_t& p, uint64_t struct_v) const;

  void encode(ceph::buffer::list::contiguous_appender& p, uint64_t struct_v) const;
  void decode(ceph::buffer::ptr::const_iterator& p, uint64_t struct_v);

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
  bool has_short_extents_list() const {
    return has_flag(FLAG_SHORT_EXTENTS_LIST);
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
  uint64_t calc_offset(uint64_t x_off, uint64_t* plen) const;

  // validates whether or not the status of pextents within the given range
  // meets the requirement(allocated or unallocated).
  bool _validate_range(uint64_t b_off, uint64_t b_len,
    bool require_allocated) const;

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

  int for_each_extent(bool include_invalid,
    std::function<int(const bluestore_pextent_t&)> fn) const;

  // map_f_invoke templates intended to mask parameters which are not expected
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
    bool found = false;
    int r = for_each_extent(true,
      [&](const bluestore_pextent_t& p) {
        if (!found) {
          if (x_off >= p.length) {
            x_off -= p.length;
          } else {
            found = true;
          }
        }
        if (found && x_len > 0) {
          uint64_t l = std::min(p.length - x_off, x_len);
          int r = map_f_invoke(x_off0, p, p.offset + x_off, l, f);
          if (r < 0)
            return r;
          x_off = 0;
          x_len -= l;
          x_off0 += l;
        }
        return 0;
      });
    ceph_assert(found);
    return r;
  }

  template<class F>
  int map_bl(uint64_t x_off,
              ceph::buffer::list& bl,
              F&& f) const {
    static_assert(std::is_invocable_v<F, uint64_t, ceph::buffer::list&>);

    bool found = false;
    bufferlist::iterator it = bl.begin();
    uint64_t x_len = bl.length();

    int r = for_each_extent(true,
      [&](const bluestore_pextent_t& p) {
        if (!found) {
          if (x_off >= p.length) {
            x_off -= p.length;
          } else {
            found = true;
          }
        }
        if (found && x_len > 0) {
          uint64_t l = std::min(p.length - x_off, x_len);
          bufferlist t;
          it.copy(l, t);
          int r = f(p.offset + x_off, t);
          if (r < 0)
            return r;
          x_off = 0;
          x_len -= l;
        }
        return 0;
      });
    ceph_assert(found);
    return r;
  }

  template<class F>
  static int map_bl(const PExtentVector& extents,
                      uint64_t x_off,
                      bufferlist& bl,
                      F&& f) {
    static_assert(std::is_invocable_v<F, uint64_t, bufferlist&>);

    auto p = extents.cbegin();
    ceph_assert(p != extents.cend());
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      ceph_assert(p != extents.cend());
    }
    ceph::buffer::list::iterator it = bl.begin();
    uint64_t x_len = bl.length();
    while (x_len > 0) {
      ceph_assert(p != extents.cend());
      uint64_t l = std::min(p->length - x_off, x_len);
      ceph::buffer::list t;
      it.copy(l, t);
      int r = f(p->offset + x_off, t);
      if (r < 0) {
        return r;
      }
      x_off = 0;
      x_len -= l;
      ++p;
    }
    return 0;
  }

  uint32_t get_ondisk_length() const;
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

  bool try_prune_tail();
  void add_tail(uint32_t new_len);
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
  void allocated_test(const bluestore_pextent_t& alloc); // intended for UT only

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

  void swap_extents(PExtentVector& others) {
    // For the sake of simplicity doing in a straightforward manner
    // without trying to preserve short format if any.
    // Good enough for now as the function has limited usage (fsck/repair only).
    //
    if (has_short_extents_list()) {
      unsqueeze_extents();
    }
    _extents.swap(others);
  }
};
WRITE_CLASS_DENC_FEATURED(bluestore_blob_t)

std::ostream& operator<<(std::ostream& out, const bluestore_blob_t& o);


/// shared blob state
struct bluestore_shared_blob_t {
  MEMPOOL_CLASS_HELPERS();
  uint64_t sbid;                       ///> shared blob id
  bluestore_extent_ref_map_t ref_map;  ///< shared blob extents

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
  };
  std::vector<shard_info> extent_map_shards; ///< extent std::map shards (if any)

  uint32_t expected_object_size = 0;
  uint32_t expected_write_size = 0;
  uint32_t alloc_hint_flags = 0;

  uint8_t flags = 0;

  enum {
    FLAG_OMAP = 1,       ///< object may have omap data
    FLAG_PGMETA_OMAP = 2,  ///< omap data is in meta omap prefix
    FLAG_PERPOOL_OMAP = 4, ///< omap data is in per-pool prefix; per-pool keys
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
      s += "+perpool_omap";
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
  bool is_pgmeta_omap() const {
    return has_flag(FLAG_PGMETA_OMAP);
  }
  bool is_perpool_omap() const {
    return has_flag(FLAG_PERPOOL_OMAP);
  }

  void set_omap_flags() {
    set_flag(FLAG_OMAP | FLAG_PERPOOL_OMAP);
  }
  void set_omap_flags_pgmeta() {
    set_flag(FLAG_OMAP | FLAG_PGMETA_OMAP);
  }

  void clear_omap_flag() {
    clear_flag(FLAG_OMAP);
  }

  DENC(bluestore_onode_t, v, p) {
    DENC_START(1, 1, p);
    denc_varint(v.nid, p);
    denc_varint(v.size, p);
    denc(v.attrs, p);
    denc(v.flags, p);
    denc(v.extent_map_shards, p);
    denc_varint(v.expected_object_size, p);
    denc_varint(v.expected_write_size, p);
    denc_varint(v.alloc_hint_flags, p);
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
  boost::optional<int32_t> compressor_message;

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


#endif
