// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "osd/osd_types_core.h"
#include "include/rados/rados_types.hpp"

class ObjectCleanRegions {
private:
  bool new_object;
  bool clean_omap;
  interval_set<uint64_t> clean_offsets;
  inline static std::atomic<uint32_t> max_num_intervals{10};

  /**
   * trim the number of intervals if clean_offsets.num_intervals()
   * exceeds the given upbound max_num_intervals
   * etc. max_num_intervals=2, clean_offsets:{[5~10], [20~5]}
   * then new interval [30~10] will evict out the shortest one [20~5]
   * finally, clean_offsets becomes {[5~10], [30~10]}
   */
  void trim();
  friend std::ostream& operator<<(std::ostream& out, const ObjectCleanRegions& ocr);
public:
  ObjectCleanRegions() : new_object(false), clean_omap(true) {
    clean_offsets.insert(0, (uint64_t)-1);
  }
  ObjectCleanRegions(uint64_t offset, uint64_t len, bool co)
    : new_object(false), clean_omap(co) {
    clean_offsets.insert(offset, len);
  }
  bool operator==(const ObjectCleanRegions &orc) const {
    return new_object == orc.new_object && clean_omap == orc.clean_omap && clean_offsets == orc.clean_offsets;
  }
  static void set_max_num_intervals(uint32_t num);
  void merge(const ObjectCleanRegions &other);
  void mark_data_region_dirty(uint64_t offset, uint64_t len);
  void mark_omap_dirty();
  void mark_object_new();
  void mark_fully_dirty();
  interval_set<uint64_t> get_dirty_regions() const;
  bool omap_is_dirty() const;
  bool object_is_exist() const;
  bool is_clean_region(uint64_t offset, uint64_t len) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  std::string fmt_print() const;
  static std::list<ObjectCleanRegions> generate_test_instances();
};
WRITE_CLASS_ENCODER(ObjectCleanRegions)
std::ostream& operator<<(std::ostream& out, const ObjectCleanRegions& ocr);


// -------






/*
 * attached to object head.  describes most recent snap context, and
 * set of existing clones.
 */
struct SnapSet {
  snapid_t seq;
  std::vector<snapid_t> clones;   // ascending
  std::map<snapid_t, interval_set<uint64_t> > clone_overlap;  // overlap w/ next newest
  std::map<snapid_t, uint64_t> clone_size;
  std::map<snapid_t, std::vector<snapid_t>> clone_snaps; // descending

  SnapSet() : seq(0) {}
  explicit SnapSet(ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
    decode(p);
  }

  /// populate SnapSet from a librados::snap_set_t
  void from_snap_set(const librados::snap_set_t& ss, bool legacy);

  /// get space accounted to clone
  uint64_t get_clone_bytes(snapid_t clone) const;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<SnapSet> generate_test_instances();

  SnapContext get_ssc_as_of(snapid_t as_of) const {
    SnapContext out;
    out.seq = as_of;
    for (auto p = clone_snaps.rbegin();
	 p != clone_snaps.rend();
	 ++p) {
      for (auto snap : p->second) {
	if (snap <= as_of) {
	  out.snaps.push_back(snap);
	}
      }
    }
    return out;
  }

};
WRITE_CLASS_ENCODER(SnapSet)

std::ostream& operator<<(std::ostream& out, const SnapSet& cs);

inline static const bool should_whiteout(
  const SnapSet &ss,
  const SnapContext &client_snapc) {
  return !ss.clones.empty() ||
    (!client_snapc.snaps.empty() && client_snapc.snaps[0] > ss.seq);
}

#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_info_t() : cookie(0), timeout_seconds(0) { }
  watch_info_t(uint64_t c, uint32_t t, const entity_addr_t& a) : cookie(c), timeout_seconds(t), addr(a) {}

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  std::string fmt_print() const;
  static std::list<watch_info_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds
	    && l.addr == r.addr;
}

static inline std::ostream& operator<<(std::ostream& out, const watch_info_t& w) {
  return out << w.fmt_print();
}

struct notify_info_t {
  uint64_t cookie;
  uint64_t notify_id;
  uint32_t timeout;
  ceph::buffer::list bl;
};

static inline std::ostream& operator<<(std::ostream& out, const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie
	     << " notify" << n.notify_id
	     << " " << n.timeout << "s)";
}

class object_ref_delta_t {
  std::map<hobject_t, int> ref_delta;

public:
  object_ref_delta_t() = default;
  object_ref_delta_t(const object_ref_delta_t &) = default;
  object_ref_delta_t(object_ref_delta_t &&) = default;

  object_ref_delta_t(decltype(ref_delta) &&ref_delta)
    : ref_delta(std::move(ref_delta)) {}
  object_ref_delta_t(const decltype(ref_delta) &ref_delta)
    : ref_delta(ref_delta) {}

  object_ref_delta_t &operator=(const object_ref_delta_t &) = default;
  object_ref_delta_t &operator=(object_ref_delta_t &&) = default;

  void dec_ref(const hobject_t &hoid, unsigned num=1) {
    mut_ref(hoid, -num);
  }
  void inc_ref(const hobject_t &hoid, unsigned num=1) {
    mut_ref(hoid, num);
  }
  void mut_ref(const hobject_t &hoid, int num) {
    [[maybe_unused]] auto [iter, _] = ref_delta.try_emplace(hoid, 0);
    iter->second += num;
    if (iter->second == 0)
      ref_delta.erase(iter);
  }

  auto begin() const { return ref_delta.begin(); }
  auto end() const { return ref_delta.end(); }
  auto find(hobject_t &key) const { return ref_delta.find(key); }

  bool operator==(const object_ref_delta_t &rhs) const {
    return ref_delta == rhs.ref_delta;
  }
  bool operator!=(const object_ref_delta_t &rhs) const {
    return !(*this == rhs);
  }
  bool is_empty() {
    return ref_delta.empty();
  }
  uint64_t size() {
    return ref_delta.size();
  }
  friend std::ostream& operator<<(std::ostream& out, const object_ref_delta_t & ci);
};

struct chunk_info_t {
  typedef enum {
    FLAG_DIRTY = 1, 
    FLAG_MISSING = 2,
    FLAG_HAS_REFERENCE = 4,
    FLAG_HAS_FINGERPRINT = 8,
  } cflag_t;
  uint32_t offset;
  uint32_t length;
  hobject_t oid;
  cflag_t flags;   // FLAG_*

  chunk_info_t() : offset(0), length(0), flags((cflag_t)0) { }
  chunk_info_t(uint32_t offset, uint32_t length, hobject_t oid) : 
    offset(offset), length(length), oid(oid), flags((cflag_t)0) { }

  static std::string get_flag_string(uint64_t flags) {
    std::string r;
    if (flags & FLAG_DIRTY) {
      r += "|dirty";
    }
    if (flags & FLAG_MISSING) {
      r += "|missing";
    }
    if (flags & FLAG_HAS_REFERENCE) {
      r += "|has_reference";
    }
    if (flags & FLAG_HAS_FINGERPRINT) {
      r += "|has_fingerprint";
    }
    if (r.length())
      return r.substr(1);
    return r;
  }
  bool test_flag(cflag_t f) const {
    return (flags & f) == f;
  }
  void set_flag(cflag_t f) {
    flags = (cflag_t)(flags | f);
  }
  void set_flags(cflag_t f) {
    flags = f;
  }
  void clear_flag(cflag_t f) {
    flags = (cflag_t)(flags & ~f);
  }
  void clear_flags() {
    flags = (cflag_t)0;
  }
  bool is_dirty() const {
    return test_flag(FLAG_DIRTY);
  }
  bool is_missing() const {
    return test_flag(FLAG_MISSING);
  }
  bool has_reference() const {
    return test_flag(FLAG_HAS_REFERENCE);
  }
  bool has_fingerprint() const {
    return test_flag(FLAG_HAS_FINGERPRINT);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<chunk_info_t> generate_test_instances();
  friend std::ostream& operator<<(std::ostream& out, const chunk_info_t& ci);
  bool operator==(const chunk_info_t& cit) const;
  bool operator!=(const chunk_info_t& cit) const {
    return !(cit == *this);
  }
};
WRITE_CLASS_ENCODER(chunk_info_t)
std::ostream& operator<<(std::ostream& out, const chunk_info_t& ci);

struct object_info_t;
struct object_manifest_t {
  enum {
    TYPE_NONE = 0,
    TYPE_REDIRECT = 1, 
    TYPE_CHUNKED = 2, 
  };
  uint8_t type;  // redirect, chunked, ...
  hobject_t redirect_target;
  std::map<uint64_t, chunk_info_t> chunk_map;

  object_manifest_t() : type(0) { }
  object_manifest_t(uint8_t type, const hobject_t& redirect_target) 
    : type(type), redirect_target(redirect_target) { }

  bool is_empty() const {
    return type == TYPE_NONE;
  }
  bool is_redirect() const {
    return type == TYPE_REDIRECT;
  }
  bool is_chunked() const {
    return type == TYPE_CHUNKED;
  }
  static std::string_view get_type_name(uint8_t m) {
    switch (m) {
    case TYPE_NONE: return "none";
    case TYPE_REDIRECT: return "redirect";
    case TYPE_CHUNKED: return "chunked";
    default: return "unknown";
    }
  }
  std::string_view get_type_name() const {
    return get_type_name(type);
  }
  void clear() {
    type = 0;
    redirect_target = hobject_t();
    chunk_map.clear();
  }

  /**
   * calc_refs_to_inc_on_set
   *
   * Takes a manifest and returns the set of refs to
   * increment upon set-chunk
   *
   * l should be nullptr if there are no clones, or 
   * l and g may each be null if the corresponding clone does not exist.
   * *this contains the set of new references to set
   *
   */
  void calc_refs_to_inc_on_set(
    const object_manifest_t* g, ///< [in] manifest for clone > *this
    const object_manifest_t* l, ///< [in] manifest for clone < *this
    object_ref_delta_t &delta   ///< [out] set of refs to drop
  ) const;

  /**
   * calc_refs_to_drop_on_modify
   *
   * Takes a manifest and returns the set of refs to
   * drop upon modification 
   *
   * l should be nullptr if there are no clones, or 
   * l may be null if the corresponding clone does not exist.
   *
   */
  void calc_refs_to_drop_on_modify(
    const object_manifest_t* l, ///< [in] manifest for previous clone 
    const ObjectCleanRegions& clean_regions, ///< [in] clean regions
    object_ref_delta_t &delta    ///< [out] set of refs to drop
  ) const;

  /**
   * calc_refs_to_drop_on_removal
   *
   * Takes the two adjacent manifests and returns the set of refs to
   * drop upon removal of the clone containing *this.
   *
   * g should be nullptr if *this is on HEAD, l should be nullptr if
   * *this is on the oldest clone (or head if there are no clones).
   */
  void calc_refs_to_drop_on_removal(
    const object_manifest_t* g, ///< [in] manifest for clone > *this
    const object_manifest_t* l, ///< [in] manifest for clone < *this
    object_ref_delta_t &delta    ///< [out] set of refs to drop
  ) const;

  static std::list<object_manifest_t> generate_test_instances();
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& out, const object_info_t& oi);
};
WRITE_CLASS_ENCODER(object_manifest_t)
std::ostream& operator<<(std::ostream& out, const object_manifest_t& oi);

struct object_info_t {
  hobject_t soid;
  eversion_t version, prior_version;
  version_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  utime_t mtime;
  utime_t local_mtime; // local mtime

  // note: these are currently encoded into a total 16 bits; see
  // encode()/decode() for the weirdness.
  typedef enum {
    FLAG_LOST        = 1<<0,
    FLAG_WHITEOUT    = 1<<1, // object logically does not exist
    FLAG_DIRTY       = 1<<2, // object has been modified since last flushed or undirtied
    FLAG_OMAP        = 1<<3, // has (or may have) some/any omap data
    FLAG_DATA_DIGEST = 1<<4, // has data crc
    FLAG_OMAP_DIGEST = 1<<5, // has omap crc
    FLAG_CACHE_PIN   = 1<<6, // pin the object in cache tier
    FLAG_MANIFEST    = 1<<7, // has manifest
    FLAG_USES_TMAP   = 1<<8, // deprecated; no longer used
    FLAG_REDIRECT_HAS_REFERENCE = 1<<9, // has reference
  } flag_t;

  flag_t flags;

  static std::string get_flag_string(flag_t flags) {
    std::string s;
    std::vector<std::string> sv = get_flag_vector(flags);
    for (auto ss : sv) {
      s += std::string("|") + ss;
    }
    if (s.length())
      return s.substr(1);
    return s;
  }
  static std::vector<std::string> get_flag_vector(flag_t flags) {
    std::vector<std::string> sv;
    if (flags & FLAG_LOST)
      sv.insert(sv.end(), "lost");
    if (flags & FLAG_WHITEOUT)
      sv.insert(sv.end(), "whiteout");
    if (flags & FLAG_DIRTY)
      sv.insert(sv.end(), "dirty");
    if (flags & FLAG_USES_TMAP)
      sv.insert(sv.end(), "uses_tmap");
    if (flags & FLAG_OMAP)
      sv.insert(sv.end(), "omap");
    if (flags & FLAG_DATA_DIGEST)
      sv.insert(sv.end(), "data_digest");
    if (flags & FLAG_OMAP_DIGEST)
      sv.insert(sv.end(), "omap_digest");
    if (flags & FLAG_CACHE_PIN)
      sv.insert(sv.end(), "cache_pin");
    if (flags & FLAG_MANIFEST)
      sv.insert(sv.end(), "manifest");
    if (flags & FLAG_REDIRECT_HAS_REFERENCE)
      sv.insert(sv.end(), "redirect_has_reference");
    return sv;
  }
  std::string get_flag_string() const {
    return get_flag_string(flags);
  }

  uint64_t truncate_seq, truncate_size;

  std::map<std::pair<uint64_t, entity_name_t>, watch_info_t> watchers;

  // opportunistic checksums; may or may not be present
  __u32 data_digest;  ///< data crc32c
  __u32 omap_digest;  ///< omap crc32c
  
  // alloc hint attribute
  uint64_t expected_object_size, expected_write_size;
  uint32_t alloc_hint_flags;

  struct object_manifest_t manifest;

  std::map<shard_id_t,eversion_t> shard_versions;

  void copy_user_bits(const object_info_t& other);

  bool test_flag(flag_t f) const {
    return (flags & f) == f;
  }
  void set_flag(flag_t f) {
    flags = (flag_t)(flags | f);
  }
  void clear_flag(flag_t f) {
    flags = (flag_t)(flags & ~f);
  }
  bool is_lost() const {
    return test_flag(FLAG_LOST);
  }
  bool is_whiteout() const {
    return test_flag(FLAG_WHITEOUT);
  }
  bool is_dirty() const {
    return test_flag(FLAG_DIRTY);
  }
  bool is_omap() const {
    return test_flag(FLAG_OMAP);
  }
  bool is_data_digest() const {
    return test_flag(FLAG_DATA_DIGEST);
  }
  bool is_omap_digest() const {
    return test_flag(FLAG_OMAP_DIGEST);
  }
  bool is_cache_pinned() const {
    return test_flag(FLAG_CACHE_PIN);
  }
  bool has_manifest() const {
    return test_flag(FLAG_MANIFEST);
  }
  void set_data_digest(__u32 d) {
    set_flag(FLAG_DATA_DIGEST);
    data_digest = d;
  }
  void set_omap_digest(__u32 d) {
    set_flag(FLAG_OMAP_DIGEST);
    omap_digest = d;
  }
  void clear_data_digest() {
    clear_flag(FLAG_DATA_DIGEST);
    data_digest = -1;
  }
  void clear_omap_digest() {
    clear_flag(FLAG_OMAP_DIGEST);
    omap_digest = -1;
  }
  void new_object() {
    clear_data_digest();
    clear_omap_digest();
  }

  eversion_t get_version_for_shard(shard_id_t shard) const {
    auto iter = shard_versions.find(shard);

    // If the shard_versions is not included, then it is the same as this.
    if (iter == shard_versions.end()) {
      return version;
    }
    // Otherwise, the shard_versions should be fully populated.
    return iter->second;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void decode(const ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
    decode(p);
  }

  void encode_no_oid(ceph::buffer::list& bl, uint64_t features) {
    // TODO: drop soid field and remove the denc no_oid methods
    auto tmp_oid = hobject_t(hobject_t::get_max());
    tmp_oid.swap(soid);
    encode(bl, features);
    soid = tmp_oid;
  }
  void decode_no_oid(ceph::buffer::list::const_iterator& bl) {
    decode(bl);
    ceph_assert(soid.is_max());
  }
  void decode_no_oid(const ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
    decode_no_oid(p);
  }
  void decode_no_oid(const ceph::buffer::list& bl, const hobject_t& _soid) {
    auto p = std::cbegin(bl);
    decode_no_oid(p);
    soid = _soid;
  }

  void dump(ceph::Formatter *f) const;
  static std::list<object_info_t> generate_test_instances();

  explicit object_info_t()
    : user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1),
      expected_object_size(0), expected_write_size(0),
      alloc_hint_flags(0)
  {}

  explicit object_info_t(const hobject_t& s)
    : soid(s),
      user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1),
      expected_object_size(0), expected_write_size(0),
      alloc_hint_flags(0)
  {}

  explicit object_info_t(const ceph::buffer::list& bl) {
    decode(bl);
  }

  explicit object_info_t(const ceph::buffer::list& bl, const hobject_t& _soid) {
    decode_no_oid(bl);
    soid = _soid;
  }
};
WRITE_CLASS_ENCODER_FEATURES(object_info_t)

std::ostream& operator<<(std::ostream& out, const object_info_t& oi);



// Object recovery
struct ObjectRecoveryInfo {
  hobject_t soid;
  eversion_t version;
  uint64_t size;
  uint64_t num_omap_keys;
  object_info_t oi;
  SnapSet ss;   // only populated if soid is_snap()
  interval_set<uint64_t> copy_subset;
  std::map<hobject_t, interval_set<uint64_t>> clone_subset;
  bool object_exist;

  ObjectRecoveryInfo() : size(0), num_omap_keys(0), object_exist(true) { }

  static std::list<ObjectRecoveryInfo> generate_test_instances();
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1);
  std::string fmt_print() const;
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER_FEATURES(ObjectRecoveryInfo)
std::ostream& operator<<(std::ostream& out, const ObjectRecoveryInfo &inf);

struct ObjectRecoveryProgress {
  uint64_t data_recovered_to{0};
  std::string omap_recovered_to;
  bool first{true};
  bool data_complete{false};
  bool omap_complete{false};
  bool error{false};

  ObjectRecoveryProgress() {}

  bool is_complete(const ObjectRecoveryInfo& info) const {
    return (data_recovered_to >= (
      info.copy_subset.empty() ?
      0 : info.copy_subset.range_end())) &&
      omap_complete;
  }

  uint64_t estimate_remaining_data_to_recover(const ObjectRecoveryInfo& info) const {
    // Overestimates in case of clones, but avoids traversing copy_subset
    return info.size - data_recovered_to;
  }

  static std::list<ObjectRecoveryProgress> generate_test_instances();
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  std::string fmt_print() const;
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryProgress)
std::ostream& operator<<(std::ostream& out, const ObjectRecoveryProgress &prog);

struct PushReplyOp {
  hobject_t soid;

  static std::list<PushReplyOp> generate_test_instances();
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER(PushReplyOp)
std::ostream& operator<<(std::ostream& out, const PushReplyOp &op);

struct PullOp {
  hobject_t soid;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress recovery_progress;

  static std::list<PullOp> generate_test_instances();
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER_FEATURES(PullOp)
std::ostream& operator<<(std::ostream& out, const PullOp &op);

struct PushOp {
  hobject_t soid;
  eversion_t version;
  ceph::buffer::list data;
  interval_set<uint64_t> data_included;
  ceph::buffer::list omap_header;
  std::map<std::string, ceph::buffer::list> omap_entries;
  std::map<std::string, ceph::buffer::list, std::less<>> attrset;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress before_progress;
  ObjectRecoveryProgress after_progress;

  static std::list<PushOp> generate_test_instances();
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER_FEATURES(PushOp)
std::ostream& operator<<(std::ostream& out, const PushOp &op);
