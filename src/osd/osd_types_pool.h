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

#ifndef CEPH_OSD_TYPES_POOL_H
#define CEPH_OSD_TYPES_POOL_H

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
#include <variant>

#include "osd/HitSet.h"
#include "osd/osd_types_core.h"

class CrushWrapper;

/*
 * pool_snap_info_t
 *
 * attributes for a single pool snapshot.  
 */
struct pool_snap_info_t {
  snapid_t snapid;
  utime_t stamp;
  std::string name;

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static std::list<pool_snap_info_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(pool_snap_info_t)

inline std::ostream& operator<<(std::ostream& out, const pool_snap_info_t& si) {
  return out << si.snapid << '(' << si.name << ' ' << si.stamp << ')';
}


/*
 * pool_opts_t
 *
 * pool options.
 */

// The order of items in the list is important, therefore,
// you should always add to the end of the list when adding new options.

class pool_opts_t {
public:
  enum key_t {
    SCRUB_MIN_INTERVAL,
    SCRUB_MAX_INTERVAL,
    DEEP_SCRUB_INTERVAL,
    RECOVERY_PRIORITY,
    RECOVERY_OP_PRIORITY,
    SCRUB_PRIORITY,
    COMPRESSION_MODE,
    COMPRESSION_ALGORITHM,
    COMPRESSION_REQUIRED_RATIO,
    COMPRESSION_MAX_BLOB_SIZE,
    COMPRESSION_MIN_BLOB_SIZE,
    CSUM_TYPE,
    CSUM_MAX_BLOCK,
    CSUM_MIN_BLOCK,
    FINGERPRINT_ALGORITHM,
    PG_NUM_MIN,         // min pg_num
    TARGET_SIZE_BYTES,  // total bytes in pool
    TARGET_SIZE_RATIO,  // fraction of total cluster
    PG_AUTOSCALE_BIAS,
    READ_LEASE_INTERVAL,
    DEDUP_TIER,
    DEDUP_CHUNK_ALGORITHM,
    DEDUP_CDC_CHUNK_SIZE,
    PG_NUM_MAX, // max pg_num
    READ_RATIO, // read ration for the read balancer work [0-100]
    /**
     * PCT_UPDATE_DELAY
     *
     * Time to wait (seconds) after there are no in progress writes before
     * updating pg_committed_to on replicas.  If the period between writes on
     * a PG is usually longer than this value, most writes will trigger an
     * extra message.
     *
     * The primary reason to enable this feature would be to limit the time
     * between a write and when that write is available to be read on replicas.
     *
     * A value <= 0 will cause the update to be sent immediately upon write
     * completion if there are no other in progress writes.
     */
    PCT_UPDATE_DELAY,
  };

  enum type_t {
    STR,
    INT,
    DOUBLE,
  };

  struct opt_desc_t {
    key_t key;
    type_t type;

    opt_desc_t(key_t k, type_t t) : key(k), type(t) {}

    bool operator==(const opt_desc_t& rhs) const {
      return key == rhs.key && type == rhs.type;
    }
  };

  typedef std::variant<std::string,int64_t,double> value_t;

  static bool is_opt_name(const std::string& name);
  static opt_desc_t get_opt_desc(const std::string& name);

  pool_opts_t() : opts() {}

  bool is_set(key_t key) const;

  template<typename T>
  void set(key_t key, T &&val) {
    opts.insert_or_assign(key, std::forward<T>(val));
  }

  template<typename T>
  bool get(key_t key, T *val) const {
    opts_t::const_iterator i = opts.find(key);
    if (i == opts.end()) {
      return false;
    }
    *val = std::get<T>(i->second);
    return true;
  }

  template<typename T>
  T value_or(key_t key, T&& default_value) const {
    auto i = opts.find(key);
    if (i == opts.end()) {
      return std::forward<T>(default_value);
    }
    return std::get<T>(i->second);
  }

  const value_t& get(key_t key) const;

  bool unset(key_t key);

  void dump(const std::string& name, ceph::Formatter *f) const;

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static std::list<pool_opts_t> generate_test_instances();

private:
  typedef std::map<key_t, value_t> opts_t;
  opts_t opts;

  friend std::ostream& operator<<(std::ostream& out, const pool_opts_t& opts);
};
WRITE_CLASS_ENCODER_FEATURES(pool_opts_t)

template <typename T, typename... Ts>
std::ostream& operator<<(std::ostream& out, const std::variant<T, Ts...>& v) {
  std::visit([&out](const auto& value) {
    out << value;
  }, v);
  return out;
}

struct pg_merge_meta_t {
  pg_t source_pgid;
  epoch_t ready_epoch = 0;
  epoch_t last_epoch_started = 0;
  epoch_t last_epoch_clean = 0;
  eversion_t source_version;
  eversion_t target_version;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_pgid, bl);
    encode(ready_epoch, bl);
    encode(last_epoch_started, bl);
    encode(last_epoch_clean, bl);
    encode(source_version, bl);
    encode(target_version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(source_pgid, p);
    decode(ready_epoch, p);
    decode(last_epoch_started, p);
    decode(last_epoch_clean, p);
    decode(source_version, p);
    decode(target_version, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("source_pgid") << source_pgid;
    f->dump_unsigned("ready_epoch", ready_epoch);
    f->dump_unsigned("last_epoch_started", last_epoch_started);
    f->dump_unsigned("last_epoch_clean", last_epoch_clean);
    f->dump_stream("source_version") << source_version;
    f->dump_stream("target_version") << target_version;
  }
  static std::list<pg_merge_meta_t> generate_test_instances() {
    std::list<pg_merge_meta_t> o;
    o.emplace_back();
    o.emplace_back();
    o.back().source_pgid = pg_t(1,2);
    o.back().ready_epoch = 1;
    o.back().last_epoch_started = 2;
    o.back().last_epoch_clean = 3;
    o.back().source_version = eversion_t(4,5);
    o.back().target_version = eversion_t(6,7);
    return o;
  }
};
WRITE_CLASS_ENCODER(pg_merge_meta_t)

class OSDMap;

/*
 * pg_pool
 */
struct pg_pool_t {
  inline static constexpr const char *APPLICATION_NAME_CEPHFS = "cephfs";
  inline static constexpr const char *APPLICATION_NAME_RBD = "rbd";
  inline static constexpr const char *APPLICATION_NAME_RGW = "rgw";

  enum {
    TYPE_REPLICATED = 1,     // replication
    //TYPE_RAID4 = 2,   // raid4 (never implemented)
    TYPE_ERASURE = 3,      // erasure-coded
  };
  static constexpr uint32_t pg_CRUSH_ITEM_NONE = 0x7fffffff; /* can't import crush.h here */
  static std::string_view get_type_name(int t) {
    switch (t) {
    case TYPE_REPLICATED: return "replicated";
      //case TYPE_RAID4: return "raid4";
    case TYPE_ERASURE: return "erasure";
    default: return "???";
    }
  }
  std::string_view get_type_name() const {
    return get_type_name(type);
  }

  enum {
    FLAG_HASHPSPOOL = 1<<0, // hash pg seed and pool together (instead of adding)
    FLAG_FULL       = 1<<1, // pool is full
    FLAG_EC_OVERWRITES = 1<<2, // enables overwrites, once enabled, cannot be disabled
    FLAG_INCOMPLETE_CLONES = 1<<3, // may have incomplete clones (bc we are/were an overlay)
    FLAG_NODELETE = 1<<4, // pool can't be deleted
    FLAG_NOPGCHANGE = 1<<5, // pool's pg and pgp num can't be changed
    FLAG_NOSIZECHANGE = 1<<6, // pool's size and min size can't be changed
    FLAG_WRITE_FADVISE_DONTNEED = 1<<7, // write mode with LIBRADOS_OP_FLAG_FADVISE_DONTNEED
    FLAG_NOSCRUB = 1<<8, // block periodic scrub
    FLAG_NODEEP_SCRUB = 1<<9, // block periodic deep-scrub
    FLAG_FULL_QUOTA = 1<<10, // pool is currently running out of quota, will set FLAG_FULL too
    FLAG_NEARFULL = 1<<11, // pool is nearfull
    FLAG_BACKFILLFULL = 1<<12, // pool is backfillfull
    FLAG_SELFMANAGED_SNAPS = 1<<13, // pool uses selfmanaged snaps
    FLAG_POOL_SNAPS = 1<<14,        // pool has pool snaps
    FLAG_CREATING = 1<<15,          // initial pool PGs are being created
    FLAG_EIO = 1<<16,               // return EIO for all client ops
    FLAG_BULK = 1<<17, //pool is large
    // PGs from this pool are allowed to be created on crimson osds.
    // Pool features are restricted to those supported by crimson-osd.
    // Note, does not prohibit being created on classic osd.
    FLAG_CRIMSON = 1<<18,
    FLAG_EC_OPTIMIZATIONS = 1<<19, // enable optimizations, once enabled, cannot be disabled
    FLAG_CLIENT_SPLIT_READS = 1<<20, // Optimized EC is permitted to do direct reads.
    FLAG_OMAP = 1<<21, // Pool is permitted to perform OMAP operations
    // Allow decreasing pg_num/pgp_num (PG merge) for crimson pools.
    // Note: requires that the pool is currently all bluestore.
    FLAG_CRIMSON_ALLOW_PG_MERGE = 1<<22,
  };

  static const char *get_flag_name(uint64_t f) {
    switch (f) {
    case FLAG_HASHPSPOOL: return "hashpspool";
    case FLAG_FULL: return "full";
    case FLAG_EC_OVERWRITES: return "ec_overwrites";
    case FLAG_INCOMPLETE_CLONES: return "incomplete_clones";
    case FLAG_NODELETE: return "nodelete";
    case FLAG_NOPGCHANGE: return "nopgchange";
    case FLAG_NOSIZECHANGE: return "nosizechange";
    case FLAG_WRITE_FADVISE_DONTNEED: return "write_fadvise_dontneed";
    case FLAG_NOSCRUB: return "noscrub";
    case FLAG_NODEEP_SCRUB: return "nodeep-scrub";
    case FLAG_FULL_QUOTA: return "full_quota";
    case FLAG_NEARFULL: return "nearfull";
    case FLAG_BACKFILLFULL: return "backfillfull";
    case FLAG_SELFMANAGED_SNAPS: return "selfmanaged_snaps";
    case FLAG_POOL_SNAPS: return "pool_snaps";
    case FLAG_CREATING: return "creating";
    case FLAG_EIO: return "eio";
    case FLAG_BULK: return "bulk";
    case FLAG_CRIMSON: return "crimson";
    case FLAG_EC_OPTIMIZATIONS: return "ec_optimizations";
    case FLAG_CLIENT_SPLIT_READS: return "split_reads";
    case FLAG_OMAP: return "supports_omap";
    case FLAG_CRIMSON_ALLOW_PG_MERGE: return "crimson_allow_pg_merge";
    default: return "???";
    }
  }
  static std::string get_flags_string(uint64_t f) {
    std::string s;
    for (unsigned n=0; f && n<64; ++n) {
      if (f & (1ull << n)) {
	if (s.length())
	  s += ",";
	s += get_flag_name(1ull << n);
      }
    }
    return s;
  }
  std::string get_flags_string() const {
    return get_flags_string(flags);
  }
  static uint64_t get_flag_by_name(const std::string& name) {
    if (name == "hashpspool")
      return FLAG_HASHPSPOOL;
    if (name == "full")
      return FLAG_FULL;
    if (name == "ec_overwrites")
      return FLAG_EC_OVERWRITES;
    if (name == "incomplete_clones")
      return FLAG_INCOMPLETE_CLONES;
    if (name == "nodelete")
      return FLAG_NODELETE;
    if (name == "nopgchange")
      return FLAG_NOPGCHANGE;
    if (name == "nosizechange")
      return FLAG_NOSIZECHANGE;
    if (name == "write_fadvise_dontneed")
      return FLAG_WRITE_FADVISE_DONTNEED;
    if (name == "noscrub")
      return FLAG_NOSCRUB;
    if (name == "nodeep-scrub")
      return FLAG_NODEEP_SCRUB;
    if (name == "full_quota")
      return FLAG_FULL_QUOTA;
    if (name == "nearfull")
      return FLAG_NEARFULL;
    if (name == "backfillfull")
      return FLAG_BACKFILLFULL;
    if (name == "selfmanaged_snaps")
      return FLAG_SELFMANAGED_SNAPS;
    if (name == "pool_snaps")
      return FLAG_POOL_SNAPS;
    if (name == "creating")
      return FLAG_CREATING;
    if (name == "eio")
      return FLAG_EIO;
    if (name == "bulk")
      return FLAG_BULK;
    if (name == "crimson")
      return FLAG_CRIMSON;
    if (name == "crimson_allow_pg_merge")
      return FLAG_CRIMSON_ALLOW_PG_MERGE;
    if (name == "ec_optimizations")
      return FLAG_EC_OPTIMIZATIONS;
    if (name == "split_reads")
      return FLAG_CLIENT_SPLIT_READS;
    if (name == "supports_omap")
      return FLAG_OMAP;
    return 0;
  }

  /// converts the acting/up vector to a set of pg shards
  void convert_to_pg_shards(const std::vector<int> &from, std::set<pg_shard_t>* to) const;

  typedef enum {
    CACHEMODE_NONE = 0,                  ///< no caching
    CACHEMODE_WRITEBACK = 1,             ///< write to cache, flush later
    CACHEMODE_FORWARD = 2,               ///< forward if not in cache
    CACHEMODE_READONLY = 3,              ///< handle reads, forward writes [not strongly consistent]
    CACHEMODE_READFORWARD = 4,           ///< forward reads, write to cache flush later
    CACHEMODE_READPROXY = 5,             ///< proxy reads, write to cache flush later
    CACHEMODE_PROXY = 6,                 ///< proxy if not in cache
  } cache_mode_t;
  static const char *get_cache_mode_name(cache_mode_t m) {
    switch (m) {
    case CACHEMODE_NONE: return "none";
    case CACHEMODE_WRITEBACK: return "writeback";
    case CACHEMODE_FORWARD: return "forward";
    case CACHEMODE_READONLY: return "readonly";
    case CACHEMODE_READFORWARD: return "readforward";
    case CACHEMODE_READPROXY: return "readproxy";
    case CACHEMODE_PROXY: return "proxy";
    default: return "unknown";
    }
  }
  static cache_mode_t get_cache_mode_from_str(const std::string& s) {
    if (s == "none")
      return CACHEMODE_NONE;
    if (s == "writeback")
      return CACHEMODE_WRITEBACK;
    if (s == "forward")
      return CACHEMODE_FORWARD;
    if (s == "readonly")
      return CACHEMODE_READONLY;
    if (s == "readforward")
      return CACHEMODE_READFORWARD;
    if (s == "readproxy")
      return CACHEMODE_READPROXY;
    if (s == "proxy")
      return CACHEMODE_PROXY;
    return (cache_mode_t)-1;
  }
  const char *get_cache_mode_name() const {
    return get_cache_mode_name(cache_mode);
  }
  bool cache_mode_requires_hit_set() const {
    switch (cache_mode) {
    case CACHEMODE_NONE:
    case CACHEMODE_FORWARD:
    case CACHEMODE_READONLY:
    case CACHEMODE_PROXY:
      return false;
    case CACHEMODE_WRITEBACK:
    case CACHEMODE_READFORWARD:
    case CACHEMODE_READPROXY:
      return true;
    default:
      ceph_abort_msg("implement me");
    }
  }

  enum class pg_autoscale_mode_t : uint8_t {
    OFF = 0,
    WARN = 1,
    ON = 2,
    UNKNOWN = UINT8_MAX,
  };
  static const char *get_pg_autoscale_mode_name(pg_autoscale_mode_t m) {
    switch (m) {
    case pg_autoscale_mode_t::OFF: return "off";
    case pg_autoscale_mode_t::ON: return "on";
    case pg_autoscale_mode_t::WARN: return "warn";
    default: return "???";
    }
  }
  static pg_autoscale_mode_t get_pg_autoscale_mode_by_name(const std::string& m) {
    if (m == "off") {
      return pg_autoscale_mode_t::OFF;
    }
    if (m == "warn") {
      return pg_autoscale_mode_t::WARN;
    }
    if (m == "on") {
      return pg_autoscale_mode_t::ON;
    }
    return pg_autoscale_mode_t::UNKNOWN;
  }

  utime_t create_time;
  uint64_t flags = 0;           ///< FLAG_*
  __u8 type = 0;                ///< TYPE_*
  __u8 size = 0, min_size = 0;  ///< number of osds in each pg
  __u8 crush_rule = 0;          ///< crush placement rule
  __u8 object_hash = 0;         ///< hash mapping object name to ps
  pg_autoscale_mode_t pg_autoscale_mode = pg_autoscale_mode_t::UNKNOWN;

private:
  __u32 pg_num = 0, pgp_num = 0;  ///< number of pgs
  __u32 pg_num_pending = 0;       ///< pg_num we are about to merge down to
  __u32 pg_num_target = 0;        ///< pg_num we should converge toward
  __u32 pgp_num_target = 0;       ///< pgp_num we should converge toward

public:
  std::map<std::string, std::string> properties;  ///< OBSOLETE
  std::string erasure_code_profile; ///< name of the erasure code profile in OSDMap
  // Profile values stored in integer format to allow reading efficiently
  // without parsing the erasure_code_profile string
  std::optional<uint8_t> ec_data_shard_count, ec_coding_shard_count; ///< ec profile values
  epoch_t last_change = 0;      ///< most recent epoch changed, exclusing snapshot changes
  // If non-zero, require OSDs in at least this many different instances...
  uint32_t peering_crush_bucket_count = 0;
  // of this bucket type...
  uint32_t peering_crush_bucket_barrier = 0;
  // including this one
  int32_t peering_crush_mandatory_member = pg_CRUSH_ITEM_NONE;
  // The per-bucket replica count is calculated with this "target"
  // instead of the above crush_bucket_count. This means we can maintain a
  // target size of 4 without attempting to place them all in 1 DC
  uint32_t peering_crush_bucket_target = 0;
  /// last epoch that forced clients to resend
  epoch_t last_force_op_resend = 0;
  /// last epoch that forced clients to resend (pre-nautilus clients only)
  epoch_t last_force_op_resend_prenautilus = 0;
  /// last epoch that forced clients to resend (pre-luminous clients only)
  epoch_t last_force_op_resend_preluminous = 0;

  /// metadata for the most recent PG merge
  pg_merge_meta_t last_pg_merge_meta;
  
  snapid_t snap_seq = 0;        ///< seq for per-pool snapshot
  epoch_t snap_epoch = 0;       ///< osdmap epoch of last snap
  uint64_t auid = 0;            ///< who owns the pg

  uint64_t quota_max_bytes = 0; ///< maximum number of bytes for this pool
  uint64_t quota_max_objects = 0; ///< maximum number of objects for this pool

  /*
   * Pool snaps (global to this pool).  These define a SnapContext for
   * the pool, unless the client manually specifies an alternate
   * context.
   */
  std::map<snapid_t, pool_snap_info_t> snaps;
  /*
   * Alternatively, if we are defining non-pool snaps (e.g. via the
   * Ceph MDS), we must track @removed_snaps (since @snaps is not
   * used).  Snaps and removed_snaps are to be used exclusive of each
   * other!
   */
  interval_set<snapid_t> removed_snaps;

  unsigned pg_num_mask = 0, pgp_num_mask = 0;

  std::set<uint64_t> tiers;      ///< pools that are tiers of us
  int64_t tier_of = -1;         ///< pool for which we are a tier
  // Note that write wins for read+write ops
  int64_t read_tier = -1;       ///< pool/tier for objecter to direct reads to
  int64_t write_tier = -1;      ///< pool/tier for objecter to direct writes to
  cache_mode_t cache_mode = CACHEMODE_NONE;  ///< cache pool mode

  bool is_tier() const { return tier_of >= 0; }
  bool has_tiers() const { return !tiers.empty(); }
  void clear_tier() {
    tier_of = -1;
    clear_read_tier();
    clear_write_tier();
    clear_tier_tunables();
  }
  bool has_read_tier() const { return read_tier >= 0; }
  void clear_read_tier() { read_tier = -1; }
  bool has_write_tier() const { return write_tier >= 0; }
  void clear_write_tier() { write_tier = -1; }
  void clear_tier_tunables() {
    if (cache_mode != CACHEMODE_NONE)
      flags |= FLAG_INCOMPLETE_CLONES;
    cache_mode = CACHEMODE_NONE;

    target_max_bytes = 0;
    target_max_objects = 0;
    cache_target_dirty_ratio_micro = 0;
    cache_target_dirty_high_ratio_micro = 0;
    cache_target_full_ratio_micro = 0;
    hit_set_params = HitSet::Params();
    hit_set_period = 0;
    hit_set_count = 0;
    hit_set_grade_decay_rate = 0;
    hit_set_search_last_n = 0;
    grade_table.resize(0);
  }

  bool has_snaps() const {
    return snaps.size() > 0;
  }

  bool is_stretch_pool() const {
    return peering_crush_bucket_count != 0;
  }
  
  std::optional<std::tuple<uint32_t,uint32_t,uint32_t,uint32_t>> maybe_peering_crush_data() const {
    if (!is_stretch_pool()) {
        return std::nullopt;
    } else {
        return std::make_optional(std::tuple<uint32_t,uint32_t,uint32_t,uint32_t>(peering_crush_bucket_count,
                                                                    peering_crush_bucket_target ,
                                                                    peering_crush_bucket_barrier,
                                                                    peering_crush_mandatory_member
                                                                    ));
    }
  }

  bool stretch_set_can_peer(const std::set<int>& want, const OSDMap& osdmap,
			    std::ostream *out) const;
  bool stretch_set_can_peer(const std::vector<int>& want, const OSDMap& osdmap,
			    std::ostream *out) const {
    if (!is_stretch_pool()) return true;
    std::set<int> swant;
    for (auto i : want) swant.insert(i);
    return stretch_set_can_peer(swant, osdmap, out);
  }

  uint64_t target_max_bytes = 0;   ///< tiering: target max pool size
  uint64_t target_max_objects = 0; ///< tiering: target max pool size

  uint32_t cache_target_dirty_ratio_micro = 0; ///< cache: fraction of target to leave dirty
  uint32_t cache_target_dirty_high_ratio_micro = 0; ///< cache: fraction of  target to flush with high speed
  uint32_t cache_target_full_ratio_micro = 0;  ///< cache: fraction of target to fill before we evict in earnest

  uint32_t cache_min_flush_age = 0;  ///< minimum age (seconds) before we can flush
  uint32_t cache_min_evict_age = 0;  ///< minimum age (seconds) before we can evict

  HitSet::Params hit_set_params; ///< The HitSet params to use on this pool
  uint32_t hit_set_period = 0;   ///< periodicity of HitSet segments (seconds)
  uint32_t hit_set_count = 0;    ///< number of periods to retain
  bool use_gmt_hitset = true;	 ///< use gmt to name the hitset archive object
  uint32_t min_read_recency_for_promote = 0;   ///< minimum number of HitSet to check before promote on read
  uint32_t min_write_recency_for_promote = 0;  ///< minimum number of HitSet to check before promote on write
  uint32_t hit_set_grade_decay_rate = 0; ///< current hit_set has highest priority on objects
                                         ///< temperature count,the follow hit_set's priority decay
                                         ///< by this params than pre hit_set
  uint32_t hit_set_search_last_n = 0;    ///< accumulate atmost N hit_sets for temperature

  uint32_t stripe_width = 0;        ///< erasure coded stripe size in bytes

  uint64_t expected_num_objects = 0; ///< expected number of objects on this pool, a value of 0 indicates
                                     ///< user does not specify any expected value
  bool fast_read = false;            ///< whether turn on fast read on the pool or not
  shard_id_set nonprimary_shards; ///< EC partial writes: shards that cannot become a primary
  pool_opts_t opts; ///< options

  typedef enum {
    TYPE_FINGERPRINT_NONE = 0,
    TYPE_FINGERPRINT_SHA1 = 1,     
    TYPE_FINGERPRINT_SHA256 = 2,     
    TYPE_FINGERPRINT_SHA512 = 3,     
  } fingerprint_t;
  static fingerprint_t get_fingerprint_from_str(const std::string& s) {
    if (s == "none")
      return TYPE_FINGERPRINT_NONE;
    if (s == "sha1")
      return TYPE_FINGERPRINT_SHA1;
    if (s == "sha256")
      return TYPE_FINGERPRINT_SHA256;
    if (s == "sha512")
      return TYPE_FINGERPRINT_SHA512;
    return (fingerprint_t)-1;
  }
  const fingerprint_t get_fingerprint_type() const {
    std::string fp_str;
    opts.get(pool_opts_t::FINGERPRINT_ALGORITHM, &fp_str);
    return get_fingerprint_from_str(fp_str);
  }
  const char *get_fingerprint_name() const {
    std::string fp_str;
    fingerprint_t fp_t;
    opts.get(pool_opts_t::FINGERPRINT_ALGORITHM, &fp_str);
    fp_t = get_fingerprint_from_str(fp_str);
    return get_fingerprint_name(fp_t);
  }
  static const char *get_fingerprint_name(fingerprint_t m) {
    switch (m) {
    case TYPE_FINGERPRINT_NONE: return "none";
    case TYPE_FINGERPRINT_SHA1: return "sha1";
    case TYPE_FINGERPRINT_SHA256: return "sha256";
    case TYPE_FINGERPRINT_SHA512: return "sha512";
    default: return "unknown";
    }
  }

  typedef enum {
    TYPE_DEDUP_CHUNK_NONE = 0,
    TYPE_DEDUP_CHUNK_FASTCDC = 1,     
    TYPE_DEDUP_CHUNK_FIXEDCDC = 2,     
  } dedup_chunk_algo_t;
  static dedup_chunk_algo_t get_dedup_chunk_algorithm_from_str(const std::string& s) {
    if (s == "none")
      return TYPE_DEDUP_CHUNK_NONE;
    if (s == "fastcdc")
      return TYPE_DEDUP_CHUNK_FASTCDC;
    if (s == "fixed")
      return TYPE_DEDUP_CHUNK_FIXEDCDC;
    return (dedup_chunk_algo_t)-1;
  }
  const dedup_chunk_algo_t get_dedup_chunk_algorithm_type() const {
    std::string algo_str;
    opts.get(pool_opts_t::DEDUP_CHUNK_ALGORITHM, &algo_str);
    return get_dedup_chunk_algorithm_from_str(algo_str);
  }
  const char *get_dedup_chunk_algorithm_name() const {
    std::string dedup_chunk_algo_str;
    dedup_chunk_algo_t dedup_chunk_algo_t;
    opts.get(pool_opts_t::DEDUP_CHUNK_ALGORITHM, &dedup_chunk_algo_str);
    dedup_chunk_algo_t = get_dedup_chunk_algorithm_from_str(dedup_chunk_algo_str);
    return get_dedup_chunk_algorithm_name(dedup_chunk_algo_t);
  }
  static const char *get_dedup_chunk_algorithm_name(dedup_chunk_algo_t m) {
    switch (m) {
    case TYPE_DEDUP_CHUNK_NONE: return "none";
    case TYPE_DEDUP_CHUNK_FASTCDC: return "fastcdc";
    case TYPE_DEDUP_CHUNK_FIXEDCDC: return "fixed";
    default: return "unknown";
    }
  }

  int64_t get_dedup_tier() const {
    int64_t tier_id = 0;
    opts.get(pool_opts_t::DEDUP_TIER, &tier_id);
    return tier_id;
  }
  int64_t get_dedup_cdc_chunk_size() const {
    int64_t chunk_size = 0;
    opts.get(pool_opts_t::DEDUP_CDC_CHUNK_SIZE, &chunk_size);
    return chunk_size;
  }

  /// application -> key/value metadata
  std::map<std::string, std::map<std::string, std::string>> application_metadata;

private:
  std::vector<uint32_t> grade_table;
  std::vector<shard_id_t> shard_mapping; // Used by EC direct reads.

public:
  uint32_t get_grade(unsigned i) const {
    if (grade_table.size() <= i)
      return 0;
    return grade_table[i];
  }
  void calc_grade_table() {
    unsigned v = 1000000;
    grade_table.resize(hit_set_count);
    for (unsigned i = 0; i < hit_set_count; i++) {
      v = v * (1 - (hit_set_grade_decay_rate / 100.0));
      grade_table[i] = v;
    }
  }

  pg_pool_t() = default;

  // When `crush` is non-null, `show_rule_names` is true, and the
  // pool's crush rule still exists, an extra `crush_rule_name`
  // string is emitted alongside the (always-int) `crush_rule`
  // field. Defaults preserve the existing JSON schema.
  void dump(ceph::Formatter *f,
            const CrushWrapper *crush = nullptr,
            bool show_rule_names = false) const;

  // Renders the pool fields used by `pg_pool_t`'s stream operator
  // (which delegates here with `crush == nullptr`). When `crush`
  // is non-null and the pool's rule still exists, `crush_rule` is
  // rendered as the rule name instead of the numeric id; otherwise
  // the numeric id is used so the output is never misleading.
  void print(std::ostream& out, const CrushWrapper *crush = nullptr) const;

  const utime_t &get_create_time() const { return create_time; }
  uint64_t get_flags() const { return flags; }
  bool has_flag(uint64_t f) const { return flags & f; }
  void set_flag(uint64_t f) { flags |= f; }
  void unset_flag(uint64_t f) { flags &= ~f; }

  bool require_rollback() const {
    return is_erasure();
  }

  /// true if incomplete clones may be present
  bool allow_incomplete_clones() const {
    return cache_mode != CACHEMODE_NONE || has_flag(FLAG_INCOMPLETE_CLONES);
  }

  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  unsigned get_min_size() const { return min_size; }
  int get_crush_rule() const { return crush_rule; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  epoch_t get_last_force_op_resend() const { return last_force_op_resend; }
  epoch_t get_last_force_op_resend_prenautilus() const {
    return last_force_op_resend_prenautilus;
  }
  epoch_t get_last_force_op_resend_preluminous() const {
    return last_force_op_resend_preluminous;
  }
  epoch_t get_snap_epoch() const { return snap_epoch; }
  snapid_t get_snap_seq() const { return snap_seq; }
  uint64_t get_auid() const { return auid; }

  uint8_t get_ec_data_shard_count() const {
    return ec_data_shard_count.value_or(nonprimary_shards.size() + 1);
  }

  void set_snap_seq(snapid_t s) { snap_seq = s; }
  void set_snap_epoch(epoch_t e) { snap_epoch = e; }

  void set_stripe_width(uint32_t s) { stripe_width = s; }
  uint32_t get_stripe_width() const { return stripe_width; }

  bool is_replicated()   const { return get_type() == TYPE_REPLICATED; }
  bool is_erasure() const { return get_type() == TYPE_ERASURE; }

  bool supports_omap() const {
    return has_flag(FLAG_OMAP) || is_replicated();
  }

  bool requires_aligned_append() const {
    return is_erasure() && !has_flag(FLAG_EC_OVERWRITES);
  }
  uint64_t required_alignment() const { return stripe_width; }

  bool allows_ecoverwrites() const {
    return has_flag(FLAG_EC_OVERWRITES);
  }

  bool allows_ecoptimizations() const {
    return has_flag(FLAG_EC_OPTIMIZATIONS);
  }

  bool allows_nonprimary_reads() const {
    return !is_tier() && !has_tiers() && (get_dedup_tier() <= 0);
  }

  bool is_crimson() const {
    return has_flag(FLAG_CRIMSON);
  }

  bool can_shift_osds() const {
    switch (get_type()) {
    case TYPE_REPLICATED:
      return true;
    case TYPE_ERASURE:
      return false;
    default:
      ceph_abort_msg("unhandled pool type");
    }
  }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }
  unsigned get_pg_num_target() const { return pg_num_target; }
  unsigned get_pgp_num_target() const { return pgp_num_target; }
  unsigned get_pg_num_pending() const { return pg_num_pending; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

  // if pg_num is not a multiple of two, pgs are not equally sized.
  // return, for a given pg, the fraction (denominator) of the total
  // pool size that it represents.
  unsigned get_pg_num_divisor(pg_t pgid) const;

  bool is_pending_merge(pg_t pgid, bool *target) const;

  void set_pg_num(int p) {
    pg_num = p;
    pg_num_pending = p;
    calc_pg_masks();
  }
  void set_pgp_num(int p) {
    pgp_num = p;
    calc_pg_masks();
  }
  void set_pg_num_pending(int p) {
    pg_num_pending = p;
    calc_pg_masks();
  }
  void set_pg_num_target(int p) {
    pg_num_target = p;
  }
  void set_pgp_num_target(int p) {
    pgp_num_target = p;
  }
  void dec_pg_num(pg_t source_pgid,
		  epoch_t ready_epoch,
		  eversion_t source_version,
		  eversion_t target_version,
		  epoch_t last_epoch_started,
		  epoch_t last_epoch_clean) {
    --pg_num;
    last_pg_merge_meta.source_pgid = source_pgid;
    last_pg_merge_meta.ready_epoch = ready_epoch;
    last_pg_merge_meta.source_version = source_version;
    last_pg_merge_meta.target_version = target_version;
    last_pg_merge_meta.last_epoch_started = last_epoch_started;
    last_pg_merge_meta.last_epoch_clean = last_epoch_clean;
    calc_pg_masks();
  }

  void set_quota_max_bytes(uint64_t m) {
    quota_max_bytes = m;
  }
  uint64_t get_quota_max_bytes() {
    return quota_max_bytes;
  }

  void set_quota_max_objects(uint64_t m) {
    quota_max_objects = m;
  }
  uint64_t get_quota_max_objects() {
    return quota_max_objects;
  }

  void set_last_force_op_resend(uint64_t t) {
    last_force_op_resend = t;
    last_force_op_resend_prenautilus = t;
    last_force_op_resend_preluminous = t;
  }

  void calc_pg_masks();

  /*
   * we have two snap modes:
   *  - pool global snaps
   *    - snap existence/non-existence defined by snaps[] and snap_seq
   *  - user managed snaps
   *    - removal governed by removed_snaps
   *
   * we know which mode we're using based on whether removed_snaps is empty.
   * If nothing has been created, both functions report false.
   */
  bool is_pool_snaps_mode() const;
  bool is_unmanaged_snaps_mode() const;
  bool is_removed_snap(snapid_t s) const;

  snapid_t snap_exists(std::string_view s) const;
  void add_snap(const char *n, utime_t stamp);
  uint64_t add_unmanaged_snap(bool preoctopus_compat);
  void remove_snap(snapid_t s);
  void remove_unmanaged_snap(snapid_t s, bool preoctopus_compat);

  SnapContext get_snap_context() const;

  /// hash a object name+namespace key to a hash position
  uint32_t hash_key(const std::string& key, const std::string& ns) const;

  /// round a hash position down to a pg num
  uint32_t raw_hash_to_pg(uint32_t v) const;

  /*
   * map a raw pg (with full precision ps) into an actual pg, for storage
   */
  pg_t raw_pg_to_pg(pg_t pg) const;
  
  /*
   * map raw pg (full precision ps) into a placement seed.  include
   * pool id in that value so that different pools don't use the same
   * seeds.
   */
  ps_t raw_pg_to_pps(pg_t pg) const;

  /// choose a random hash position within a pg
  uint32_t get_random_pg_position(pg_t pgid, uint32_t seed) const;

  /// EC partial writes: test if a shard is a non-primary
  bool is_nonprimary_shard(const shard_id_t shard) const {
    return !nonprimary_shards.empty() && nonprimary_shards.contains(shard);
  }

  void set_shard_mapping(std::vector<shard_id_t> && mapping) {
    shard_mapping = mapping;
  }

  shard_id_t get_shard(raw_shard_id_t raw) const {
    if (shard_mapping.empty()) {
      return shard_id_t((int)raw);
    }
    if (std::cmp_less((int)raw, shard_mapping.size())) {
      return shard_mapping[(int)raw];
    } else {
      return shard_id_t::NO_SHARD;
    }
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);

  static std::list<pg_pool_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

std::ostream& operator<<(std::ostream& out, const pg_pool_t& p);

struct pool_pg_num_history_t {
  /// last epoch updated
  epoch_t epoch = 0;
  /// poolid -> epoch -> pg_num
  std::map<int64_t, std::map<epoch_t,uint32_t>> pg_nums;
  /// pair(epoch, poolid)
  std::set<std::pair<epoch_t,int64_t>> deleted_pools;

  void log_pg_num_change(epoch_t epoch, int64_t pool, uint32_t pg_num) {
    pg_nums[pool][epoch] = pg_num;
  }
  void log_pool_delete(epoch_t epoch, int64_t pool) {
    deleted_pools.insert(std::make_pair(epoch, pool));
  }

  /// prune history based on oldest osdmap epoch in the cluster
  void prune(epoch_t oldest_epoch) {
    auto i = deleted_pools.begin();
    while (i != deleted_pools.end()) {
      if (i->first >= oldest_epoch) {
	break;
      }
      pg_nums.erase(i->second);
      i = deleted_pools.erase(i);
    }
    for (auto& j : pg_nums) {
      auto k = j.second.lower_bound(oldest_epoch);
      // keep this and the entry before it (just to be paranoid)
      if (k != j.second.begin()) {
	--k;
	j.second.erase(j.second.begin(), k);
      }
    }
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(epoch, bl);
    encode(pg_nums, bl);
    encode(deleted_pools, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(epoch, p);
    decode(pg_nums, p);
    decode(deleted_pools, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("epoch", epoch);
    f->open_object_section("pools");
    for (auto& i : pg_nums) {
      f->open_object_section("pool");
      f->dump_unsigned("pool_id", i.first);
      f->open_array_section("changes");
      for (auto& j : i.second) {
	f->open_object_section("change");
	f->dump_unsigned("epoch", j.first);
	f->dump_unsigned("pg_num", j.second);
	f->close_section();
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
    f->open_array_section("deleted_pools");
    for (auto& i : deleted_pools) {
      f->open_object_section("deletion");
      f->dump_unsigned("pool_id", i.second);
      f->dump_unsigned("epoch", i.first);
      f->close_section();
    }
    f->close_section();
  }
  static std::list<pool_pg_num_history_t> generate_test_instances() {
    std::list<pool_pg_num_history_t> ls;
    ls.emplace_back();
    return ls;
  }
  friend std::ostream& operator<<(std::ostream& out, const pool_pg_num_history_t& h) {
    return out << "pg_num_history(e" << h.epoch
	       << " pg_nums " << h.pg_nums
	       << " deleted_pools " << h.deleted_pools
	       << ")";
  }
};
WRITE_CLASS_ENCODER(pool_pg_num_history_t)

#endif // CEPH_OSD_TYPES_POOL_H
