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

#ifndef CEPH_OSD_TYPES_STATS_H
#define CEPH_OSD_TYPES_STATS_H

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

#include "common/histogram.h"
#include "osd/osd_types_core.h"

/**
 * objectstore_perf_stat_t
 *
 * current perf information about the osd
 */
struct objectstore_perf_stat_t {
  // cur_op_latency is in ns since double add/sub are not associative
  uint64_t os_commit_latency_ns;
  uint64_t os_apply_latency_ns;

  objectstore_perf_stat_t() :
    os_commit_latency_ns(0), os_apply_latency_ns(0) {}

  bool operator==(const objectstore_perf_stat_t &r) const {
    return os_commit_latency_ns == r.os_commit_latency_ns &&
      os_apply_latency_ns == r.os_apply_latency_ns;
  }

  void add(const objectstore_perf_stat_t &o) {
    os_commit_latency_ns += o.os_commit_latency_ns;
    os_apply_latency_ns += o.os_apply_latency_ns;
  }
  void sub(const objectstore_perf_stat_t &o) {
    os_commit_latency_ns -= o.os_commit_latency_ns;
    os_apply_latency_ns -= o.os_apply_latency_ns;
  }
  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static std::list<objectstore_perf_stat_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(objectstore_perf_stat_t)

/*
 * pg states
 */
#define PG_STATE_CREATING           (1ULL << 0)  // creating
#define PG_STATE_ACTIVE             (1ULL << 1)  // i am active.  (primary: replicas too)
#define PG_STATE_CLEAN              (1ULL << 2)  // peers are complete, clean of stray replicas.
#define PG_STATE_DOWN               (1ULL << 4)  // a needed replica is down, PG offline
#define PG_STATE_RECOVERY_UNFOUND   (1ULL << 5)  // recovery stopped due to unfound
#define PG_STATE_BACKFILL_UNFOUND   (1ULL << 6)  // backfill stopped due to unfound
#define PG_STATE_PREMERGE           (1ULL << 7)  // i am prepare to merging
#define PG_STATE_SCRUBBING          (1ULL << 8)  // scrubbing
//#define PG_STATE_SCRUBQ           (1ULL << 9)  // queued for scrub
#define PG_STATE_DEGRADED           (1ULL << 10) // pg contains objects with reduced redundancy
#define PG_STATE_INCONSISTENT       (1ULL << 11) // pg replicas are inconsistent (but shouldn't be)
#define PG_STATE_PEERING            (1ULL << 12) // pg is (re)peering
#define PG_STATE_REPAIR             (1ULL << 13) // pg should repair on next scrub
#define PG_STATE_RECOVERING         (1ULL << 14) // pg is recovering/migrating objects
#define PG_STATE_BACKFILL_WAIT      (1ULL << 15) // [active] reserving backfill
#define PG_STATE_INCOMPLETE         (1ULL << 16) // incomplete content, peering failed.
#define PG_STATE_STALE              (1ULL << 17) // our state for this pg is stale, unknown.
#define PG_STATE_REMAPPED           (1ULL << 18) // pg is explicitly remapped to different OSDs than CRUSH
#define PG_STATE_DEEP_SCRUB         (1ULL << 19) // deep scrub: check CRC32 on files
#define PG_STATE_BACKFILLING        (1ULL << 20) // [active] backfilling pg content
#define PG_STATE_BACKFILL_TOOFULL   (1ULL << 21) // backfill can't proceed: too full
#define PG_STATE_RECOVERY_WAIT      (1ULL << 22) // waiting for recovery reservations
#define PG_STATE_UNDERSIZED         (1ULL << 23) // pg acting < pool size
#define PG_STATE_ACTIVATING         (1ULL << 24) // pg is peered but not yet active
#define PG_STATE_PEERED             (1ULL << 25) // peered, cannot go active, can recover
#define PG_STATE_SNAPTRIM           (1ULL << 26) // trimming snaps
#define PG_STATE_SNAPTRIM_WAIT      (1ULL << 27) // queued to trim snaps
#define PG_STATE_RECOVERY_TOOFULL   (1ULL << 28) // recovery can't proceed: too full
#define PG_STATE_SNAPTRIM_ERROR     (1ULL << 29) // error stopped trimming snaps
#define PG_STATE_FORCED_RECOVERY    (1ULL << 30) // force recovery of this pg before any other
#define PG_STATE_FORCED_BACKFILL    (1ULL << 31) // force backfill of this pg before any other
#define PG_STATE_FAILED_REPAIR      (1ULL << 32) // A repair failed to fix all errors
#define PG_STATE_LAGGY              (1ULL << 33) // PG is laggy/unreabable due to slow/delayed pings
#define PG_STATE_WAIT               (1ULL << 34) // PG is waiting for prior intervals' readable period to expire

std::string pg_state_string(uint64_t state);
std::string pg_vector_string(const std::vector<int32_t> &a);
std::optional<uint64_t> pg_string_state(const std::string& state);



/**
 * a summation of object stats
 *
 * This is just a container for object stats; we don't know what for.
 *
 * If you add members in object_stat_sum_t, you should make sure there are
 * not padding among these members.
 * You should also modify the padding_check function.

 */
struct object_stat_sum_t {
  /**************************************************************************
   * WARNING: be sure to update operator==, floor, and split when
   * adding/removing fields!
   **************************************************************************/
  int64_t num_bytes{0};    // in bytes
  int64_t num_objects{0};
  int64_t num_object_clones{0};
  int64_t num_object_copies{0};  // num_objects * num_replicas
  int64_t num_objects_missing_on_primary{0};
  int64_t num_objects_degraded{0};
  int64_t num_objects_unfound{0};
  int64_t num_rd{0};
  int64_t num_rd_kb{0};
  int64_t num_wr{0};
  int64_t num_wr_kb{0};
  int64_t num_scrub_errors{0};	// total deep and shallow scrub errors
  int64_t num_objects_recovered{0};
  int64_t num_bytes_recovered{0};
  int64_t num_keys_recovered{0};
  int64_t num_shallow_scrub_errors{0};
  int64_t num_deep_scrub_errors{0};
  int64_t num_objects_dirty{0};
  int64_t num_whiteouts{0};
  int64_t num_objects_omap{0};
  int64_t num_objects_hit_set_archive{0};
  int64_t num_objects_misplaced{0};
  int64_t num_bytes_hit_set_archive{0};
  int64_t num_flush{0};
  int64_t num_flush_kb{0};
  int64_t num_evict{0};
  int64_t num_evict_kb{0};
  int64_t num_promote{0};
  int32_t num_flush_mode_high{0};  // 1 when in high flush mode, otherwise 0
  int32_t num_flush_mode_low{0};   // 1 when in low flush mode, otherwise 0
  int32_t num_evict_mode_some{0};  // 1 when in evict some mode, otherwise 0
  int32_t num_evict_mode_full{0};  // 1 when in evict full mode, otherwise 0
  int64_t num_objects_pinned{0};
  int64_t num_objects_missing{0};
  int64_t num_legacy_snapsets{0}; ///< upper bound on pre-luminous-style SnapSets
  int64_t num_large_omap_objects{0};
  int64_t num_objects_manifest{0};
  int64_t num_omap_bytes{0};
  int64_t num_omap_keys{0};
  int64_t num_objects_repaired{0};

  object_stat_sum_t() = default;

  void floor(int64_t f) {
#define FLOOR(x) if (x < f) x = f
    FLOOR(num_bytes);
    FLOOR(num_objects);
    FLOOR(num_object_clones);
    FLOOR(num_object_copies);
    FLOOR(num_objects_missing_on_primary);
    FLOOR(num_objects_missing);
    FLOOR(num_objects_degraded);
    FLOOR(num_objects_misplaced);
    FLOOR(num_objects_unfound);
    FLOOR(num_rd);
    FLOOR(num_rd_kb);
    FLOOR(num_wr);
    FLOOR(num_wr_kb);
    FLOOR(num_large_omap_objects);
    FLOOR(num_objects_manifest);
    FLOOR(num_omap_bytes);
    FLOOR(num_omap_keys);
    FLOOR(num_shallow_scrub_errors);
    FLOOR(num_deep_scrub_errors);
    num_scrub_errors = num_shallow_scrub_errors + num_deep_scrub_errors;
    FLOOR(num_objects_recovered);
    FLOOR(num_bytes_recovered);
    FLOOR(num_keys_recovered);
    FLOOR(num_objects_dirty);
    FLOOR(num_whiteouts);
    FLOOR(num_objects_omap);
    FLOOR(num_objects_hit_set_archive);
    FLOOR(num_bytes_hit_set_archive);
    FLOOR(num_flush);
    FLOOR(num_flush_kb);
    FLOOR(num_evict);
    FLOOR(num_evict_kb);
    FLOOR(num_promote);
    FLOOR(num_flush_mode_high);
    FLOOR(num_flush_mode_low);
    FLOOR(num_evict_mode_some);
    FLOOR(num_evict_mode_full);
    FLOOR(num_objects_pinned);
    FLOOR(num_legacy_snapsets);
    FLOOR(num_objects_repaired);
#undef FLOOR
  }

  void split(std::vector<object_stat_sum_t> &out) const {
#define SPLIT(PARAM)                            \
    for (unsigned i = 0; i < out.size(); ++i) { \
      out[i].PARAM = PARAM / out.size();        \
      if (i < (PARAM % out.size())) {           \
	out[i].PARAM++;                         \
      }                                         \
    }
#define SPLIT_PRESERVE_NONZERO(PARAM)		\
    for (unsigned i = 0; i < out.size(); ++i) { \
      if (PARAM)				\
	out[i].PARAM = 1 + PARAM / out.size();	\
      else					\
	out[i].PARAM = 0;			\
    }

    SPLIT(num_bytes);
    SPLIT(num_objects);
    SPLIT(num_object_clones);
    SPLIT(num_object_copies);
    SPLIT(num_objects_missing_on_primary);
    SPLIT(num_objects_missing);
    SPLIT(num_objects_degraded);
    SPLIT(num_objects_misplaced);
    SPLIT(num_objects_unfound);
    SPLIT(num_rd);
    SPLIT(num_rd_kb);
    SPLIT(num_wr);
    SPLIT(num_wr_kb);
    SPLIT(num_large_omap_objects);
    SPLIT(num_objects_manifest);
    SPLIT(num_omap_bytes);
    SPLIT(num_omap_keys);
    SPLIT(num_objects_repaired);
    SPLIT_PRESERVE_NONZERO(num_shallow_scrub_errors);
    SPLIT_PRESERVE_NONZERO(num_deep_scrub_errors);
    for (unsigned i = 0; i < out.size(); ++i) {
      out[i].num_scrub_errors = out[i].num_shallow_scrub_errors +
				out[i].num_deep_scrub_errors;
    }
    SPLIT(num_objects_recovered);
    SPLIT(num_bytes_recovered);
    SPLIT(num_keys_recovered);
    SPLIT(num_objects_dirty);
    SPLIT(num_whiteouts);
    SPLIT(num_objects_omap);
    SPLIT(num_objects_hit_set_archive);
    SPLIT(num_bytes_hit_set_archive);
    SPLIT(num_flush);
    SPLIT(num_flush_kb);
    SPLIT(num_evict);
    SPLIT(num_evict_kb);
    SPLIT(num_promote);
    SPLIT(num_flush_mode_high);
    SPLIT(num_flush_mode_low);
    SPLIT(num_evict_mode_some);
    SPLIT(num_evict_mode_full);
    SPLIT(num_objects_pinned);
    SPLIT_PRESERVE_NONZERO(num_legacy_snapsets);
#undef SPLIT
#undef SPLIT_PRESERVE_NONZERO
  }

  void clear() {
    // FIPS zeroization audit 20191117: this memset is not security related.
    memset(this, 0, sizeof(*this));
  }

  void calc_copies(int nrep) {
    num_object_copies = nrep * num_objects;
  }

  bool is_zero() const {
    return mem_is_zero((char*)this, sizeof(*this));
  }

  void add(const object_stat_sum_t& o);
  void sub(const object_stat_sum_t& o);

  void dump(ceph::Formatter *f) const;
  void padding_check() {
    static_assert(
      sizeof(object_stat_sum_t) ==
        sizeof(num_bytes) +
        sizeof(num_objects) +
        sizeof(num_object_clones) +
        sizeof(num_object_copies) +
        sizeof(num_objects_missing_on_primary) +
        sizeof(num_objects_degraded) +
        sizeof(num_objects_unfound) +
        sizeof(num_rd) +
        sizeof(num_rd_kb) +
        sizeof(num_wr) +
        sizeof(num_wr_kb) +
        sizeof(num_scrub_errors) +
        sizeof(num_large_omap_objects) +
        sizeof(num_objects_manifest) +
        sizeof(num_omap_bytes) +
        sizeof(num_omap_keys) +
        sizeof(num_objects_repaired) +
        sizeof(num_objects_recovered) +
        sizeof(num_bytes_recovered) +
        sizeof(num_keys_recovered) +
        sizeof(num_shallow_scrub_errors) +
        sizeof(num_deep_scrub_errors) +
        sizeof(num_objects_dirty) +
        sizeof(num_whiteouts) +
        sizeof(num_objects_omap) +
        sizeof(num_objects_hit_set_archive) +
        sizeof(num_objects_misplaced) +
        sizeof(num_bytes_hit_set_archive) +
        sizeof(num_flush) +
        sizeof(num_flush_kb) +
        sizeof(num_evict) +
        sizeof(num_evict_kb) +
        sizeof(num_promote) +
        sizeof(num_flush_mode_high) +
        sizeof(num_flush_mode_low) +
        sizeof(num_evict_mode_some) +
        sizeof(num_evict_mode_full) +
        sizeof(num_objects_pinned) +
        sizeof(num_objects_missing) +
        sizeof(num_legacy_snapsets)
      ,
      "object_stat_sum_t have padding");
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static std::list<object_stat_sum_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(object_stat_sum_t)

bool operator==(const object_stat_sum_t& l, const object_stat_sum_t& r);

/**
 * a collection of object stat sums
 *
 * This is a collection of stat sums over different categories.
 */
struct object_stat_collection_t {
  /**************************************************************************
   * WARNING: be sure to update the operator== when adding/removing fields! *
   **************************************************************************/
  object_stat_sum_t sum;

  void calc_copies(int nrep) {
    sum.calc_copies(nrep);
  }

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static std::list<object_stat_collection_t> generate_test_instances();

  bool is_zero() const {
    return sum.is_zero();
  }

  void clear() {
    sum.clear();
  }

  void floor(int64_t f) {
    sum.floor(f);
  }

  void add(const object_stat_sum_t& o) {
    sum.add(o);
  }

  void add(const object_stat_collection_t& o) {
    sum.add(o.sum);
  }
  void sub(const object_stat_collection_t& o) {
    sum.sub(o.sum);
  }
};
WRITE_CLASS_ENCODER(object_stat_collection_t)

inline bool operator==(const object_stat_collection_t& l,
		       const object_stat_collection_t& r) {
  return l.sum == r.sum;
}

enum class scrub_level_t : bool { shallow = false, deep = true };
enum class scrub_type_t : bool { not_repair = false, do_repair = true };

/// is there a scrub in our future?
enum class pg_scrub_sched_status_t : uint16_t {
  unknown,         ///< status not reported yet
  not_queued,	   ///< not in the OSD's scrub queue. Probably not active.
  active,          ///< scrubbing
  scheduled,	   ///< scheduled for a scrub at an already determined time
  queued,	   ///< queued to be scrubbed
  blocked	   ///< blocked waiting for objects to be unlocked
};

struct pg_scrubbing_status_t {
  utime_t m_scheduled_at{};
  int32_t m_duration_seconds{0}; // relevant when scrubbing
  pg_scrub_sched_status_t m_sched_status{pg_scrub_sched_status_t::unknown};
  bool m_is_active{false};
  scrub_level_t m_is_deep{scrub_level_t::shallow};
  bool m_is_periodic{true};
  // the following are only relevant when we are reserving replicas:
  uint16_t m_osd_to_respond{0};
  /// this is the n'th replica we are reserving (out of m_num_to_reserve)
  uint8_t m_ordinal_of_requested_replica{0};
  /// the number of replicas we are reserving for scrubbing. 0 means we are not
  /// in the process of reserving replicas.
  uint8_t m_num_to_reserve{0};
};

bool operator==(const pg_scrubbing_status_t& l, const pg_scrubbing_status_t& r);

/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  /**************************************************************************
   * WARNING: be sure to update the operator== when adding/removing fields! *
   **************************************************************************/
  eversion_t version;
  version_t reported_seq;  // sequence number
  epoch_t reported_epoch;  // epoch of this report
  uint64_t state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;  // state & PG_STATE_ACTIVE
  utime_t last_peered;  // state & PG_STATE_ACTIVE || state & PG_STATE_PEERED
  utime_t last_clean;   // state & PG_STATE_CLEAN
  utime_t last_degraded; // state & (PG_STATE_DEGRADED | PG_STATE_UNDERSIZED)
  utime_t last_unstale; // (state & PG_STATE_STALE) == 0
  utime_t last_undegraded; // (state & PG_STATE_DEGRADED) == 0
  utime_t last_fullsized; // (state & PG_STATE_UNDERSIZED) == 0

  eversion_t log_start;         // (log_start,version]
  eversion_t ondisk_log_start;  // there may be more on disk

  epoch_t created;
  epoch_t last_epoch_clean;
  pg_t parent;
  __u32 parent_split_bits;

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;
  int32_t last_scrub_duration{0};

  object_stat_collection_t stats;

  int64_t log_size;
  int64_t log_dups_size;
  int64_t ondisk_log_size;    // >= active_log_size
  int64_t objects_scrubbed;
  double scrub_duration;

  std::vector<int32_t> up, acting;
  std::vector<pg_shard_t> avail_no_missing;
  std::map< std::set<pg_shard_t>, int32_t > object_location_counts;
  epoch_t mapping_epoch;

  std::vector<int32_t> blocked_by;  ///< osds on which the pg is blocked

  interval_set<snapid_t> purged_snaps;  ///< recently removed snaps that we've purged

  utime_t last_became_active;
  utime_t last_became_peered;

  /// up, acting primaries
  int32_t up_primary;
  int32_t acting_primary;

  // snaptrimq.size() is 64bit, but let's be serious - anything over 50k is
  // absurd already, so cap it to 2^32 and save 4 bytes at the same time
  uint32_t snaptrimq_len;
  int64_t objects_trimmed;
  double snaptrim_duration;

  pg_scrubbing_status_t scrub_sched_status;

  bool stats_invalid:1;
  /// true if num_objects_dirty is not accurate (because it was not
  /// maintained starting from pool creation)
  bool dirty_stats_invalid:1;
  bool omap_stats_invalid:1;
  bool hitset_stats_invalid:1;
  bool hitset_bytes_stats_invalid:1;
  bool pin_stats_invalid:1;
  bool manifest_stats_invalid:1;

  pg_stat_t()
    : reported_seq(0),
      reported_epoch(0),
      state(0),
      created(0), last_epoch_clean(0),
      parent_split_bits(0),
      log_size(0), log_dups_size(0),
      ondisk_log_size(0),
      objects_scrubbed(0),
      scrub_duration(0),
      mapping_epoch(0),
      up_primary(-1),
      acting_primary(-1),
      snaptrimq_len(0),
      objects_trimmed(0),
      snaptrim_duration(0.0),
      stats_invalid(false),
      dirty_stats_invalid(false),
      omap_stats_invalid(false),
      hitset_stats_invalid(false),
      hitset_bytes_stats_invalid(false),
      pin_stats_invalid(false),
      manifest_stats_invalid(false)
  { }

  epoch_t get_effective_last_epoch_clean() const {
    if (state & PG_STATE_CLEAN) {
      // we are clean as of this report, and should thus take the
      // reported epoch
      return reported_epoch;
    } else {
      return last_epoch_clean;
    }
  }

  std::pair<epoch_t, version_t> get_version_pair() const {
    return { reported_epoch, reported_seq };
  }

  void floor(int64_t f) {
    stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
    if (snaptrimq_len < f)
      snaptrimq_len = f;
  }

  void add_sub_invalid_flags(const pg_stat_t& o) {
    // adding (or subtracting!) invalid stats render our stats invalid too
    stats_invalid |= o.stats_invalid;
    dirty_stats_invalid |= o.dirty_stats_invalid;
    omap_stats_invalid |= o.omap_stats_invalid;
    hitset_stats_invalid |= o.hitset_stats_invalid;
    hitset_bytes_stats_invalid |= o.hitset_bytes_stats_invalid;
    pin_stats_invalid |= o.pin_stats_invalid;
    manifest_stats_invalid |= o.manifest_stats_invalid;
  }
  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    log_dups_size += o.log_dups_size;
    ondisk_log_size += o.ondisk_log_size;
    snaptrimq_len = std::min((uint64_t)snaptrimq_len + o.snaptrimq_len,
                             (uint64_t)(1ull << 31));
    add_sub_invalid_flags(o);
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    log_dups_size -= o.log_dups_size;
    ondisk_log_size -= o.ondisk_log_size;
    if (o.snaptrimq_len < snaptrimq_len) {
      snaptrimq_len -= o.snaptrimq_len;
    } else {
      snaptrimq_len = 0;
    }
    add_sub_invalid_flags(o);
  }

  bool is_acting_osd(int32_t osd, bool primary) const;
  void dump(ceph::Formatter *f) const;
  void dump_brief(ceph::Formatter *f) const;
  std::string dump_scrub_schedule() const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static std::list<pg_stat_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_stat_t)

bool operator==(const pg_stat_t& l, const pg_stat_t& r);

/** store_statfs_t
 * ObjectStore full statfs information
 */
struct store_statfs_t
{
  uint64_t total = 0;                  ///< Total bytes
  uint64_t available = 0;              ///< Free bytes available
  uint64_t internally_reserved = 0;    ///< Bytes reserved for internal purposes

  int64_t allocated = 0;               ///< Bytes allocated by the store

  int64_t data_stored = 0;                ///< Bytes actually stored by the user
  int64_t data_compressed = 0;            ///< Bytes stored after compression
  int64_t data_compressed_allocated = 0;  ///< Bytes allocated for compressed data
  int64_t data_compressed_original = 0;   ///< Bytes that were compressed

  int64_t omap_allocated = 0;         ///< approx usage of omap data
  int64_t internal_metadata = 0;      ///< approx usage of internal metadata

  void reset() {
    *this = store_statfs_t();
  }
  void floor(int64_t f) {
#define FLOOR(x) if (int64_t(x) < f) x = f
    FLOOR(total);
    FLOOR(available);
    FLOOR(internally_reserved);
    FLOOR(allocated);
    FLOOR(data_stored);
    FLOOR(data_compressed);
    FLOOR(data_compressed_allocated);
    FLOOR(data_compressed_original);

    FLOOR(omap_allocated);
    FLOOR(internal_metadata);
#undef FLOOR
  }

  bool operator ==(const store_statfs_t& other) const;
  bool is_zero() const {
    return *this == store_statfs_t();
  }

  uint64_t get_used() const {
    return total - available - internally_reserved;
  }

  // this accumulates both actually used and statfs's internally_reserved
  uint64_t get_used_raw() const {
    return total - available;
  }

  float get_used_raw_ratio() const {
    if (total) {
      return (float)get_used_raw() / (float)total;
    } else {
      return 0.0;
    }
  }

  // helpers to ease legacy code porting
  uint64_t kb_avail() const {
    return available >> 10;
  }
  uint64_t kb() const {
    return total >> 10;
  }
  uint64_t kb_used() const {
    return (total - available - internally_reserved) >> 10;
  }
  uint64_t kb_used_raw() const {
    return get_used_raw() >> 10;
  }

  uint64_t kb_used_data() const {
    return allocated >> 10;
  }
  uint64_t kb_used_omap() const {
    return omap_allocated >> 10;
  }

  uint64_t kb_used_internal_metadata() const {
    return internal_metadata >> 10;
  }

  void add(const store_statfs_t& o) {
    total += o.total;
    available += o.available;
    internally_reserved += o.internally_reserved;
    allocated += o.allocated;
    data_stored += o.data_stored;
    data_compressed += o.data_compressed;
    data_compressed_allocated += o.data_compressed_allocated;
    data_compressed_original += o.data_compressed_original;
    omap_allocated += o.omap_allocated;
    internal_metadata += o.internal_metadata;
  }
  void sub(const store_statfs_t& o) {
    total -= o.total;
    available -= o.available;
    internally_reserved -= o.internally_reserved;
    allocated -= o.allocated;
    data_stored -= o.data_stored;
    data_compressed -= o.data_compressed;
    data_compressed_allocated -= o.data_compressed_allocated;
    data_compressed_original -= o.data_compressed_original;
    omap_allocated -= o.omap_allocated;
    internal_metadata -= o.internal_metadata;
  }
  void dump(ceph::Formatter *f) const;
  DENC(store_statfs_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.total, p);
    denc(v.available, p);
    denc(v.internally_reserved, p);
    denc(v.allocated, p);
    denc(v.data_stored, p);
    denc(v.data_compressed, p);
    denc(v.data_compressed_allocated, p);
    denc(v.data_compressed_original, p);
    denc(v.omap_allocated, p);
    denc(v.internal_metadata, p);
    DENC_FINISH(p);
  }
  static std::list<store_statfs_t> generate_test_instances();
};
WRITE_CLASS_DENC(store_statfs_t)

std::ostream &operator<<(std::ostream &lhs, const store_statfs_t &rhs);

/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  store_statfs_t statfs;
  std::vector<int> hb_peers;
  int32_t snap_trim_queue_len, num_snap_trimming;
  uint64_t num_shards_repaired;

  pow2_hist_t op_queue_age_hist;

  objectstore_perf_stat_t os_perf_stat;
  osd_alerts_t os_alerts;

  epoch_t up_from = 0;
  uint64_t seq = 0;

  uint32_t num_pgs = 0;

  uint32_t num_osds = 0;
  uint32_t num_per_pool_osds = 0;
  uint32_t num_per_pool_omap_osds = 0;

  struct Interfaces {
    uint32_t last_update;  // in seconds
    uint32_t back_pingtime[3];
    uint32_t back_min[3];
    uint32_t back_max[3];
    uint32_t back_last;
    uint32_t front_pingtime[3];
    uint32_t front_min[3];
    uint32_t front_max[3];
    uint32_t front_last;
  };
  std::map<int, Interfaces> hb_pingtime;  ///< map of osd id to Interfaces

  osd_stat_t() : snap_trim_queue_len(0), num_snap_trimming(0),
       num_shards_repaired(0)	{}

 void add(const osd_stat_t& o) {
    statfs.add(o.statfs);
    snap_trim_queue_len += o.snap_trim_queue_len;
    num_snap_trimming += o.num_snap_trimming;
    num_shards_repaired += o.num_shards_repaired;
    op_queue_age_hist.add(o.op_queue_age_hist);
    os_perf_stat.add(o.os_perf_stat);
    num_pgs += o.num_pgs;
    num_osds += o.num_osds;
    num_per_pool_osds += o.num_per_pool_osds;
    num_per_pool_omap_osds += o.num_per_pool_omap_osds;
    for (const auto& a : o.os_alerts) {
      auto& target = os_alerts[a.first];
      for (auto& i : a.second) {
	target.emplace(i.first, i.second);
      }
    }
  }
  void sub(const osd_stat_t& o) {
    statfs.sub(o.statfs);
    snap_trim_queue_len -= o.snap_trim_queue_len;
    num_snap_trimming -= o.num_snap_trimming;
    num_shards_repaired -= o.num_shards_repaired;
    op_queue_age_hist.sub(o.op_queue_age_hist);
    os_perf_stat.sub(o.os_perf_stat);
    num_pgs -= o.num_pgs;
    num_osds -= o.num_osds;
    num_per_pool_osds -= o.num_per_pool_osds;
    num_per_pool_omap_osds -= o.num_per_pool_omap_osds;
    for (const auto& a : o.os_alerts) {
      auto& target = os_alerts[a.first];
      for (auto& i : a.second) {
        target.erase(i.first);
      }
      if (target.empty()) {
	os_alerts.erase(a.first);
      }
    }
  }
  void dump(ceph::Formatter *f, bool with_net = true) const;
  void dump_ping_time(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static std::list<osd_stat_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(osd_stat_t)

inline bool operator==(const osd_stat_t& l, const osd_stat_t& r) {
  return l.statfs == r.statfs &&
    l.snap_trim_queue_len == r.snap_trim_queue_len &&
    l.num_snap_trimming == r.num_snap_trimming &&
    l.num_shards_repaired == r.num_shards_repaired &&
    l.hb_peers == r.hb_peers &&
    l.op_queue_age_hist == r.op_queue_age_hist &&
    l.os_perf_stat == r.os_perf_stat &&
    l.num_pgs == r.num_pgs &&
    l.num_osds == r.num_osds &&
    l.num_per_pool_osds == r.num_per_pool_osds &&
    l.num_per_pool_omap_osds == r.num_per_pool_omap_osds;
}
inline bool operator!=(const osd_stat_t& l, const osd_stat_t& r) {
  return !(l == r);
}

inline std::ostream& operator<<(std::ostream& out, const osd_stat_t& s) {
  return out << "osd_stat(" << s.statfs << ", "
	     << "peers " << s.hb_peers
	     << " op hist " << s.op_queue_age_hist.h
	     << ")";
}

/*
 * summation over an entire pool
 */
struct pool_stat_t {
  object_stat_collection_t stats;
  store_statfs_t store_stats;
  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size
  int32_t up;       ///< number of up replicas or shards
  int32_t acting;   ///< number of acting replicas or shards
  int32_t num_store_stats; ///< amount of store_stats accumulated

  pool_stat_t() : log_size(0), ondisk_log_size(0), up(0), acting(0),
    num_store_stats(0)
  { }

  void floor(int64_t f) {
    stats.floor(f);
    store_stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
    if (up < f)
      up = f;
    if (acting < f)
      acting = f;
    if (num_store_stats < f)
      num_store_stats = f;
  }

  void add(const store_statfs_t& o) {
    store_stats.add(o);
    ++num_store_stats;
  }
  void sub(const store_statfs_t& o) {
    store_stats.sub(o);
    --num_store_stats;
  }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
    up += o.up.size();
    acting += o.acting.size();
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
    up -= o.up.size();
    acting -= o.acting.size();
  }

  bool is_zero() const {
    return (stats.is_zero() &&
            store_stats.is_zero() &&
	    log_size == 0 &&
	    ondisk_log_size == 0 &&
	    up == 0 &&
	    acting == 0 &&
	    num_store_stats == 0);
  }

  // helper accessors to retrieve used/netto bytes depending on the
  // collection method: new per-pool objectstore report or legacy PG
  // summation at OSD.
  // In legacy mode used and netto values are the same. But for new per-pool
  // collection 'used' provides amount of space ALLOCATED at all related OSDs 
  // and 'netto' is amount of stored user data.
  uint64_t get_allocated_data_bytes(bool per_pool) const {
    if (per_pool) {
      return store_stats.allocated;
    } else {
      // legacy mode, use numbers from 'stats'
      return stats.sum.num_bytes + stats.sum.num_bytes_hit_set_archive;
    }
  }
  uint64_t get_allocated_omap_bytes(bool per_pool_omap) const {
    if (per_pool_omap) {
      return store_stats.omap_allocated;
    } else {
      // omap is not broken out by pool by nautilus bluestore; report the
      // scrub value.  this will be imprecise in that it won't account for
      // any storage overhead/efficiency.
      return stats.sum.num_omap_bytes;
    }
  }
  uint64_t get_user_data_bytes(float raw_used_rate, ///< space amp factor
			       bool per_pool) const {
    // NOTE: we need the space amp factor so that we can work backwards from
    // the raw utilization to the amount of data that the user actually stored.
    if (per_pool) {
      return raw_used_rate ? store_stats.data_stored / raw_used_rate : 0;
    } else {
      // legacy mode, use numbers from 'stats'.  note that we do NOT use the
      // raw_used_rate factor here because we are working from the PG stats
      // directly.
      return stats.sum.num_bytes + stats.sum.num_bytes_hit_set_archive;
    }
  }
  uint64_t get_user_omap_bytes(float raw_used_rate, ///< space amp factor
			       bool per_pool_omap) const {
    if (per_pool_omap) {
      return raw_used_rate ? store_stats.omap_allocated / raw_used_rate : 0;
    } else {
      // omap usage is lazily reported during scrub; this value may lag.
      return stats.sum.num_omap_bytes;
    }
  }

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static std::list<pool_stat_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(pool_stat_t)

#endif // CEPH_OSD_TYPES_STATS_H
