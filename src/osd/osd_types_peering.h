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

#ifndef CEPH_OSD_TYPES_PEERING_H
#define CEPH_OSD_TYPES_PEERING_H

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

#ifdef WITH_CRIMSON
#include <boost/smart_ptr/local_shared_ptr.hpp>
#endif

#include "common/dout.h"
#include "osd/osd_types_core.h"
#include "osd/osd_types_pool.h"
#include "osd/osd_types_stats.h"
#include "osd/pg_features.h"

// -----------------------------------------

/**
 * pg_hit_set_info_t - information about a single recorded HitSet
 *
 * Track basic metadata about a HitSet, like the number of insertions
 * and the time range it covers.
 */
struct pg_hit_set_info_t {
  utime_t begin, end;   ///< time interval
  eversion_t version;   ///< version this HitSet object was written
  bool using_gmt;	///< use gmt for creating the hit_set archive object name

  friend bool operator==(const pg_hit_set_info_t& l,
			 const pg_hit_set_info_t& r) {
    return
      l.begin == r.begin &&
      l.end == r.end &&
      l.version == r.version &&
      l.using_gmt == r.using_gmt;
  }

  explicit pg_hit_set_info_t(bool using_gmt = true)
    : using_gmt(using_gmt) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_hit_set_info_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_hit_set_info_t)

/**
 * pg_hit_set_history_t - information about a history of hitsets
 *
 * Include information about the currently accumulating hit set as well
 * as archived/historical ones.
 */
struct pg_hit_set_history_t {
  eversion_t current_last_update;  ///< last version inserted into current set
  std::list<pg_hit_set_info_t> history; ///< archived sets, sorted oldest -> newest

  friend bool operator==(const pg_hit_set_history_t& l,
			 const pg_hit_set_history_t& r) {
    return
      l.current_last_update == r.current_last_update &&
      l.history == r.history;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_hit_set_history_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_hit_set_history_t)


// -----------------------------------------

/**
 * pg_history_t - information about recent pg peering/mapping history
 *
 * This is aggressively shared between OSDs to bound the amount of past
 * history they need to worry about.
 */
struct pg_history_t {
  epoch_t epoch_created = 0;       // epoch in which *pg* was created (pool or pg)
  epoch_t epoch_pool_created = 0;  // epoch in which *pool* was created
			       // (note: may be pg creation epoch for
			       // pre-luminous clusters)
  epoch_t last_epoch_started = 0;;  // lower bound on last epoch started (anywhere, not necessarily locally)
                                    // https://docs.ceph.com/docs/master/dev/osd_internals/last_epoch_started/
  epoch_t last_interval_started = 0;; // first epoch of last_epoch_started interval
  epoch_t last_epoch_clean = 0;;    // lower bound on last epoch the PG was completely clean.
  epoch_t last_interval_clean = 0;; // first epoch of last_epoch_clean interval
  epoch_t last_epoch_split = 0;;    // as parent or child
  epoch_t last_epoch_marked_full = 0;;  // pool or cluster

  /**
   * In the event of a map discontinuity, same_*_since may reflect the first
   * map the osd has seen in the new map sequence rather than the actual start
   * of the interval.  This is ok since a discontinuity at epoch e means there
   * must have been a clean interval between e and now and that we cannot be
   * in the active set during the interval containing e.
   */
  epoch_t same_up_since = 0;;       // same acting set since
  epoch_t same_interval_since = 0;;   // same acting AND up set since
  epoch_t same_primary_since = 0;;  // same primary at least back through this epoch.

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;

  /// upper bound on how long prior interval readable (relative to encode time)
  ceph::timespan prior_readable_until_ub = ceph::timespan::zero();

  friend bool operator==(const pg_history_t& l, const pg_history_t& r) {
    return
      l.epoch_created == r.epoch_created &&
      l.epoch_pool_created == r.epoch_pool_created &&
      l.last_epoch_started == r.last_epoch_started &&
      l.last_interval_started == r.last_interval_started &&
      l.last_epoch_clean == r.last_epoch_clean &&
      l.last_interval_clean == r.last_interval_clean &&
      l.last_epoch_split == r.last_epoch_split &&
      l.last_epoch_marked_full == r.last_epoch_marked_full &&
      l.same_up_since == r.same_up_since &&
      l.same_interval_since == r.same_interval_since &&
      l.same_primary_since == r.same_primary_since &&
      l.last_scrub == r.last_scrub &&
      l.last_deep_scrub == r.last_deep_scrub &&
      l.last_scrub_stamp == r.last_scrub_stamp &&
      l.last_deep_scrub_stamp == r.last_deep_scrub_stamp &&
      l.last_clean_scrub_stamp == r.last_clean_scrub_stamp &&
      l.prior_readable_until_ub == r.prior_readable_until_ub;
  }

  pg_history_t() {}
  pg_history_t(epoch_t created, utime_t stamp)
    : epoch_created(created),
      epoch_pool_created(created),
      same_up_since(created),
      same_interval_since(created),
      same_primary_since(created),
      last_scrub_stamp(stamp),
      last_deep_scrub_stamp(stamp),
      last_clean_scrub_stamp(stamp) {}
  
  bool merge(const pg_history_t &other) {
    // Here, we only update the fields which cannot be calculated from the OSDmap.
    bool modified = false;
    if (epoch_created < other.epoch_created) {
      epoch_created = other.epoch_created;
      modified = true;
    }
    if (epoch_pool_created < other.epoch_pool_created) {
      // FIXME: for jewel compat only; this should either be 0 or always the
      // same value across all pg instances.
      epoch_pool_created = other.epoch_pool_created;
      modified = true;
    }
    if (last_epoch_started < other.last_epoch_started) {
      last_epoch_started = other.last_epoch_started;
      modified = true;
    }
    if (last_interval_started < other.last_interval_started) {
      last_interval_started = other.last_interval_started;
      // if we are learning about a newer *started* interval, our
      // readable_until_ub is obsolete
      prior_readable_until_ub = other.prior_readable_until_ub;
      modified = true;
    } else if (other.last_interval_started == last_interval_started &&
	       other.prior_readable_until_ub < prior_readable_until_ub) {
      // if other is the *same* interval, than pull our upper bound in
      // if they have a tighter bound.
      prior_readable_until_ub = other.prior_readable_until_ub;
      modified = true;
    }
    if (last_epoch_clean < other.last_epoch_clean) {
      last_epoch_clean = other.last_epoch_clean;
      modified = true;
    }
    if (last_interval_clean < other.last_interval_clean) {
      last_interval_clean = other.last_interval_clean;
      modified = true;
    }
    if (last_epoch_split < other.last_epoch_split) {
      last_epoch_split = other.last_epoch_split; 
      modified = true;
    }
    if (last_epoch_marked_full < other.last_epoch_marked_full) {
      last_epoch_marked_full = other.last_epoch_marked_full;
      modified = true;
    }
    if (other.last_scrub > last_scrub) {
      last_scrub = other.last_scrub;
      modified = true;
    }
    if (other.last_scrub_stamp > last_scrub_stamp) {
      last_scrub_stamp = other.last_scrub_stamp;
      modified = true;
    }
    if (other.last_deep_scrub > last_deep_scrub) {
      last_deep_scrub = other.last_deep_scrub;
      modified = true;
    }
    if (other.last_deep_scrub_stamp > last_deep_scrub_stamp) {
      last_deep_scrub_stamp = other.last_deep_scrub_stamp;
      modified = true;
    }
    if (other.last_clean_scrub_stamp > last_clean_scrub_stamp) {
      last_clean_scrub_stamp = other.last_clean_scrub_stamp;
      modified = true;
    }
    return modified;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_history_t> generate_test_instances();

  ceph::signedspan refresh_prior_readable_until_ub(
    ceph::signedspan now,  ///< now, relative to osd startup_time
    ceph::signedspan ub) { ///< ub, relative to osd startup_time
    if (now >= ub) {
      // prior interval(s) are unreadable; we can zero the upper bound
      prior_readable_until_ub = ceph::signedspan::zero();
      return ceph::signedspan::zero();
    } else {
      prior_readable_until_ub = ub - now;
      return ub;
    }
  }
  ceph::signedspan get_prior_readable_until_ub(ceph::signedspan now) {
    if (prior_readable_until_ub == ceph::signedspan::zero()) {
      return ceph::signedspan::zero();
    }
    return now + prior_readable_until_ub;
  }
};
WRITE_CLASS_ENCODER(pg_history_t)

inline std::ostream& operator<<(std::ostream& out, const pg_history_t& h) {
  out << "ec=" << h.epoch_created << "/" << h.epoch_pool_created
      << " lis/c=" << h.last_interval_started
      << "/" << h.last_interval_clean
      << " les/c/f=" << h.last_epoch_started << "/" << h.last_epoch_clean
      << "/" << h.last_epoch_marked_full
      << " sis=" << h.same_interval_since;
  if (h.prior_readable_until_ub != ceph::timespan::zero()) {
    out << " pruub=" << h.prior_readable_until_ub;
  }
  return out;
}


/**
 * pg_info_t - summary of PG statistics.
 *
 * some notes: 
 *  - last_complete implies we have all objects that existed as of that
 *    stamp, OR a newer object, OR have already applied a later delete.
 *  - if last_complete >= log.tail, then we know pg contents thru log.head.
 *    otherwise, we have no idea what the pg is supposed to contain.
 */
struct pg_info_t {
  spg_t pgid;
  eversion_t last_update;      ///< last object version applied to store.
  eversion_t last_complete;    ///< last version pg was complete through.
  epoch_t last_epoch_started;  ///< last epoch at which this pg started on this osd
  epoch_t last_interval_started; ///< first epoch of last_epoch_started interval
  
  version_t last_user_version; ///< last user object version applied to store

  eversion_t log_tail;         ///< oldest log entry.

  hobject_t last_backfill;     ///< objects >= this and < last_complete may be missing

  interval_set<snapid_t> purged_snaps;

  std::map<shard_id_t,std::pair<eversion_t, eversion_t>>
    partial_writes_last_complete; ///< last_complete for shards not modified by a partial write
  epoch_t partial_writes_last_complete_epoch; ///< epoch when pwlc was last updated

  pg_stat_t stats;

  pg_history_t history;
  pg_hit_set_history_t hit_set;

  friend bool operator==(const pg_info_t& l, const pg_info_t& r) {
    return
      l.pgid == r.pgid &&
      l.last_update == r.last_update &&
      l.last_complete == r.last_complete &&
      l.last_epoch_started == r.last_epoch_started &&
      l.last_interval_started == r.last_interval_started &&
      l.last_user_version == r.last_user_version &&
      l.log_tail == r.log_tail &&
      l.last_backfill == r.last_backfill &&
      l.purged_snaps == r.purged_snaps &&
      l.partial_writes_last_complete == r.partial_writes_last_complete &&
      l.partial_writes_last_complete_epoch == r.partial_writes_last_complete_epoch &&
      l.stats == r.stats &&
      l.history == r.history &&
      l.hit_set == r.hit_set;
  }

  pg_info_t()
    : last_epoch_started(0),
      last_interval_started(0),
      last_user_version(0),
      last_backfill(hobject_t::get_max()),
      partial_writes_last_complete_epoch(0)
  { }
  // cppcheck-suppress noExplicitConstructor
  pg_info_t(spg_t p)
    : pgid(p),
      last_epoch_started(0),
      last_interval_started(0),
      last_user_version(0),
      last_backfill(hobject_t::get_max()),
      partial_writes_last_complete_epoch(0)
  { }
  
  void set_last_backfill(hobject_t pos) {
    last_backfill = pos;
  }

  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  bool has_missing() const { return last_complete != last_update; }
  bool is_incomplete() const { return !last_backfill.is_max(); }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_info_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_info_t)

inline std::ostream& operator<<(std::ostream& out, const pg_info_t& pgi) 
{
  out << pgi.pgid << "(";
  if (pgi.dne())
    out << " DNE";
  if (pgi.is_empty())
    out << " empty";
  else {
    out << " v " << pgi.last_update;
    if (pgi.last_complete != pgi.last_update)
      out << " lc " << pgi.last_complete;
    out << " (" << pgi.log_tail << "," << pgi.last_update << "]";
  }
  if (pgi.is_incomplete())
    out << " lb " << pgi.last_backfill;
  //out << " c " << pgi.epoch_created;
  out << " local-lis/les=" << pgi.last_interval_started
      << "/" << pgi.last_epoch_started;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

/**
 * pg_fast_info_t - common pg_info_t fields
 *
 * These are the fields of pg_info_t (and children) that are updated for
 * most IO operations.
 *
 * ** WARNING **
 * Because we rely on these fields to be applied to the normal
 * info struct, adding a new field here that is not also new in info
 * means that we must set an incompat OSD feature bit!
 */
struct pg_fast_info_t {
  eversion_t last_update;
  eversion_t last_complete;
  version_t last_user_version;
  std::map<shard_id_t,std::pair<eversion_t,eversion_t>> partial_writes_last_complete;
  epoch_t partial_writes_last_complete_epoch;
  struct { // pg_stat_t stats
    eversion_t version;
    version_t reported_seq;
    utime_t last_fresh;
    utime_t last_active;
    utime_t last_peered;
    utime_t last_clean;
    utime_t last_unstale;
    utime_t last_undegraded;
    utime_t last_fullsized;
    int64_t log_size;  // (also ondisk_log_size, which has the same value)
    struct { // object_stat_collection_t stats;
      struct { // objct_stat_sum_t sum
	int64_t num_bytes;    // in bytes
	int64_t num_objects;
	int64_t num_object_copies;
	int64_t num_rd;
	int64_t num_rd_kb;
	int64_t num_wr;
	int64_t num_wr_kb;
	int64_t num_objects_dirty;
      } sum;
    } stats;
  } stats;

  void populate_from(const pg_info_t& info) {
    last_update = info.last_update;
    last_complete = info.last_complete;
    last_user_version = info.last_user_version;
    partial_writes_last_complete = info.partial_writes_last_complete;
    partial_writes_last_complete_epoch = info.partial_writes_last_complete_epoch;
    stats.version = info.stats.version;
    stats.reported_seq = info.stats.reported_seq;
    stats.last_fresh = info.stats.last_fresh;
    stats.last_active = info.stats.last_active;
    stats.last_peered = info.stats.last_peered;
    stats.last_clean = info.stats.last_clean;
    stats.last_unstale = info.stats.last_unstale;
    stats.last_undegraded = info.stats.last_undegraded;
    stats.last_fullsized = info.stats.last_fullsized;
    stats.log_size = info.stats.log_size;
    stats.stats.sum.num_bytes = info.stats.stats.sum.num_bytes;
    stats.stats.sum.num_objects = info.stats.stats.sum.num_objects;
    stats.stats.sum.num_object_copies = info.stats.stats.sum.num_object_copies;
    stats.stats.sum.num_rd = info.stats.stats.sum.num_rd;
    stats.stats.sum.num_rd_kb = info.stats.stats.sum.num_rd_kb;
    stats.stats.sum.num_wr = info.stats.stats.sum.num_wr;
    stats.stats.sum.num_wr_kb = info.stats.stats.sum.num_wr_kb;
    stats.stats.sum.num_objects_dirty = info.stats.stats.sum.num_objects_dirty;
  }

  bool try_apply_to(pg_info_t* info) {
    if (last_update <= info->last_update)
      return false;
    info->last_update = last_update;
    info->last_complete = last_complete;
    info->last_user_version = last_user_version;
    info->partial_writes_last_complete = partial_writes_last_complete;
    info->partial_writes_last_complete_epoch = partial_writes_last_complete_epoch;
    info->stats.version = stats.version;
    info->stats.reported_seq = stats.reported_seq;
    info->stats.last_fresh = stats.last_fresh;
    info->stats.last_active = stats.last_active;
    info->stats.last_peered = stats.last_peered;
    info->stats.last_clean = stats.last_clean;
    info->stats.last_unstale = stats.last_unstale;
    info->stats.last_undegraded = stats.last_undegraded;
    info->stats.last_fullsized = stats.last_fullsized;
    info->stats.log_size = stats.log_size;
    info->stats.ondisk_log_size = stats.log_size;
    info->stats.stats.sum.num_bytes = stats.stats.sum.num_bytes;
    info->stats.stats.sum.num_objects = stats.stats.sum.num_objects;
    info->stats.stats.sum.num_object_copies = stats.stats.sum.num_object_copies;
    info->stats.stats.sum.num_rd = stats.stats.sum.num_rd;
    info->stats.stats.sum.num_rd_kb = stats.stats.sum.num_rd_kb;
    info->stats.stats.sum.num_wr = stats.stats.sum.num_wr;
    info->stats.stats.sum.num_wr_kb = stats.stats.sum.num_wr_kb;
    info->stats.stats.sum.num_objects_dirty = stats.stats.sum.num_objects_dirty;
    return true;
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(3, 1, bl);
    encode(last_update, bl);
    encode(last_complete, bl);
    encode(last_user_version, bl);
    encode(stats.version, bl);
    encode(stats.reported_seq, bl);
    encode(stats.last_fresh, bl);
    encode(stats.last_active, bl);
    encode(stats.last_peered, bl);
    encode(stats.last_clean, bl);
    encode(stats.last_unstale, bl);
    encode(stats.last_undegraded, bl);
    encode(stats.last_fullsized, bl);
    encode(stats.log_size, bl);
    encode(stats.stats.sum.num_bytes, bl);
    encode(stats.stats.sum.num_objects, bl);
    encode(stats.stats.sum.num_object_copies, bl);
    encode(stats.stats.sum.num_rd, bl);
    encode(stats.stats.sum.num_rd_kb, bl);
    encode(stats.stats.sum.num_wr, bl);
    encode(stats.stats.sum.num_wr_kb, bl);
    encode(stats.stats.sum.num_objects_dirty, bl);
    encode(partial_writes_last_complete, bl);
    encode(partial_writes_last_complete_epoch, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(3, p);
    decode(last_update, p);
    decode(last_complete, p);
    decode(last_user_version, p);
    decode(stats.version, p);
    decode(stats.reported_seq, p);
    decode(stats.last_fresh, p);
    decode(stats.last_active, p);
    decode(stats.last_peered, p);
    decode(stats.last_clean, p);
    decode(stats.last_unstale, p);
    decode(stats.last_undegraded, p);
    decode(stats.last_fullsized, p);
    decode(stats.log_size, p);
    decode(stats.stats.sum.num_bytes, p);
    decode(stats.stats.sum.num_objects, p);
    decode(stats.stats.sum.num_object_copies, p);
    decode(stats.stats.sum.num_rd, p);
    decode(stats.stats.sum.num_rd_kb, p);
    decode(stats.stats.sum.num_wr, p);
    decode(stats.stats.sum.num_wr_kb, p);
    decode(stats.stats.sum.num_objects_dirty, p);
    if (struct_v >= 2)
      decode(partial_writes_last_complete, p);
    if (struct_v >= 3)
      decode(partial_writes_last_complete_epoch, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("last_update") << last_update;
    f->dump_stream("last_complete") << last_complete;
    f->dump_stream("last_user_version") << last_user_version;
    f->open_array_section("partial_writes_last_complete");
    for (const auto & [shard, versionrange] : partial_writes_last_complete) {
      auto & [from, to]  = versionrange;
      f->open_object_section("shard");
      f->dump_int("id", int(shard));
      f->dump_stream("from") << from;
      f->dump_stream("to") << to;
      f->close_section();
    }
    f->close_section();
    f->dump_stream("partial_writes_last_complete_epoch") << partial_writes_last_complete_epoch;
    f->open_object_section("stats");
    f->dump_stream("version") << stats.version;
    f->dump_unsigned("reported_seq", stats.reported_seq);
    f->dump_stream("last_fresh") << stats.last_fresh;
    f->dump_stream("last_active") << stats.last_active;
    f->dump_stream("last_peered") << stats.last_peered;
    f->dump_stream("last_clean") << stats.last_clean;
    f->dump_stream("last_unstale") << stats.last_unstale;
    f->dump_stream("last_undegraded") << stats.last_undegraded;
    f->dump_stream("last_fullsized") << stats.last_fullsized;
    f->dump_unsigned("log_size", stats.log_size);
    f->dump_unsigned("ondisk_log_size", stats.log_size);
    f->dump_unsigned("num_bytes", stats.stats.sum.num_bytes);
    f->dump_unsigned("num_objects", stats.stats.sum.num_objects);
    f->dump_unsigned("num_object_copies", stats.stats.sum.num_object_copies);
    f->dump_unsigned("num_rd", stats.stats.sum.num_rd);
    f->dump_unsigned("num_rd_kb", stats.stats.sum.num_rd_kb);
    f->dump_unsigned("num_wr", stats.stats.sum.num_wr);
    f->dump_unsigned("num_wr_kb", stats.stats.sum.num_wr_kb);
    f->dump_unsigned("num_objects_dirty", stats.stats.sum.num_objects_dirty);
    f->close_section();
  }
  static std::list<pg_fast_info_t> generate_test_instances() {
    std::list<pg_fast_info_t> o;
    o.emplace_back();
    o.emplace_back();
    o.back().last_update = eversion_t(1, 2);
    o.back().last_complete = eversion_t(3, 4);
    o.back().last_user_version = version_t(5);
    o.back().stats.version = eversion_t(7, 8);
    o.back().stats.reported_seq = 9;
    o.back().stats.last_fresh = utime_t(10, 0);
    o.back().stats.last_active = utime_t(11, 0);
    o.back().stats.last_peered = utime_t(12, 0);
    o.back().stats.last_clean = utime_t(13, 0);
    o.back().stats.last_unstale = utime_t(14, 0);
    return o;
  }
};
WRITE_CLASS_ENCODER(pg_fast_info_t)


/**
 * PastIntervals -- information needed to determine the PriorSet and
 * the might_have_unfound set
 */
class PastIntervals {
#ifdef WITH_CRIMSON
  using OSDMapRef = boost::local_shared_ptr<const OSDMap>;
#else
  using OSDMapRef = std::shared_ptr<const OSDMap>;
#endif
public:
  struct pg_interval_t {
    std::vector<int32_t> up, acting;
    epoch_t first, last;
    bool maybe_went_rw;
    int32_t primary;
    int32_t up_primary;

    pg_interval_t()
      : first(0), last(0),
	maybe_went_rw(false),
	primary(-1),
	up_primary(-1)
      {}

    pg_interval_t(
      std::vector<int32_t> &&up,
      std::vector<int32_t> &&acting,
      epoch_t first,
      epoch_t last,
      bool maybe_went_rw,
      int32_t primary,
      int32_t up_primary)
      : up(up), acting(acting), first(first), last(last),
	maybe_went_rw(maybe_went_rw), primary(primary), up_primary(up_primary)
      {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    std::string fmt_print() const;
    static std::list<pg_interval_t> generate_test_instances();
  };

  PastIntervals();
  PastIntervals(PastIntervals &&rhs) = default;
  PastIntervals &operator=(PastIntervals &&rhs) = default;

  PastIntervals(const PastIntervals &rhs);
  PastIntervals &operator=(const PastIntervals &rhs);

  class interval_rep {
  public:
    virtual size_t size() const = 0;
    virtual bool empty() const = 0;
    virtual void clear() = 0;
    virtual std::pair<epoch_t, epoch_t> get_bounds() const = 0;
    virtual std::set<pg_shard_t> get_all_participants(
      bool ec_pool) const = 0;
    virtual void add_interval(bool ec_pool, const pg_interval_t &interval) = 0;
    virtual std::unique_ptr<interval_rep> clone() const = 0;
    virtual std::ostream &print(std::ostream &out) const = 0;
    virtual void encode(ceph::buffer::list &bl) const = 0;
    virtual void decode(ceph::buffer::list::const_iterator &bl) = 0;
    virtual void dump(ceph::Formatter *f) const = 0;
    virtual std::string print() const = 0;
    virtual void iterate_mayberw_back_to(
      epoch_t les,
      std::function<void(epoch_t, const std::set<pg_shard_t> &)> &&f) const = 0;

    virtual bool has_full_intervals() const { return false; }
    virtual void iterate_all_intervals(
      std::function<void(const pg_interval_t &)> &&f) const {
      ceph_assert(!has_full_intervals());
      ceph_abort_msg("not valid for this implementation");
    }
    virtual void adjust_start_backwards(epoch_t last_epoch_clean) = 0;

    virtual ~interval_rep() {}
  };
  friend class pi_compact_rep;
private:

  std::unique_ptr<interval_rep> past_intervals;

  explicit PastIntervals(interval_rep *rep) : past_intervals(rep) {}

public:
  void add_interval(bool ec_pool, const pg_interval_t &interval) {
    ceph_assert(past_intervals);
    return past_intervals->add_interval(ec_pool, interval);
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    if (past_intervals) {
      __u8 type = 2;
      encode(type, bl);
      past_intervals->encode(bl);
    } else {
      encode((__u8)0, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const {
    ceph_assert(past_intervals);
    past_intervals->dump(f);
  }

  std::string fmt_print() const;

  static std::list<PastIntervals> generate_test_instances();

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,
    int new_acting_primary,
    const std::vector<int> &old_acting,
    const std::vector<int> &new_acting,
    int old_up_primary,
    int new_up_primary,
    const std::vector<int> &old_up,
    const std::vector<int> &new_up,
    int old_size,
    int new_size,
    int old_min_size,
    int new_min_size,
    unsigned old_pg_num,
    unsigned new_pg_num,
    unsigned old_pg_num_pending,
    unsigned new_pg_num_pending,
    bool old_sort_bitwise,
    bool new_sort_bitwise,
    bool old_recovery_deletes,
    bool new_recovery_deletes,
    uint32_t old_crush_count,
    uint32_t new_crush_count,
    uint32_t old_crush_target,
    uint32_t new_crush_target,
    uint32_t old_crush_barrier,
    uint32_t new_crush_barrier,
    int32_t old_crush_member,
    int32_t new_crush_member,
    bool old_allow_ec_optimizations,
    bool new_allow_ec_optimizations,
    pg_t pgid
    );

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of lastmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    const OSDMap *osdmap,  ///< [in] current map
    const OSDMap *lastmap, ///< [in] last map
    pg_t pgid                                   ///< [in] pgid for pg
    );

  /**
   * Integrates a new map into *past_intervals, returns true
   * if an interval was closed out.
   */
  static bool check_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of osdmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    const OSDMap *osdmap,      ///< [in] current map
    const OSDMap *lastmap,     ///< [in] last map
    pg_t pgid,                                  ///< [in] pgid for pg
    const IsPGRecoverablePredicate &could_have_gone_active, ///< [in] predicate whether the pg can be active
    PastIntervals *past_intervals,              ///< [out] intervals
    std::ostream *out = 0                            ///< [out] debug ostream
    );
  static bool check_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of osdmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    OSDMapRef osdmap,      ///< [in] current map
    OSDMapRef lastmap,     ///< [in] last map
    pg_t pgid,                                  ///< [in] pgid for pg
    const IsPGRecoverablePredicate &could_have_gone_active, ///< [in] predicate whether the pg can be active
    PastIntervals *past_intervals,              ///< [out] intervals
    std::ostream *out = 0                            ///< [out] debug ostream
    ) {
    return check_new_interval(
      old_acting_primary, new_acting_primary,
      old_acting, new_acting,
      old_up_primary, new_up_primary,
      old_up, new_up,
      same_interval_since, last_epoch_clean,
      osdmap.get(), lastmap.get(),
      pgid,
      could_have_gone_active,
      past_intervals,
      out);
  }

  friend std::ostream& operator<<(std::ostream& out, const PastIntervals &i);

  template <typename F>
  void iterate_mayberw_back_to(
    epoch_t les,
    F &&f) const {
    ceph_assert(past_intervals);
    past_intervals->iterate_mayberw_back_to(les, std::forward<F>(f));
  }
  void clear() {
    ceph_assert(past_intervals);
    past_intervals->clear();
  }

  /**
   * Should return a value which gives an indication of the amount
   * of state contained
   */
  size_t size() const {
    ceph_assert(past_intervals);
    return past_intervals->size();
  }

  bool empty() const {
    ceph_assert(past_intervals);
    return past_intervals->empty();
  }

  void swap(PastIntervals &other) {
    using std::swap;
    swap(other.past_intervals, past_intervals);
  }

  /**
   * Return all shards which have been in the acting set back to the
   * latest epoch to which we have trimmed except for pg_whoami
   */
  std::set<pg_shard_t> get_might_have_unfound(
    pg_shard_t pg_whoami,
    bool ec_pool) const {
    ceph_assert(past_intervals);
    auto ret = past_intervals->get_all_participants(ec_pool);
    ret.erase(pg_whoami);
    return ret;
  }

  /**
   * Return all shards which we might want to talk to for peering
   */
  std::set<pg_shard_t> get_all_probe(
    bool ec_pool) const {
    ceph_assert(past_intervals);
    return past_intervals->get_all_participants(ec_pool);
  }

  /* Return the set of epochs [start, end) represented by the
   * past_interval set.
   */
  std::pair<epoch_t, epoch_t> get_bounds() const {
    ceph_assert(past_intervals);
    return past_intervals->get_bounds();
  }

  void adjust_start_backwards(epoch_t last_epoch_clean) {
    ceph_assert(past_intervals);
    past_intervals->adjust_start_backwards(last_epoch_clean);
  }

  enum osd_state_t {
    UP,
    DOWN,
    DNE,
    LOST
  };
  struct PriorSet {
    bool ec_pool = false;
    std::set<pg_shard_t> probe; ///< current+prior OSDs we need to probe.
    std::set<int> down;  ///< down osds that would normally be in @a probe and might be interesting.
    std::map<int, epoch_t> blocked_by;  ///< current lost_at values for any OSDs in cur set for which (re)marking them lost would affect cur set

    bool pg_down = false;   ///< some down osds are included in @a cur; the DOWN pg state bit should be set.
    const IsPGRecoverablePredicate* pcontdec = nullptr;

    PriorSet() = default;
    PriorSet(PriorSet &&) = default;
    PriorSet &operator=(PriorSet &&) = default;

    PriorSet &operator=(const PriorSet &) = delete;
    PriorSet(const PriorSet &) = delete;

    bool operator==(const PriorSet &rhs) const {
      return (ec_pool == rhs.ec_pool) &&
	(probe == rhs.probe) &&
	(down == rhs.down) &&
	(blocked_by == rhs.blocked_by) &&
	(pg_down == rhs.pg_down);
    }

    bool affected_by_map(
      const OSDMap &osdmap,
      const DoutPrefixProvider *dpp) const;

    std::string fmt_print() const;

    // For verifying tests
    PriorSet(
      bool ec_pool,
      std::set<pg_shard_t> probe,
      std::set<int> down,
      std::map<int, epoch_t> blocked_by,
      bool pg_down,
      const IsPGRecoverablePredicate *pcontdec)
      : ec_pool(ec_pool), probe(probe), down(down), blocked_by(blocked_by),
	pg_down(pg_down), pcontdec(pcontdec) {}

  private:
    template <typename F>
    PriorSet(
      const PastIntervals &past_intervals,
      bool ec_pool,
      epoch_t last_epoch_started,
      const IsPGRecoverablePredicate *c,
      F f,
      const std::vector<int> &up,
      const std::vector<int> &acting,
      const DoutPrefixProvider *dpp);

    friend class PastIntervals;
  };

  template <typename... Args>
  PriorSet get_prior_set(Args&&... args) const {
    return PriorSet(*this, std::forward<Args>(args)...);
  }
};
WRITE_CLASS_ENCODER(PastIntervals)
WRITE_CLASS_ENCODER(PastIntervals::pg_interval_t)

std::ostream& operator<<(std::ostream& out, const PastIntervals::pg_interval_t& i);
std::ostream& operator<<(std::ostream& out, const PastIntervals &i);
std::ostream& operator<<(std::ostream& out, const PastIntervals::PriorSet &i);

template <typename F>
PastIntervals::PriorSet::PriorSet(
  const PastIntervals &past_intervals,
  bool ec_pool,
  epoch_t last_epoch_started,
  const IsPGRecoverablePredicate *c,
  F f,
  const std::vector<int> &up,
  const std::vector<int> &acting,
  const DoutPrefixProvider *dpp)
  : ec_pool(ec_pool), pg_down(false), pcontdec(c)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike)
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i = 0; i < acting.size(); i++) {
    if (acting[i] != pg_pool_t::pg_CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(acting[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  // It may be possible to exclude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i = 0; i < up.size(); i++) {
    if (up[i] != pg_pool_t::pg_CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(up[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }

  std::set<pg_shard_t> all_probe = past_intervals.get_all_probe(ec_pool);
  ldpp_dout(dpp, 10) << "build_prior all_probe " << all_probe << dendl;
  for (auto &&i: all_probe) {
    switch (f(0, i.osd, nullptr)) {
    case UP: {
      probe.insert(i);
      break;
    }
    case DNE:
    case LOST:
    case DOWN: {
      down.insert(i.osd);
      break;
    }
    }
  }

  past_intervals.iterate_mayberw_back_to(
    last_epoch_started,
    [&](epoch_t start, const std::set<pg_shard_t> &acting) {
      ldpp_dout(dpp, 10) << "build_prior maybe_rw interval:" << start
			 << ", acting: " << acting << dendl;

      // look at candidate osds during this interval.  each falls into
      // one of three categories: up, down (but potentially
      // interesting), or lost (down, but we won't wait for it).
      std::set<pg_shard_t> up_now;
      std::map<int, epoch_t> candidate_blocked_by;
      // any candidates down now (that might have useful data)
      bool any_down_now = false;

      // consider ACTING osds
      for (auto &&so: acting) {
	epoch_t lost_at = 0;
	switch (f(start, so.osd, &lost_at)) {
	case UP: {
	  // include past acting osds if they are up.
	  up_now.insert(so);
	  break;
	}
	case DNE: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " no longer exists" << dendl;
	  break;
	}
	case LOST: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " is down, but lost_at " << lost_at << dendl;
	  up_now.insert(so);
	  break;
	}
	case DOWN: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " is down" << dendl;
	  candidate_blocked_by[so.osd] = lost_at;
	  any_down_now = true;
	  break;
	}
	}
      }

      // if not enough osds survived this interval, and we may have gone rw,
      // then we need to wait for one of those osds to recover to
      // ensure that we haven't lost any information.
      if (!(*pcontdec)(up_now) && any_down_now) {
	// fixme: how do we identify a "clean" shutdown anyway?
	ldpp_dout(dpp, 10) << "build_prior  possibly went active+rw,"
			   << " insufficient up; including down osds" << dendl;
	ceph_assert(!candidate_blocked_by.empty());
	pg_down = true;
	blocked_by.insert(
	  candidate_blocked_by.begin(),
	  candidate_blocked_by.end());
      }
    });

  ldpp_dout(dpp, 10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
}

struct pg_notify_t {
  epoch_t query_epoch;
  epoch_t epoch_sent;
  pg_info_t info;
  shard_id_t to;
  shard_id_t from;
  PastIntervals past_intervals;
  pg_feature_vec_t pg_features = PG_FEATURE_NONE;
  pg_notify_t() :
    query_epoch(0), epoch_sent(0), to(shard_id_t::NO_SHARD),
    from(shard_id_t::NO_SHARD) {}
  pg_notify_t(
    shard_id_t to,
    shard_id_t from,
    epoch_t query_epoch,
    epoch_t epoch_sent,
    const pg_info_t &info,
    const PastIntervals& pi,
    pg_feature_vec_t pg_features)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info), to(to), from(from),
      past_intervals(pi), pg_features(pg_features) {
    ceph_assert(from == info.pgid.shard);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_notify_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_notify_t)
std::ostream &operator<<(std::ostream &lhs, const pg_notify_t &notify);


/** 
 * pg_query_t - used to ask a peer for information about a pg.
 *
 * note: if version=0, type=LOG, then we just provide our full log.
 */
struct pg_query_t {
  enum {
    INFO = 0,
    LOG = 1,
    MISSING = 4,
    FULLLOG = 5,
  };
  std::string_view get_type_name() const {
    switch (type) {
    case INFO: return "info";
    case LOG: return "log";
    case MISSING: return "missing";
    case FULLLOG: return "fulllog";
    default: return "???";
    }
  }

  __s32 type;
  eversion_t since;
  pg_history_t history;
  epoch_t epoch_sent;
  shard_id_t to;
  shard_id_t from;

  pg_query_t() : type(-1), epoch_sent(0), to(shard_id_t::NO_SHARD),
		 from(shard_id_t::NO_SHARD) {}
  pg_query_t(
    int t,
    shard_id_t to,
    shard_id_t from,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t),
      history(h),
      epoch_sent(epoch_sent),
      to(to), from(from) {
    ceph_assert(t != LOG);
  }
  pg_query_t(
    int t,
    shard_id_t to,
    shard_id_t from,
    eversion_t s,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t), since(s), history(h),
      epoch_sent(epoch_sent), to(to), from(from) {
    ceph_assert(t == LOG);
  }
  
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const;
  static std::list<pg_query_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(pg_query_t)

inline std::ostream& operator<<(std::ostream& out, const pg_query_t& q) {
  out << "query(" << q.get_type_name() << " " << q.since;
  if (q.type == pg_query_t::LOG)
    out << " " << q.history;
  out << " epoch_sent " << q.epoch_sent;
  out << ")";
  return out;
}

/**
 * pg_lease_t - readable lease metadata, from primary -> non-primary
 *
 * This metadata serves to increase either or both of the lease expiration
 * and upper bound on the non-primary.
 */
struct pg_lease_t {
  /// pg readable_until value; replicas must not be readable beyond this
  ceph::signedspan readable_until = ceph::signedspan::zero();

  /// upper bound on any acting osd's readable_until
  ceph::signedspan readable_until_ub = ceph::signedspan::zero();

  /// duration of the lease (in case clock deltas aren't available)
  ceph::signedspan interval = ceph::signedspan::zero();

  pg_lease_t() {}
  pg_lease_t(ceph::signedspan ru, ceph::signedspan ruub,
	     ceph::signedspan i)
    : readable_until(ru),
      readable_until_ub(ruub),
      interval(i) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_lease_t> generate_test_instances();

  friend std::ostream& operator<<(std::ostream& out, const pg_lease_t& l) {
    return out << "pg_lease(ru " << l.readable_until
	       << " ub " << l.readable_until_ub
	       << " int " << l.interval << ")";
  }
};
WRITE_CLASS_ENCODER(pg_lease_t)

/**
 * pg_lease_ack_t - lease ack, from non-primary -> primary
 *
 * This metadata acknowledges to the primary what a non-primary's noted
 * upper bound is.
 */
struct pg_lease_ack_t {
  /// highest upper bound non-primary has recorded (primary's clock)
  ceph::signedspan readable_until_ub = ceph::signedspan::zero();

  pg_lease_ack_t() {}
  pg_lease_ack_t(ceph::signedspan ub)
    : readable_until_ub(ub) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_lease_ack_t> generate_test_instances();

  friend std::ostream& operator<<(std::ostream& out, const pg_lease_ack_t& l) {
    return out << "pg_lease_ack(ruub " << l.readable_until_ub << ")";
  }
};
WRITE_CLASS_ENCODER(pg_lease_ack_t)

#endif // CEPH_OSD_TYPES_PEERING_H
