// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
 
/*
 * Placement Group Map. Placement Groups are logical sets of objects
 * that are replicated by the same set of devices. pgid=(r,hash(o)&m)
 * where & is a bit-wise AND and m=2^k-1
 */

#ifndef CEPH_PGMAP_H
#define CEPH_PGMAP_H

#include "include/health.h"
#include "common/debug.h"
#include "common/TextTable.h"
#include "osd/osd_types.h"
#include "include/mempool.h"
#include "mon/health_check.h"
#include <sstream>

namespace ceph { class Formatter; }

class PGMapDigest {
public:
  MEMPOOL_CLASS_HELPERS();
  virtual ~PGMapDigest() {}

  mempool::pgmap::vector<uint64_t> osd_last_seq;

  mutable std::map<int, int64_t> avail_space_by_rule;

  // aggregate state, populated by PGMap child
  int64_t num_pg = 0, num_osd = 0;
  int64_t num_pg_active = 0;
  int64_t num_pg_unknown = 0;
  mempool::pgmap::unordered_map<int32_t,pool_stat_t> pg_pool_sum;
  mempool::pgmap::map<int64_t,int64_t> num_pg_by_pool;
  pool_stat_t pg_sum;
  osd_stat_t osd_sum;
  mempool::pgmap::map<std::string,osd_stat_t> osd_sum_by_class;
  mempool::pgmap::unordered_map<uint64_t,int32_t> num_pg_by_state;
  struct pg_count {
    int32_t acting = 0;
    int32_t up_not_acting = 0;
    int32_t primary = 0;
    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(acting, bl);
      encode(up_not_acting, bl);
      encode(primary, bl);
    }
    void decode(ceph::buffer::list::const_iterator& p) {
      using ceph::decode;
      decode(acting, p);
      decode(up_not_acting, p);
      decode(primary, p);
    }
    void dump(ceph::Formatter *f) const {
      f->dump_int("acting", acting);
      f->dump_int("up_not_acting", up_not_acting);
      f->dump_int("primary", primary);
    }
    static void generate_test_instances(std::list<pg_count*>& o) {
      o.push_back(new pg_count);
      o.push_back(new pg_count);
      o.back()->acting = 1;
      o.back()->up_not_acting = 2;
      o.back()->primary = 3;
    }
  };
  mempool::pgmap::unordered_map<int32_t,pg_count> num_pg_by_osd;

  mempool::pgmap::map<int64_t,interval_set<snapid_t>> purged_snaps;

  bool use_per_pool_stats() const {
    return osd_sum.num_osds == osd_sum.num_per_pool_osds;
  }
  bool use_per_pool_omap_stats() const {
    return osd_sum.num_osds == osd_sum.num_per_pool_omap_osds;
  }

  // recent deltas, and summation
  /**
   * keep track of last deltas for each pool, calculated using
   * @p pg_pool_sum as baseline.
   */
  mempool::pgmap::unordered_map<int64_t, mempool::pgmap::list<std::pair<pool_stat_t, utime_t> > > per_pool_sum_deltas;
  /**
   * keep track of per-pool timestamp deltas, according to last update on
   * each pool.
   */
  mempool::pgmap::unordered_map<int64_t, utime_t> per_pool_sum_deltas_stamps;
  /**
   * keep track of sum deltas, per-pool, taking into account any previous
   * deltas existing in @p per_pool_sum_deltas.  The utime_t as second member
   * of the pair is the timestamp referring to the last update (i.e., the first
   * member of the pair) for a given pool.
   */
  mempool::pgmap::unordered_map<int64_t, std::pair<pool_stat_t,utime_t> > per_pool_sum_delta;

  pool_stat_t pg_sum_delta;
  utime_t stamp_delta;

  void get_recovery_stats(
    double *misplaced_ratio,
    double *degraded_ratio,
    double *inactive_ratio,
    double *unknown_pgs_ratio) const;

  void print_summary(ceph::Formatter *f, std::ostream *out) const;
  void print_oneline_summary(ceph::Formatter *f, std::ostream *out) const;

  void recovery_summary(ceph::Formatter *f, std::list<std::string> *psl,
                        const pool_stat_t& pool_sum) const;
  void overall_recovery_summary(ceph::Formatter *f, std::list<std::string> *psl) const;
  void pool_recovery_summary(ceph::Formatter *f, std::list<std::string> *psl,
                             uint64_t poolid) const;
  void recovery_rate_summary(ceph::Formatter *f, std::ostream *out,
                             const pool_stat_t& delta_sum,
                             utime_t delta_stamp) const;
  void overall_recovery_rate_summary(ceph::Formatter *f, std::ostream *out) const;
  void pool_recovery_rate_summary(ceph::Formatter *f, std::ostream *out,
                                  uint64_t poolid) const;
  /**
   * Obtain a formatted/plain output for client I/O, source from stats for a
   * given @p delta_sum pool over a given @p delta_stamp period of time.
   */
  void client_io_rate_summary(ceph::Formatter *f, std::ostream *out,
                              const pool_stat_t& delta_sum,
                              utime_t delta_stamp) const;
  /**
   * Obtain a formatted/plain output for the overall client I/O, which is
   * calculated resorting to @p pg_sum_delta and @p stamp_delta.
   */
  void overall_client_io_rate_summary(ceph::Formatter *f, std::ostream *out) const;
  /**
   * Obtain a formatted/plain output for client I/O over a given pool
   * with id @p pool_id.  We will then obtain pool-specific data
   * from @p per_pool_sum_delta.
   */
  void pool_client_io_rate_summary(ceph::Formatter *f, std::ostream *out,
                                   uint64_t poolid) const;
  /**
   * Obtain a formatted/plain output for cache tier IO, source from stats for a
   * given @p delta_sum pool over a given @p delta_stamp period of time.
   */
  void cache_io_rate_summary(ceph::Formatter *f, std::ostream *out,
                             const pool_stat_t& delta_sum,
                             utime_t delta_stamp) const;
  /**
   * Obtain a formatted/plain output for the overall cache tier IO, which is
   * calculated resorting to @p pg_sum_delta and @p stamp_delta.
   */
  void overall_cache_io_rate_summary(ceph::Formatter *f, std::ostream *out) const;
  /**
   * Obtain a formatted/plain output for cache tier IO over a given pool
   * with id @p pool_id.  We will then obtain pool-specific data
   * from @p per_pool_sum_delta.
   */
  void pool_cache_io_rate_summary(ceph::Formatter *f, std::ostream *out,
                                  uint64_t poolid) const;

  /**
   * Return the number of additional bytes that can be stored in this
   * pool before the first OSD fills up, accounting for PG overhead.
   */
  int64_t get_pool_free_space(const OSDMap &osd_map, int64_t poolid) const;


  /**
   * Dump pool usage and io ops/bytes, used by "ceph df" command
   */
  virtual void dump_pool_stats_full(const OSDMap &osd_map, std::stringstream *ss,
				    ceph::Formatter *f, bool verbose) const;
  void dump_cluster_stats(std::stringstream *ss, ceph::Formatter *f, bool verbose) const;
  static void dump_object_stat_sum(TextTable &tbl, ceph::Formatter *f,
				   const pool_stat_t &pool_stat,
				   uint64_t avail,
				   float raw_used_rate,
				   bool verbose,
				   bool per_pool,
				   bool per_pool_omap,
				   const pg_pool_t *pool);

  size_t get_num_pg_by_osd(int osd) const {
    auto p = num_pg_by_osd.find(osd);
    if (p == num_pg_by_osd.end())
      return 0;
    else
      return p->second.acting;
  }
  int get_num_primary_pg_by_osd(int osd) const {
    auto p = num_pg_by_osd.find(osd);
    if (p == num_pg_by_osd.end())
      return 0;
    else
      return p->second.primary;
  }

  ceph_statfs get_statfs(OSDMap &osdmap,
                         std::optional<int64_t> data_pool) const;

  int64_t get_rule_avail(int ruleno) const {
    auto i = avail_space_by_rule.find(ruleno);
    if (i != avail_space_by_rule.end())
      return avail_space_by_rule[ruleno];
    else
      return 0;
  }

  // kill me post-mimic or -nautilus
  bool definitely_converted_snapsets() const {
    // false negative is okay; false positive is not!
    return
      num_pg &&
      num_pg_unknown == 0 &&
      pg_sum.stats.sum.num_legacy_snapsets == 0;
  }

  uint64_t get_last_osd_stat_seq(int osd) {
    if (osd < (int)osd_last_seq.size())
      return osd_last_seq[osd];
    return 0;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<PGMapDigest*>& ls);
};
WRITE_CLASS_ENCODER(PGMapDigest::pg_count);
WRITE_CLASS_ENCODER_FEATURES(PGMapDigest);

class PGMap : public PGMapDigest {
public:
  MEMPOOL_CLASS_HELPERS();

  // the map
  version_t version;
  epoch_t last_osdmap_epoch;   // last osdmap epoch i applied to the pgmap
  epoch_t last_pg_scan;  // osdmap epoch
  mempool::pgmap::unordered_map<int32_t,osd_stat_t> osd_stat;
  mempool::pgmap::unordered_map<pg_t,pg_stat_t> pg_stat;

  typedef mempool::pgmap::map<
    std::pair<int64_t, int>,  // <pool, osd>
    store_statfs_t>
      per_osd_pool_statfs_t;

  per_osd_pool_statfs_t pool_statfs;

  class Incremental {
  public:
    MEMPOOL_CLASS_HELPERS();
    version_t version;
    mempool::pgmap::map<pg_t,pg_stat_t> pg_stat_updates;
    epoch_t osdmap_epoch;
    epoch_t pg_scan;  // osdmap epoch
    mempool::pgmap::set<pg_t> pg_remove;
    utime_t stamp;
    per_osd_pool_statfs_t pool_statfs_updates;

  private:
    mempool::pgmap::map<int32_t,osd_stat_t> osd_stat_updates;
    mempool::pgmap::set<int32_t> osd_stat_rm;
  public:

    const mempool::pgmap::map<int32_t, osd_stat_t> &get_osd_stat_updates() const {
      return osd_stat_updates;
    }
    const mempool::pgmap::set<int32_t> &get_osd_stat_rm() const {
      return osd_stat_rm;
    }
    template<typename OsdStat>
    void update_stat(int32_t osd, OsdStat&& stat) {
      osd_stat_updates[osd] = std::forward<OsdStat>(stat);
    }
    void stat_osd_out(int32_t osd) {
      osd_stat_updates[osd] = osd_stat_t();
    }
    void stat_osd_down_up(int32_t osd, const PGMap& pg_map) {
      // 0 the op_queue_age_hist for this osd
      auto p = osd_stat_updates.find(osd);
      if (p != osd_stat_updates.end()) {
	p->second.op_queue_age_hist.clear();
	return;
      }
      auto q = pg_map.osd_stat.find(osd);
      if (q != pg_map.osd_stat.end()) {
	osd_stat_t& t = osd_stat_updates[osd] = q->second;
	t.op_queue_age_hist.clear();
      }
    }
    void rm_stat(int32_t osd) {
      osd_stat_rm.insert(osd);
      osd_stat_updates.erase(osd);
    }
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<Incremental*>& o);

    Incremental() : version(0), osdmap_epoch(0), pg_scan(0) {}
  };


  // aggregate stats (soft state), generated by calc_stats()
  mempool::pgmap::unordered_map<int,std::set<pg_t> > pg_by_osd;
  mempool::pgmap::unordered_map<int,int> blocked_by_sum;
  mempool::pgmap::list<std::pair<pool_stat_t, utime_t> > pg_sum_deltas;
  mempool::pgmap::unordered_map<int64_t,mempool::pgmap::unordered_map<uint64_t,int32_t>> num_pg_by_pool_state;

  utime_t stamp;

  void update_pool_deltas(
    CephContext *cct,
    const utime_t ts,
    const mempool::pgmap::unordered_map<int32_t, pool_stat_t>& pg_pool_sum_old);
  void clear_delta();

  void deleted_pool(int64_t pool) {
    for (auto i = pool_statfs.begin();  i != pool_statfs.end();) {
      if (i->first.first == pool) {
	i = pool_statfs.erase(i);
      } else {
        ++i;
      }
    }

    pg_pool_sum.erase(pool);
    num_pg_by_pool_state.erase(pool);
    num_pg_by_pool.erase(pool);
    per_pool_sum_deltas.erase(pool);
    per_pool_sum_deltas_stamps.erase(pool);
    per_pool_sum_delta.erase(pool);
  }

 private:
  void update_delta(
    CephContext *cct,
    const utime_t ts,
    const pool_stat_t& old_pool_sum,
    utime_t *last_ts,
    const pool_stat_t& current_pool_sum,
    pool_stat_t *result_pool_delta,
    utime_t *result_ts_delta,
    mempool::pgmap::list<std::pair<pool_stat_t,utime_t> > *delta_avg_list);

  void update_one_pool_delta(CephContext *cct,
                             const utime_t ts,
                             const int64_t pool,
                             const pool_stat_t& old_pool_sum);

 public:

  mempool::pgmap::set<pg_t> creating_pgs;
  mempool::pgmap::map<int,std::map<epoch_t,std::set<pg_t> > > creating_pgs_by_osd_epoch;

  // Bits that use to be enum StuckPG
  static const int STUCK_INACTIVE = (1<<0);
  static const int STUCK_UNCLEAN = (1<<1);
  static const int STUCK_UNDERSIZED = (1<<2);
  static const int STUCK_DEGRADED = (1<<3);
  static const int STUCK_STALE = (1<<4);
  static const int STUCK_PEERING = (1<<5);

  PGMap()
    : version(0),
      last_osdmap_epoch(0), last_pg_scan(0)
  {}

  version_t get_version() const {
    return version;
  }
  void set_version(version_t v) {
    version = v;
  }
  epoch_t get_last_osdmap_epoch() const {
    return last_osdmap_epoch;
  }
  void set_last_osdmap_epoch(epoch_t e) {
    last_osdmap_epoch = e;
  }
  epoch_t get_last_pg_scan() const {
    return last_pg_scan;
  }
  void set_last_pg_scan(epoch_t e) {
    last_pg_scan = e;
  }
  utime_t get_stamp() const {
    return stamp;
  }
  void set_stamp(utime_t s) {
    stamp = s;
  }

  pool_stat_t get_pg_pool_sum_stat(int64_t pool) const {
    auto p = pg_pool_sum.find(pool);
    if (p != pg_pool_sum.end())
      return p->second;
    return pool_stat_t();
  }

  osd_stat_t get_osd_sum(const std::set<int>& osds) const {
    if (osds.empty()) // all
      return osd_sum;
    osd_stat_t sum;
    for (auto i : osds) {
      auto os = get_osd_stat(i);
      if (os)
        sum.add(*os);
    }
    return sum;
  }

  const osd_stat_t *get_osd_stat(int osd) const {
    auto i = osd_stat.find(osd);
    if (i == osd_stat.end()) {
      return nullptr;
    }
    return &i->second;
  }


  void apply_incremental(CephContext *cct, const Incremental& inc);
  void calc_stats();
  void stat_pg_add(const pg_t &pgid, const pg_stat_t &s,
		   bool sameosds=false);
  bool stat_pg_sub(const pg_t &pgid, const pg_stat_t &s,
		   bool sameosds=false);
  void calc_purged_snaps();
  void calc_osd_sum_by_class(const OSDMap& osdmap);
  void stat_osd_add(int osd, const osd_stat_t &s);
  void stat_osd_sub(int osd, const osd_stat_t &s);
  
  void encode(ceph::buffer::list &bl, uint64_t features=-1) const;
  void decode(ceph::buffer::list::const_iterator &bl);

  /// encode subset of our data to a PGMapDigest
  void encode_digest(const OSDMap& osdmap,
		     ceph::buffer::list& bl, uint64_t features);

  int64_t get_rule_avail(const OSDMap& osdmap, int ruleno) const;
  void get_rules_avail(const OSDMap& osdmap,
		       std::map<int,int64_t> *avail_map) const;
  void dump(ceph::Formatter *f, bool with_net = true) const;
  void dump_basic(ceph::Formatter *f) const;
  void dump_pg_stats(ceph::Formatter *f, bool brief) const;
  void dump_pg_progress(ceph::Formatter *f) const;
  void dump_pool_stats(ceph::Formatter *f) const;
  void dump_osd_stats(ceph::Formatter *f, bool with_net = true) const;
  void dump_osd_ping_times(ceph::Formatter *f) const;
  void dump_delta(ceph::Formatter *f) const;
  void dump_filtered_pg_stats(ceph::Formatter *f, std::set<pg_t>& pgs) const;
  void dump_pool_stats_full(const OSDMap &osd_map, std::stringstream *ss,
			    ceph::Formatter *f, bool verbose) const override {
    get_rules_avail(osd_map, &avail_space_by_rule);
    PGMapDigest::dump_pool_stats_full(osd_map, ss, f, verbose);
  }

  /*
  * Dump client io rate, recovery io rate, cache io rate and recovery information.
  * this function is used by "ceph osd pool stats" command
  */
  void dump_pool_stats_and_io_rate(int64_t poolid, const OSDMap &osd_map, ceph::Formatter *f,
				   std::stringstream *ss) const;

  static void dump_pg_stats_plain(
    std::ostream& ss,
    const mempool::pgmap::unordered_map<pg_t, pg_stat_t>& pg_stats,
    bool brief);
  void get_stuck_stats(
    int types, const utime_t cutoff,
    mempool::pgmap::unordered_map<pg_t, pg_stat_t>& stuck_pgs) const;
  void dump_stuck(ceph::Formatter *f, int types, utime_t cutoff) const;
  void dump_stuck_plain(std::ostream& ss, int types, utime_t cutoff) const;
  int dump_stuck_pg_stats(std::stringstream &ds,
			  ceph::Formatter *f,
			  int threshold,
			  std::vector<std::string>& args) const;
  void dump(std::ostream& ss) const;
  void dump_basic(std::ostream& ss) const;
  void dump_pg_stats(std::ostream& ss, bool brief) const;
  void dump_pg_sum_stats(std::ostream& ss, bool header) const;
  void dump_pool_stats(std::ostream& ss, bool header) const;
  void dump_osd_stats(std::ostream& ss) const;
  void dump_osd_sum_stats(std::ostream& ss) const;
  void dump_filtered_pg_stats(std::ostream& ss, std::set<pg_t>& pgs) const;

  void dump_osd_perf_stats(ceph::Formatter *f) const;
  void print_osd_perf_stats(std::ostream *ss) const;

  void dump_osd_blocked_by_stats(ceph::Formatter *f) const;
  void print_osd_blocked_by_stats(std::ostream *ss) const;

  void get_filtered_pg_stats(uint64_t state, int64_t poolid, int64_t osdid,
                             bool primary, std::set<pg_t>& pgs) const;

  std::set<std::string> osd_parentage(const OSDMap& osdmap, int id) const;
  void get_health_checks(
    CephContext *cct,
    const OSDMap& osdmap,
    health_check_map_t *checks) const;
  void print_summary(ceph::Formatter *f, std::ostream *out) const;

  static void generate_test_instances(std::list<PGMap*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(PGMap)

inline std::ostream& operator<<(std::ostream& out, const PGMapDigest& m) {
  m.print_oneline_summary(NULL, &out);
  return out;
}

int process_pg_map_command(
  const std::string& prefix,
  const cmdmap_t& cmdmap,
  const PGMap& pg_map,
  const OSDMap& osdmap,
  ceph::Formatter *f,
  std::stringstream *ss,
  ceph::buffer::list *odata);

class PGMapUpdater
{
public:
  static void check_osd_map(
    CephContext *cct,
    const OSDMap &osdmap,
    const PGMap& pg_map,
    PGMap::Incremental *pending_inc);

  // mark pg's state stale if its acting primary osd is down
  static void check_down_pgs(
      const OSDMap &osd_map,
      const PGMap &pg_map,
      bool check_all,
      const std::set<int>& need_check_down_pg_osds,
      PGMap::Incremental *pending_inc);
};

namespace reweight {
/* Assign a lower weight to overloaded OSDs.
 *
 * The osds that will get a lower weight are those with with a utilization
 * percentage 'oload' percent greater than the average utilization.
 */
  int by_utilization(const OSDMap &osd_map,
		     const PGMap &pg_map,
		     int oload,
		     double max_changef,
		     int max_osds,
		     bool by_pg, const std::set<int64_t> *pools,
		     bool no_increasing,
		     mempool::osdmap::map<int32_t, uint32_t>* new_weights,
		     std::stringstream *ss,
		     std::string *out_str,
		     ceph::Formatter *f);
}

#endif
