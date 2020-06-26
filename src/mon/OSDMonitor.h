// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

/* Object Store Device (OSD) Monitor
 */

#ifndef CEPH_OSDMONITOR_H
#define CEPH_OSDMONITOR_H

#include <map>
#include <set>

#include "include/types.h"
#include "include/encoding.h"
#include "common/simple_cache.hpp"
#include "common/PriorityCache.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"
#include "osd/OSDMapMapping.h"

#include "CreatingPGs.h"
#include "PaxosService.h"

#include "erasure-code/ErasureCodeInterface.h"
#include "mon/MonOpRequest.h"
#include <boost/functional/hash.hpp>

class Monitor;
class PGMap;
struct MonSession;
class MOSDMap;


/// information about a particular peer's failure reports for one osd
struct failure_reporter_t {
  utime_t failed_since;     ///< when they think it failed
  MonOpRequestRef op;       ///< failure op request

  failure_reporter_t() {}
  explicit failure_reporter_t(utime_t s) : failed_since(s) {}
  ~failure_reporter_t() { }
};

/// information about all failure reports for one osd
struct failure_info_t {
  std::map<int, failure_reporter_t> reporters;  ///< reporter -> failed_since etc
  utime_t max_failed_since;                ///< most recent failed_since

  failure_info_t() {}

  utime_t get_failed_since() {
    if (max_failed_since == utime_t() && !reporters.empty()) {
      // the old max must have canceled; recalculate.
      for (auto p = reporters.begin(); p != reporters.end(); ++p)
	if (p->second.failed_since > max_failed_since)
	  max_failed_since = p->second.failed_since;
    }
    return max_failed_since;
  }

  // set the message for the latest report.  return any old op request we had,
  // if any, so we can discard it.
  MonOpRequestRef add_report(int who, utime_t failed_since,
			     MonOpRequestRef op) {
    auto p = reporters.find(who);
    if (p == reporters.end()) {
      if (max_failed_since != utime_t() && max_failed_since < failed_since)
	max_failed_since = failed_since;
      p = reporters.insert(std::map<int, failure_reporter_t>::value_type(who, failure_reporter_t(failed_since))).first;
    }

    MonOpRequestRef ret = p->second.op;
    p->second.op = op;
    return ret;
  }

  void take_report_messages(std::list<MonOpRequestRef>& ls) {
    for (auto p = reporters.begin(); p != reporters.end(); ++p) {
      if (p->second.op) {
	ls.push_back(p->second.op);
        p->second.op.reset();
      }
    }
  }

  MonOpRequestRef cancel_report(int who) {
    auto p = reporters.find(who);
    if (p == reporters.end())
      return MonOpRequestRef();
    MonOpRequestRef ret = p->second.op;
    reporters.erase(p);
    max_failed_since = utime_t();
    return ret;
  }
};


class LastEpochClean {
  struct Lec {
    std::vector<epoch_t> epoch_by_pg;
    ps_t next_missing = 0;
    epoch_t floor = std::numeric_limits<epoch_t>::max();
    void report(ps_t pg, epoch_t last_epoch_clean);
  };
  std::map<uint64_t, Lec> report_by_pool;
public:
  void report(const pg_t& pg, epoch_t last_epoch_clean);
  void remove_pool(uint64_t pool);
  epoch_t get_lower_bound(const OSDMap& latest) const;

  void dump(Formatter *f) const;
};


struct osdmap_manifest_t {
  // all the maps we have pinned -- i.e., won't be removed unless
  // they are inside a trim interval.
  std::set<version_t> pinned;

  osdmap_manifest_t() {}

  version_t get_last_pinned() const
  {
    auto it = pinned.crbegin();
    if (it == pinned.crend()) {
      return 0;
    }
    return *it;
  }

  version_t get_first_pinned() const
  {
    auto it = pinned.cbegin();
    if (it == pinned.cend()) {
      return 0;
    }
    return *it;
  }

  bool is_pinned(version_t v) const
  {
    return pinned.find(v) != pinned.end();
  }

  void pin(version_t v)
  {
    pinned.insert(v);
  }

  version_t get_lower_closest_pinned(version_t v) const {
    auto p = pinned.lower_bound(v);
    if (p == pinned.cend()) {
      return 0;
    } else if (*p > v) {
      if (p == pinned.cbegin()) {
        return 0;
      }
      --p;
    }
    return *p;
  }

  void encode(ceph::buffer::list& bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(pinned, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(pinned, bl);
    DECODE_FINISH(bl);
  }

  void decode(ceph::buffer::list& bl) {
    auto p = bl.cbegin();
    decode(p);
  }

  void dump(ceph::Formatter *f) {
    f->dump_unsigned("first_pinned", get_first_pinned());
    f->dump_unsigned("last_pinned", get_last_pinned());
    f->open_array_section("pinned_maps");
    for (auto& i : pinned) {
      f->dump_unsigned("epoch", i);
    }
    f->close_section();
 }
};
WRITE_CLASS_ENCODER(osdmap_manifest_t);

class OSDMonitor : public PaxosService,
                   public md_config_obs_t {
  CephContext *cct;

public:
  OSDMap osdmap;

  // config observer
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
    const std::set<std::string> &changed) override;
  // [leader]
  OSDMap::Incremental pending_inc;
  std::map<int, ceph::buffer::list> pending_metadata;
  std::set<int>             pending_metadata_rm;
  std::map<int, failure_info_t> failure_info;
  std::map<int,utime_t>    down_pending_out;  // osd down -> out
  bool priority_convert = false;
  std::map<int64_t,std::set<snapid_t>> pending_pseudo_purged_snaps;
  std::shared_ptr<PriorityCache::PriCache> rocksdb_binned_kv_cache = nullptr;
  std::shared_ptr<PriorityCache::Manager> pcm = nullptr;
  ceph::mutex balancer_lock = ceph::make_mutex("OSDMonitor::balancer_lock");

  std::map<int,double> osd_weight;

  using osdmap_key_t = std::pair<version_t, uint64_t>;
  using osdmap_cache_t = SimpleLRU<osdmap_key_t,
                                   ceph::buffer::list,
                                   std::less<osdmap_key_t>,
                                   boost::hash<osdmap_key_t>>;
  osdmap_cache_t inc_osd_cache;
  osdmap_cache_t full_osd_cache;

  bool has_osdmap_manifest;
  osdmap_manifest_t osdmap_manifest;

  bool check_failures(utime_t now);
  bool check_failure(utime_t now, int target_osd, failure_info_t& fi);
  void force_failure(int target_osd, int by);

  bool _have_pending_crush();
  CrushWrapper &_get_stable_crush();
  void _get_pending_crush(CrushWrapper& newcrush);

  enum FastReadType {
    FAST_READ_OFF,
    FAST_READ_ON,
    FAST_READ_DEFAULT
  };

  struct CleanUpmapJob : public ParallelPGMapper::Job {
    CephContext *cct;
    const OSDMap& osdmap;
    OSDMap::Incremental& pending_inc;
    // lock to protect pending_inc form changing
    // when checking is done
    ceph::mutex pending_inc_lock =
      ceph::make_mutex("CleanUpmapJob::pending_inc_lock");

    CleanUpmapJob(CephContext *cct, const OSDMap& om, OSDMap::Incremental& pi)
      : ParallelPGMapper::Job(&om),
        cct(cct),
        osdmap(om),
        pending_inc(pi) {}

    void process(const std::vector<pg_t>& to_check) override {
      std::vector<pg_t> to_cancel;
      std::map<pg_t, mempool::osdmap::vector<std::pair<int,int>>> to_remap;
      osdmap.check_pg_upmaps(cct, to_check, &to_cancel, &to_remap);
      // don't bother taking lock if nothing changes
      if (!to_cancel.empty() || !to_remap.empty()) {
        std::lock_guard l(pending_inc_lock);
        osdmap.clean_pg_upmaps(cct, &pending_inc, to_cancel, to_remap);
      }
    }

    void process(int64_t poolid, unsigned ps_begin, unsigned ps_end) override {}
    void complete() override {}
  }; // public as this will need to be accessible from TestTestOSDMap.cc

  // svc
public:
  void create_initial() override;
  void get_store_prefixes(std::set<std::string>& s) const override;

private:
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;  // prepare a new pending
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  void on_active() override;
  void on_restart() override;
  void on_shutdown() override;

  /* osdmap full map prune */
  void load_osdmap_manifest();
  bool should_prune() const;
  void _prune_update_trimmed(
      MonitorDBStore::TransactionRef tx,
      version_t first);
  void prune_init(osdmap_manifest_t& manifest);
  bool _prune_sanitize_options() const;
  bool is_prune_enabled() const;
  bool is_prune_supported() const;
  bool do_prune(MonitorDBStore::TransactionRef tx);

  // Priority cache control
  uint32_t mon_osd_cache_size = 0;  ///< Number of cached OSDMaps
  uint64_t rocksdb_cache_size = 0;  ///< Cache for kv Db
  double cache_kv_ratio = 0;        ///< Cache ratio dedicated to kv
  double cache_inc_ratio = 0;       ///< Cache ratio dedicated to inc
  double cache_full_ratio = 0;      ///< Cache ratio dedicated to full
  uint64_t mon_memory_base = 0;     ///< Mon base memory for cache autotuning
  double mon_memory_fragmentation = 0; ///< Expected memory fragmentation
  uint64_t mon_memory_target = 0;   ///< Mon target memory for cache autotuning
  uint64_t mon_memory_min = 0;      ///< Min memory to cache osdmaps
  bool mon_memory_autotune = false; ///< Cache auto tune setting
  int register_cache_with_pcm();
  int _set_cache_sizes();
  int _set_cache_ratios();
  void _set_new_cache_sizes();
  void _set_cache_autotuning();
  int _update_mon_cache_settings();

  friend struct OSDMemCache;
  friend struct IncCache;
  friend struct FullCache;

  /**
   * we haven't delegated full version stashing to paxosservice for some time
   * now, making this function useless in current context.
   */
  void encode_full(MonitorDBStore::TransactionRef t) override { }
  /**
   * do not let paxosservice periodically stash full osdmaps, or we will break our
   * locally-managed full maps.  (update_from_paxos loads the latest and writes them
   * out going forward from there, but if we just synced that may mean we skip some.)
   */
  bool should_stash_full() override {
    return false;
  }

  /**
   * hook into trim to include the oldest full map in the trim transaction
   *
   * This ensures that anyone post-sync will have enough to rebuild their
   * full osdmaps.
   */
  void encode_trim_extra(MonitorDBStore::TransactionRef tx, version_t first) override;

  void update_msgr_features();
  int check_cluster_features(uint64_t features, std::stringstream &ss);
  /**
   * check if the cluster supports the features required by the
   * given crush map. Outputs the daemons which don't support it
   * to the stringstream.
   *
   * @returns true if the map is passable, false otherwise
   */
  bool validate_crush_against_features(const CrushWrapper *newcrush,
				       std::stringstream &ss);
  void check_osdmap_subs();
  void share_map_with_random_osd();

  ceph::mutex prime_pg_temp_lock =
    ceph::make_mutex("OSDMonitor::prime_pg_temp_lock");
  struct PrimeTempJob : public ParallelPGMapper::Job {
    OSDMonitor *osdmon;
    PrimeTempJob(const OSDMap& om, OSDMonitor *m)
      : ParallelPGMapper::Job(&om), osdmon(m) {}
    void process(int64_t pool, unsigned ps_begin, unsigned ps_end) override {
      for (unsigned ps = ps_begin; ps < ps_end; ++ps) {
	pg_t pgid(ps, pool);
	osdmon->prime_pg_temp(*osdmap, pgid);
      }
    }
    void process(const std::vector<pg_t>& pgs) override {}
    void complete() override {}
  };
  void maybe_prime_pg_temp();
  void prime_pg_temp(const OSDMap& next, pg_t pgid);

  ParallelPGMapper mapper;                        ///< for background pg work
  OSDMapMapping mapping;                          ///< pg <-> osd mappings
  std::unique_ptr<ParallelPGMapper::Job> mapping_job;  ///< background mapping job
  void start_mapping();

  void update_logger();

  void handle_query(PaxosServiceMessage *m);
  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;
  bool should_propose(double &delay) override;

  version_t get_trim_to() const override;

  bool can_mark_down(int o);
  bool can_mark_up(int o);
  bool can_mark_out(int o);
  bool can_mark_in(int o);

  // ...
  MOSDMap *build_latest_full(uint64_t features);
  MOSDMap *build_incremental(epoch_t first, epoch_t last, uint64_t features);
  void send_full(MonOpRequestRef op);
  void send_incremental(MonOpRequestRef op, epoch_t first);
public:
  // @param req an optional op request, if the osdmaps are replies to it. so
  //            @c Monitor::send_reply() can mark_event with it.
  void send_incremental(epoch_t first, MonSession *session, bool onetime,
			MonOpRequestRef req = MonOpRequestRef());

private:
  void print_utilization(std::ostream &out, ceph::Formatter *f, bool tree) const;

  bool check_source(MonOpRequestRef op, uuid_d fsid);
 
  bool preprocess_get_osdmap(MonOpRequestRef op);

  bool preprocess_mark_me_down(MonOpRequestRef op);

  friend class C_AckMarkedDown;
  bool preprocess_failure(MonOpRequestRef op);
  bool prepare_failure(MonOpRequestRef op);
  bool prepare_mark_me_down(MonOpRequestRef op);
  void process_failures();
  void take_all_failures(std::list<MonOpRequestRef>& ls);

  bool preprocess_mark_me_dead(MonOpRequestRef op);
  bool prepare_mark_me_dead(MonOpRequestRef op);

  bool preprocess_full(MonOpRequestRef op);
  bool prepare_full(MonOpRequestRef op);

  bool preprocess_boot(MonOpRequestRef op);
  bool prepare_boot(MonOpRequestRef op);
  void _booted(MonOpRequestRef op, bool logit);

  void update_up_thru(int from, epoch_t up_thru);
  bool preprocess_alive(MonOpRequestRef op);
  bool prepare_alive(MonOpRequestRef op);
  void _reply_map(MonOpRequestRef op, epoch_t e);

  bool preprocess_pgtemp(MonOpRequestRef op);
  bool prepare_pgtemp(MonOpRequestRef op);

  bool preprocess_pg_created(MonOpRequestRef op);
  bool prepare_pg_created(MonOpRequestRef op);

  bool preprocess_pg_ready_to_merge(MonOpRequestRef op);
  bool prepare_pg_ready_to_merge(MonOpRequestRef op);

  int _check_remove_pool(int64_t pool_id, const pg_pool_t &pool, std::ostream *ss);
  bool _check_become_tier(
      int64_t tier_pool_id, const pg_pool_t *tier_pool,
      int64_t base_pool_id, const pg_pool_t *base_pool,
      int *err, std::ostream *ss) const;
  bool _check_remove_tier(
      int64_t base_pool_id, const pg_pool_t *base_pool, const pg_pool_t *tier_pool,
      int *err, std::ostream *ss) const;

  int _prepare_remove_pool(int64_t pool, std::ostream *ss, bool no_fake);
  int _prepare_rename_pool(int64_t pool, std::string newname);

  bool enforce_pool_op_caps(MonOpRequestRef op);
  bool preprocess_pool_op (MonOpRequestRef op);
  bool preprocess_pool_op_create (MonOpRequestRef op);
  bool prepare_pool_op (MonOpRequestRef op);
  bool prepare_pool_op_create (MonOpRequestRef op);
  bool prepare_pool_op_delete(MonOpRequestRef op);
  int crush_rename_bucket(const std::string& srcname,
			  const std::string& dstname,
			  std::ostream *ss);
  void check_legacy_ec_plugin(const std::string& plugin, 
			      const std::string& profile) const;
  int normalize_profile(const std::string& profilename, 
			ceph::ErasureCodeProfile &profile,
			bool force,
			std::ostream *ss);
  int crush_rule_create_erasure(const std::string &name,
				const std::string &profile,
				int *rule,
				std::ostream *ss);
  int get_crush_rule(const std::string &rule_name,
		     int *crush_rule,
		     std::ostream *ss);
  int get_erasure_code(const std::string &erasure_code_profile,
		       ceph::ErasureCodeInterfaceRef *erasure_code,
		       std::ostream *ss) const;
  int prepare_pool_crush_rule(const unsigned pool_type,
			      const std::string &erasure_code_profile,
			      const std::string &rule_name,
			      int *crush_rule,
			      std::ostream *ss);
  bool erasure_code_profile_in_use(
    const mempool::osdmap::map<int64_t, pg_pool_t> &pools,
    const std::string &profile,
    std::ostream *ss);
  int parse_erasure_code_profile(const std::vector<std::string> &erasure_code_profile,
				 std::map<std::string,std::string> *erasure_code_profile_map,
				 std::ostream *ss);
  int prepare_pool_size(const unsigned pool_type,
			const std::string &erasure_code_profile,
                        uint8_t repl_size,
			unsigned *size, unsigned *min_size,
			std::ostream *ss);
  int prepare_pool_stripe_width(const unsigned pool_type,
				const std::string &erasure_code_profile,
				unsigned *stripe_width,
				std::ostream *ss);
  int check_pg_num(int64_t pool, int pg_num, int size, std::ostream* ss);
  int prepare_new_pool(std::string& name,
		       int crush_rule,
		       const std::string &crush_rule_name,
                       unsigned pg_num, unsigned pgp_num,
		       unsigned pg_num_min,
                       uint64_t repl_size,
		       const uint64_t target_size_bytes,
		       const float target_size_ratio,
		       const std::string &erasure_code_profile,
                       const unsigned pool_type,
                       const uint64_t expected_num_objects,
                       FastReadType fast_read,
		       const std::string& pg_autoscale_mode,
		       std::ostream *ss);
  int prepare_new_pool(MonOpRequestRef op);

  void set_pool_flags(int64_t pool_id, uint64_t flags);
  void clear_pool_flags(int64_t pool_id, uint64_t flags);
  bool update_pools_status();

  bool _is_removed_snap(int64_t pool_id, snapid_t snapid);
  bool _is_pending_removed_snap(int64_t pool_id, snapid_t snapid);

  std::string make_purged_snap_epoch_key(epoch_t epoch);
  std::string make_purged_snap_key(int64_t pool, snapid_t snap);
  std::string make_purged_snap_key_value(int64_t pool, snapid_t snap, snapid_t num,
				    epoch_t epoch, ceph::buffer::list *v);

  bool try_prune_purged_snaps();
  int lookup_purged_snap(int64_t pool, snapid_t snap,
			 snapid_t *begin, snapid_t *end);

  void insert_purged_snap_update(
    int64_t pool,
    snapid_t start, snapid_t end,
    epoch_t epoch,
    MonitorDBStore::TransactionRef t);

  bool prepare_set_flag(MonOpRequestRef op, int flag);
  bool prepare_unset_flag(MonOpRequestRef op, int flag);

  void _pool_op_reply(MonOpRequestRef op,
                      int ret, epoch_t epoch, ceph::buffer::list *blp=NULL);

  struct C_Booted : public C_MonOp {
    OSDMonitor *cmon;
    bool logit;
    C_Booted(OSDMonitor *cm, MonOpRequestRef op_, bool l=true) :
      C_MonOp(op_), cmon(cm), logit(l) {}
    void _finish(int r) override {
      if (r >= 0)
	cmon->_booted(op, logit);
      else if (r == -ECANCELED)
        return;
      else if (r == -EAGAIN)
        cmon->dispatch(op);
      else
	ceph_abort_msg("bad C_Booted return value");
    }
  };

  struct C_ReplyMap : public C_MonOp {
    OSDMonitor *osdmon;
    epoch_t e;
    C_ReplyMap(OSDMonitor *o, MonOpRequestRef op_, epoch_t ee)
      : C_MonOp(op_), osdmon(o), e(ee) {}
    void _finish(int r) override {
      if (r >= 0)
	osdmon->_reply_map(op, e);
      else if (r == -ECANCELED)
        return;
      else if (r == -EAGAIN)
	osdmon->dispatch(op);
      else
	ceph_abort_msg("bad C_ReplyMap return value");
    }    
  };
  struct C_PoolOp : public C_MonOp {
    OSDMonitor *osdmon;
    int replyCode;
    int epoch;
    ceph::buffer::list reply_data;
    C_PoolOp(OSDMonitor * osd, MonOpRequestRef op_, int rc, int e, ceph::buffer::list *rd=NULL) :
      C_MonOp(op_), osdmon(osd), replyCode(rc), epoch(e) {
      if (rd)
	reply_data = *rd;
    }
    void _finish(int r) override {
      if (r >= 0)
	osdmon->_pool_op_reply(op, replyCode, epoch, &reply_data);
      else if (r == -ECANCELED)
        return;
      else if (r == -EAGAIN)
	osdmon->dispatch(op);
      else
	ceph_abort_msg("bad C_PoolOp return value");
    }
  };

  bool preprocess_remove_snaps(MonOpRequestRef op);
  bool prepare_remove_snaps(MonOpRequestRef op);

  bool preprocess_get_purged_snaps(MonOpRequestRef op);

  int load_metadata(int osd, std::map<std::string, std::string>& m,
		    std::ostream *err);
  void count_metadata(const std::string& field, ceph::Formatter *f);

  void reencode_incremental_map(ceph::buffer::list& bl, uint64_t features);
  void reencode_full_map(ceph::buffer::list& bl, uint64_t features);
public:
  void count_metadata(const std::string& field, std::map<std::string,int> *out);
protected:
  int get_osd_objectstore_type(int osd, std::string *type);
  bool is_pool_currently_all_bluestore(int64_t pool_id, const pg_pool_t &pool,
				       std::ostream *err);

  // when we last received PG stats from each osd
  std::map<int,utime_t> last_osd_report;
  // TODO: use last_osd_report to store the osd report epochs, once we don't
  //       need to upgrade from pre-luminous releases.
  std::map<int,epoch_t> osd_epochs;
  LastEpochClean last_epoch_clean;
  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);
  epoch_t get_min_last_epoch_clean() const;

  friend class C_UpdateCreatingPGs;
  std::map<int, std::map<epoch_t, std::set<spg_t>>> creating_pgs_by_osd_epoch;
  std::vector<pg_t> pending_created_pgs;
  // the epoch when the pg mapping was calculated
  epoch_t creating_pgs_epoch = 0;
  creating_pgs_t creating_pgs;
  mutable std::mutex creating_pgs_lock;

  creating_pgs_t update_pending_pgs(const OSDMap::Incremental& inc,
				    const OSDMap& nextmap);
  unsigned scan_for_creating_pgs(
    const mempool::osdmap::map<int64_t,pg_pool_t>& pools,
    const mempool::osdmap::set<int64_t>& removed_pools,
    utime_t modified,
    creating_pgs_t* creating_pgs) const;
  std::pair<int32_t, pg_t> get_parent_pg(pg_t pgid) const;
  void update_creating_pgs();
  void check_pg_creates_subs();
  epoch_t send_pg_creates(int osd, Connection *con, epoch_t next) const;

  int32_t _allocate_osd_id(int32_t* existing_id);

  int get_grace_interval_threshold();
  bool grace_interval_threshold_exceeded(int last_failed);
  void set_default_laggy_params(int target_osd);

public:
  OSDMonitor(CephContext *cct, Monitor *mn, Paxos *p, const std::string& service_name);

  void tick() override;  // check state, take actions

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);
  bool prepare_command_impl(MonOpRequestRef op, const cmdmap_t& cmdmap);

  int validate_osd_create(
      const int32_t id,
      const uuid_d& uuid,
      const bool check_osd_exists,
      int32_t* existing_id,
      std::stringstream& ss);
  int prepare_command_osd_create(
      const int32_t id,
      const uuid_d& uuid,
      int32_t* existing_id,
      std::stringstream& ss);
  void do_osd_create(const int32_t id, const uuid_d& uuid,
		     const std::string& device_class,
		     int32_t* new_id);
  int prepare_command_osd_purge(int32_t id, std::stringstream& ss);
  int prepare_command_osd_destroy(int32_t id, std::stringstream& ss);
  int _prepare_command_osd_crush_remove(
      CrushWrapper &newcrush,
      int32_t id,
      int32_t ancestor,
      bool has_ancestor,
      bool unlink_only);
  void do_osd_crush_remove(CrushWrapper& newcrush);
  int prepare_command_osd_crush_remove(
      CrushWrapper &newcrush,
      int32_t id,
      int32_t ancestor,
      bool has_ancestor,
      bool unlink_only);
  int prepare_command_osd_remove(int32_t id);
  int prepare_command_osd_new(
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      const std::map<std::string,std::string>& secrets,
      std::stringstream &ss,
      ceph::Formatter *f);

  int prepare_command_pool_set(const cmdmap_t& cmdmap,
                               std::stringstream& ss);

  int prepare_command_pool_application(const std::string &prefix,
                                       const cmdmap_t& cmdmap,
                                       std::stringstream& ss);
  int preprocess_command_pool_application(const std::string &prefix,
                                          const cmdmap_t& cmdmap,
                                          std::stringstream& ss,
                                          bool *modified);
  int _command_pool_application(const std::string &prefix,
				const cmdmap_t& cmdmap,
				std::stringstream& ss,
				bool *modified,
				bool preparing);

  bool handle_osd_timeouts(const utime_t &now,
			   std::map<int,utime_t> &last_osd_report);

  void send_latest(MonOpRequestRef op, epoch_t start=0);
  void send_latest_now_nodelete(MonOpRequestRef op, epoch_t start=0) {
    op->mark_osdmon_event(__func__);
    send_incremental(op, start);
  }

  int get_version(version_t ver, ceph::buffer::list& bl) override;
  int get_version(version_t ver, uint64_t feature, ceph::buffer::list& bl);

  int get_version_full(version_t ver, uint64_t feature, ceph::buffer::list& bl);
  int get_version_full(version_t ver, ceph::buffer::list& bl) override;
  int get_inc(version_t ver, OSDMap::Incremental& inc);
  int get_full_from_pinned_map(version_t ver, ceph::buffer::list& bl);

  epoch_t blacklist(const entity_addrvec_t& av, utime_t until);
  epoch_t blacklist(entity_addr_t a, utime_t until);

  void dump_info(ceph::Formatter *f);
  int dump_osd_metadata(int osd, ceph::Formatter *f, std::ostream *err);
  void print_nodes(ceph::Formatter *f);

  void check_osdmap_sub(Subscription *sub);
  void check_pg_creates_sub(Subscription *sub);

  void do_application_enable(int64_t pool_id, const std::string &app_name,
			     const std::string &app_key="",
			     const std::string &app_value="",
			     bool force=false);
  void do_set_pool_opt(int64_t pool_id, pool_opts_t::key_t opt,
		       pool_opts_t::value_t);

  void add_flag(int flag) {
    if (!(osdmap.flags & flag)) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags |= flag;
    }
  }

  void remove_flag(int flag) {
    if(osdmap.flags & flag) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags &= ~flag;
    }
  }
  void convert_pool_priorities(void);
};

#endif
