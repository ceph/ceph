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

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/container/flat_set.hpp>
#include "include/mempool.h"

// re-include our assert to clobber boost's
#include "include/ceph_assert.h" 

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/xlist.h"
#include "SnapMapper.h"
#include "Session.h"
#include "common/Timer.h"

#include "PGLog.h"
#include "OSDMap.h"
#include "messages/MOSDPGLog.h"
#include "include/str_list.h"
#include "PGBackend.h"
#include "PGPeeringEvent.h"

#include "mgr/OSDPerfMetricTypes.h"

#include <atomic>
#include <list>
#include <memory>
#include <stack>
#include <string>
#include <tuple>

//#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs
//#define PG_DEBUG_REFS    // track provenance of pg refs, helpful for finding leaks

class OSD;
class OSDService;
class OSDShard;
class OSDShardPGSlot;
class MOSDOp;
class MOSDPGScan;
class MOSDPGBackfill;
class MOSDPGInfo;

class PG;
struct OpRequest;
typedef OpRequest::Ref OpRequestRef;
class MOSDPGLog;
class CephContext;
class DynamicPerfStats;

namespace Scrub {
  class Store;
}

using state_history_entry = std::tuple<utime_t, utime_t, const char*>;
using embedded_state = std::pair<utime_t, const char*>;

struct PGStateInstance {
  // Time spent in pg states

  void setepoch(const epoch_t current_epoch) {
    this_epoch = current_epoch;
  }

  void enter_state(const utime_t entime, const char* state) {
    embedded_states.push(std::make_pair(entime, state));
  }

  void exit_state(const utime_t extime) {
    embedded_state this_state = embedded_states.top();
    state_history.push_back(state_history_entry{
        this_state.first, extime, this_state.second});
    embedded_states.pop();
  }

  epoch_t this_epoch;
  utime_t enter_time;
  std::vector<state_history_entry> state_history;
  std::stack<embedded_state> embedded_states;
};

class PGStateHistory {
  // Member access protected with the PG lock
public:
  PGStateHistory() : buffer(10) {}

  void enter(PG* pg, const utime_t entime, const char* state);

  void exit(const char* state);

  void reset() {
    pi = nullptr;
  }

  void set_pg_in_destructor() { pg_in_destructor = true; }

  void dump(Formatter* f) const;

  string get_current_state() {
    if (pi == nullptr) return "unknown";
    return std::get<1>(pi->embedded_states.top());
  }

private:
  bool pg_in_destructor = false;
  PG* thispg = nullptr;
  std::unique_ptr<PGStateInstance> tmppi;
  PGStateInstance* pi = nullptr;
  boost::circular_buffer<std::unique_ptr<PGStateInstance>> buffer;

};

#ifdef PG_DEBUG_REFS
#include "common/tracked_int_ptr.hpp"
  uint64_t get_with_id(PG *pg);
  void put_with_id(PG *pg, uint64_t id);
  typedef TrackedIntPtr<PG> PGRef;
#else
  typedef boost::intrusive_ptr<PG> PGRef;
#endif

class PGRecoveryStats {
  struct per_state_info {
    uint64_t enter, exit;     // enter/exit counts
    uint64_t events;
    utime_t event_time;       // time spent processing events
    utime_t total_time;       // total time in state
    utime_t min_time, max_time;

    // cppcheck-suppress unreachableCode
    per_state_info() : enter(0), exit(0), events(0) {}
  };
  map<const char *,per_state_info> info;
  Mutex lock;

  public:
  PGRecoveryStats() : lock("PGRecoverStats::lock") {}

  void reset() {
    std::lock_guard l(lock);
    info.clear();
  }
  void dump(ostream& out) {
    std::lock_guard l(lock);
    for (map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); ++p) {
      per_state_info& i = p->second;
      out << i.enter << "\t" << i.exit << "\t"
	  << i.events << "\t" << i.event_time << "\t"
	  << i.total_time << "\t"
	  << i.min_time << "\t" << i.max_time << "\t"
	  << p->first << "\n";
    }
  }

  void dump_formatted(Formatter *f) {
    std::lock_guard l(lock);
    f->open_array_section("pg_recovery_stats");
    for (map<const char *,per_state_info>::iterator p = info.begin();
	 p != info.end(); ++p) {
      per_state_info& i = p->second;
      f->open_object_section("recovery_state");
      f->dump_int("enter", i.enter);
      f->dump_int("exit", i.exit);
      f->dump_int("events", i.events);
      f->dump_stream("event_time") << i.event_time;
      f->dump_stream("total_time") << i.total_time;
      f->dump_stream("min_time") << i.min_time;
      f->dump_stream("max_time") << i.max_time;
      vector<string> states;
      get_str_vec(p->first, "/", states);
      f->open_array_section("nested_states");
      for (vector<string>::iterator st = states.begin();
	   st != states.end(); ++st) {
	f->dump_string("state", *st);
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
  }

  void log_enter(const char *s) {
    std::lock_guard l(lock);
    info[s].enter++;
  }
  void log_exit(const char *s, utime_t dur, uint64_t events, utime_t event_dur) {
    std::lock_guard l(lock);
    per_state_info &i = info[s];
    i.exit++;
    i.total_time += dur;
    if (dur > i.max_time)
      i.max_time = dur;
    if (dur < i.min_time || i.min_time == utime_t())
      i.min_time = dur;
    i.events += events;
    i.event_time += event_dur;
  }
};

struct PGPool {
  CephContext* cct;
  epoch_t cached_epoch;
  int64_t id;
  string name;

  pg_pool_t info;      
  SnapContext snapc;   // the default pool snapc, ready to go.

  // these two sets are for < mimic only
  interval_set<snapid_t> cached_removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(CephContext* cct, OSDMapRef map, int64_t i, const pg_pool_t& info,
	 const string& name)
    : cct(cct),
      cached_epoch(map->get_epoch()),
      id(i),
      name(name),
      info(info) {
    snapc = info.get_snap_context();
    if (map->require_osd_release < CEPH_RELEASE_MIMIC) {
      info.build_removed_snaps(cached_removed_snaps);
    }
  }

  void update(CephContext *cct, OSDMapRef map);
};

/** PG - Replica Placement Group
 *
 */

class PG : public DoutPrefixProvider {
public:
  // -- members --
  const spg_t pg_id;
  const coll_t coll;

  ObjectStore::CollectionHandle ch;

  struct RecoveryCtx;

  // -- methods --
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext *get_cct() const override {
    return cct;
  }
  unsigned get_subsys() const override {
    return ceph_subsys_osd;
  }

  const OSDMapRef& get_osdmap() const {
    ceph_assert(is_locked());
    ceph_assert(osdmap_ref);
    return osdmap_ref;
  }
  epoch_t get_osdmap_epoch() const {
    return osdmap_ref->get_epoch();
  }

  void lock_suspend_timeout(ThreadPool::TPHandle &handle) {
    handle.suspend_tp_timeout();
    lock();
    handle.reset_tp_timeout();
  }
  void lock(bool no_lockdep = false) const;
  void unlock() const {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    ceph_assert(!dirty_info);
    ceph_assert(!dirty_big_info);
    _lock.Unlock();
  }
  bool is_locked() const {
    return _lock.is_locked();
  }

  const spg_t& get_pgid() const {
    return pg_id;
  }

  const PGPool& get_pool() const {
    return pool;
  }
  uint64_t get_last_user_version() const {
    return info.last_user_version;
  }
  const pg_history_t& get_history() const {
    return info.history;
  }
  bool get_need_up_thru() const {
    return need_up_thru;
  }
  epoch_t get_same_interval_since() const {
    return info.history.same_interval_since;
  }

  void set_last_scrub_stamp(utime_t t) {
    info.stats.last_scrub_stamp = t;
    info.history.last_scrub_stamp = t;
  }

  void set_last_deep_scrub_stamp(utime_t t) {
    info.stats.last_deep_scrub_stamp = t;
    info.history.last_deep_scrub_stamp = t;
  }

  bool is_deleting() const {
    return deleting;
  }
  bool is_deleted() const {
    return deleted;
  }
  bool is_replica() const {
    return role > 0;
  }
  bool is_primary() const {
    return pg_whoami == primary;
  }
  bool pg_has_reset_since(epoch_t e) {
    ceph_assert(is_locked());
    return deleted || e < get_last_peering_reset();
  }

  bool is_ec_pg() const {
    return pool.info.is_erasure();
  }
  int get_role() const {
    return role;
  }
  const vector<int> get_acting() const {
    return acting;
  }
  int get_acting_primary() const {
    return primary.osd;
  }
  pg_shard_t get_primary() const {
    return primary;
  }
  const vector<int> get_up() const {
    return up;
  }
  int get_up_primary() const {
    return up_primary.osd;
  }
  const PastIntervals& get_past_intervals() const {
    return past_intervals;
  }

  /// initialize created PG
  void init(
    int role,
    const vector<int>& up,
    int up_primary,
    const vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    bool backfill,
    ObjectStore::Transaction *t);

  /// read existing pg state off disk
  void read_state(ObjectStore *store);
  static int peek_map_epoch(ObjectStore *store, spg_t pgid, epoch_t *pepoch);

  static int get_latest_struct_v() {
    return latest_struct_v;
  }
  static int get_compat_struct_v() {
    return compat_struct_v;
  }
  static int read_info(
    ObjectStore *store, spg_t pgid, const coll_t &coll,
    pg_info_t &info, PastIntervals &past_intervals,
    __u8 &);
  static bool _has_removal_flag(ObjectStore *store, spg_t pgid);

  void rm_backoff(BackoffRef b);

  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
  void start_split_stats(const set<spg_t>& childpgs, vector<object_stat_sum_t> *v);
  virtual void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction *t) = 0;
  void split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void merge_from(map<spg_t,PGRef>& sources, RecoveryCtx *rctx,
		  unsigned split_bits,
		  const pg_merge_meta_t& last_pg_merge_meta);
  void finish_split_stats(const object_stat_sum_t& stats, ObjectStore::Transaction *t);

  void scrub(epoch_t queued, ThreadPool::TPHandle &handle);
  void reg_next_scrub();
  void unreg_next_scrub();

  bool is_forced_recovery_or_backfill() const {
    return get_state() & (PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);
  }
  bool set_force_recovery(bool b);
  bool set_force_backfill(bool b);

  void queue_peering_event(PGPeeringEventRef evt);
  void do_peering_event(PGPeeringEventRef evt, RecoveryCtx *rcx);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(
    OSDMapRef osdmap, OSDMapRef lastmap,
    vector<int>& newup, int up_primary,
    vector<int>& newacting, int acting_primary,
    RecoveryCtx *rctx);
  void handle_activate_map(RecoveryCtx *rctx);
  void handle_initialize(RecoveryCtx *rctx);
  void handle_query_state(Formatter *f);

  /**
   * @param ops_begun returns how many recovery ops the function started
   * @returns true if any useful work was accomplished; false otherwise
   */
  virtual bool start_recovery_ops(
    uint64_t max,
    ThreadPool::TPHandle &handle,
    uint64_t *ops_begun) = 0;

  // more work after the above, but with a RecoveryCtx
  void find_unfound(epoch_t queued, RecoveryCtx *rctx);

  virtual void get_watchers(std::list<obj_watch_item_t> *ls) = 0;

  void dump_pgstate_history(Formatter *f);
  void dump_missing(Formatter *f);

  void get_pg_stats(std::function<void(const pg_stat_t&, epoch_t lec)> f);
  void with_heartbeat_peers(std::function<void(int)> f);

  void shutdown();
  virtual void on_shutdown() = 0;

  bool get_must_scrub() const {
    return scrubber.must_scrub;
  }
  bool sched_scrub();

  virtual void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle
  ) = 0;
  virtual void clear_cache() = 0;
  virtual int get_cache_obj_count() = 0;

  virtual void snap_trimmer(epoch_t epoch_queued) = 0;
  virtual int do_command(
    cmdmap_t cmdmap,
    ostream& ss,
    bufferlist& idata,
    bufferlist& odata,
    ConnectionRef conn,
    ceph_tid_t tid) = 0;

  virtual bool agent_work(int max) = 0;
  virtual bool agent_work(int max, int agent_flush_quota) = 0;
  virtual void agent_stop() = 0;
  virtual void agent_delay() = 0;
  virtual void agent_clear() = 0;
  virtual void agent_choose_mode_restart() = 0;

  virtual void on_removal(ObjectStore::Transaction *t) = 0;

  void _delete_some(ObjectStore::Transaction *t);

  virtual void set_dynamic_perf_stats_queries(
    const std::list<OSDPerfMetricQuery> &queries) {
  }
  virtual void get_dynamic_perf_stats(DynamicPerfStats *stats) {
  }

  // reference counting
#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  void get(const char* tag);
  void put(const char* tag);
  int get_num_ref() {
    return ref;
  }

  // ctor
  PG(OSDService *o, OSDMapRef curmap,
     const PGPool &pool, spg_t p);
  ~PG() override;

  // prevent copying
  explicit PG(const PG& rhs) = delete;
  PG& operator=(const PG& rhs) = delete;

protected:
  // -------------
  // protected
  OSDService *osd;
public:
  OSDShard *osd_shard = nullptr;
  OSDShardPGSlot *pg_slot = nullptr;
protected:
  CephContext *cct;

  // osdmap
  OSDMapRef osdmap_ref;

  PGPool pool;

  // locking and reference counting.
  // I destroy myself when the reference count hits zero.
  // lock() should be called before doing anything.
  // get() should be called on pointer copy (to another thread, etc.).
  // put() should be called on destruction of some previously copied pointer.
  // unlock() when done with the current pointer (_most common_).
  mutable Mutex _lock = {"PG::_lock"};

  std::atomic<unsigned int> ref{0};

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock = {"PG::_ref_id_lock"};
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id = 0;

  friend uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  friend void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

private:
  friend void intrusive_ptr_add_ref(PG *pg) {
    pg->get("intptr");
  }
  friend void intrusive_ptr_release(PG *pg) {
    pg->put("intptr");
  }


  // =====================

protected:
  OSDriver osdriver;
  SnapMapper snap_mapper;
  bool eio_errors_to_process = false;

  virtual PGBackend *get_pgbackend() = 0;
  virtual const PGBackend* get_pgbackend() const = 0;

protected:
  /*** PG ****/
  /// get_is_recoverable_predicate: caller owns returned pointer and must delete when done
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const {
    return get_pgbackend()->get_is_recoverable_predicate();
  }
protected:
  epoch_t last_persisted_osdmap;

  void requeue_map_waiters();

  void update_osdmap_ref(OSDMapRef newmap) {
    ceph_assert(_lock.is_locked_by_me());
    osdmap_ref = std::move(newmap);
  }

protected:


  bool deleting;  // true while in removing or OSD is shutting down
  atomic<bool> deleted = {false};

  ZTracer::Endpoint trace_endpoint;


protected:
  bool dirty_info, dirty_big_info;

protected:
  // pg state
  pg_info_t info;               ///< current pg info
  pg_info_t last_written_info;  ///< last written info
  __u8 info_struct_v = 0;
  static const __u8 latest_struct_v = 10;
  // v10 is the new past_intervals encoding
  // v9 was fastinfo_key addition
  // v8 was the move to a per-pg pgmeta object
  // v7 was SnapMapper addition in 86658392516d5175b2756659ef7ffaaf95b0f8ad
  // (first appeared in cuttlefish).
  static const __u8 compat_struct_v = 10;
  void upgrade(ObjectStore *store);

protected:
  PGLog  pg_log;
  ghobject_t    pgmeta_oid;

  // ------------------
  // MissingLoc
  
  class MissingLoc {
  public:
    // a loc_count indicates how many locations we know in each of
    // these distinct sets
    struct loc_count_t {
      int up = 0;        //< up
      int other = 0;    //< other

      friend bool operator<(const loc_count_t& l,
			    const loc_count_t& r) {
	return (l.up < r.up ||
		(l.up == r.up &&
		   (l.other < r.other)));
      }
      friend ostream& operator<<(ostream& out, const loc_count_t& l) {
	ceph_assert(l.up >= 0);
	ceph_assert(l.other >= 0);
	return out << "(" << l.up << "+" << l.other << ")";
      }
    };


  private:

    loc_count_t _get_count(const set<pg_shard_t>& shards) {
      loc_count_t r;
      for (auto s : shards) {
        if (pg->upset.count(s)) {
	  r.up++;
	} else {
	  r.other++;
	}
      }
      return r;
    }

    map<hobject_t, pg_missing_item> needs_recovery_map;
    map<hobject_t, set<pg_shard_t> > missing_loc;
    set<pg_shard_t> missing_loc_sources;

    // for every entry in missing_loc, we count how many of each type of shard we have,
    // and maintain totals here.  The sum of the values for this map will always equal
    // missing_loc.size().
    map < shard_id_t, map<loc_count_t,int> > missing_by_count;

   void pgs_by_shard_id(const set<pg_shard_t>& s, map< shard_id_t, set<pg_shard_t> >& pgsbs) {
      if (pg->get_osdmap()->pg_is_ec(pg->info.pgid.pgid)) {
        int num_shards = pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid);
        // For completely missing shards initialize with empty set<pg_shard_t>
	for (int i = 0 ; i < num_shards ; ++i) {
	  shard_id_t shard(i);
	  pgsbs[shard];
	}
	for (auto pgs: s)
	  pgsbs[pgs.shard].insert(pgs);
      } else {
        pgsbs[shard_id_t::NO_SHARD] = s;
      }
    }

    void _inc_count(const set<pg_shard_t>& s) {
      map< shard_id_t, set<pg_shard_t> > pgsbs;
      pgs_by_shard_id(s, pgsbs);
      for (auto shard: pgsbs)
        ++missing_by_count[shard.first][_get_count(shard.second)];
    }
    void _dec_count(const set<pg_shard_t>& s) {
      map< shard_id_t, set<pg_shard_t> > pgsbs;
      pgs_by_shard_id(s, pgsbs);
      for (auto shard: pgsbs) {
        auto p = missing_by_count[shard.first].find(_get_count(shard.second));
        ceph_assert(p != missing_by_count[shard.first].end());
        if (--p->second == 0) {
	  missing_by_count[shard.first].erase(p);
        }
      }
    }

    PG *pg;
    set<pg_shard_t> empty_set;
  public:
    boost::scoped_ptr<IsPGReadablePredicate> is_readable;
    boost::scoped_ptr<IsPGRecoverablePredicate> is_recoverable;
    explicit MissingLoc(PG *pg)
      : pg(pg) { }
    void set_backend_predicates(
      IsPGReadablePredicate *_is_readable,
      IsPGRecoverablePredicate *_is_recoverable) {
      is_readable.reset(_is_readable);
      is_recoverable.reset(_is_recoverable);
    }
    std::ostream& gen_prefix(std::ostream& out) const {
      return pg->gen_prefix(out);
    }
    bool needs_recovery(
      const hobject_t &hoid,
      eversion_t *v = 0) const {
      map<hobject_t, pg_missing_item>::const_iterator i =
	needs_recovery_map.find(hoid);
      if (i == needs_recovery_map.end())
	return false;
      if (v)
	*v = i->second.need;
      return true;
    }
    bool is_deleted(const hobject_t &hoid) const {
      auto i = needs_recovery_map.find(hoid);
      if (i == needs_recovery_map.end())
	return false;
      return i->second.is_delete();
    }
    bool is_unfound(const hobject_t &hoid) const {
      auto it = needs_recovery_map.find(hoid);
      if (it == needs_recovery_map.end()) {
        return false;
      }
      if (it->second.is_delete()) {
        return false;
      }
      auto mit = missing_loc.find(hoid);
      return mit == missing_loc.end() || !(*is_recoverable)(mit->second);
    }
    bool readable_with_acting(
      const hobject_t &hoid,
      const set<pg_shard_t> &acting) const;
    uint64_t num_unfound() const {
      uint64_t ret = 0;
      for (map<hobject_t, pg_missing_item>::const_iterator i =
	     needs_recovery_map.begin();
	   i != needs_recovery_map.end();
	   ++i) {
	if (i->second.is_delete())
	  continue;
	auto mi = missing_loc.find(i->first);
	if (mi == missing_loc.end() || !(*is_recoverable)(mi->second))
	  ++ret;
      }
      return ret;
    }

    bool have_unfound() const {
      for (map<hobject_t, pg_missing_item>::const_iterator i =
	     needs_recovery_map.begin();
	   i != needs_recovery_map.end();
	   ++i) {
        if (i->second.is_delete())
          continue;
        auto mi = missing_loc.find(i->first);
        if (mi == missing_loc.end() || !(*is_recoverable)(mi->second))
	  return true;
      }
      return false;
    }
    void clear() {
      needs_recovery_map.clear();
      missing_loc.clear();
      missing_loc_sources.clear();
      missing_by_count.clear();
    }

    void add_location(const hobject_t &hoid, pg_shard_t location) {
      auto p = missing_loc.find(hoid);
      if (p == missing_loc.end()) {
	p = missing_loc.emplace(hoid, set<pg_shard_t>()).first;
      } else {
	_dec_count(p->second);
      }
      p->second.insert(location);
      _inc_count(p->second);
    }
    void remove_location(const hobject_t &hoid, pg_shard_t location) {
      auto p = missing_loc.find(hoid);
      if (p != missing_loc.end()) {
	_dec_count(p->second);
	p->second.erase(location);
        if (p->second.empty()) {
          missing_loc.erase(p);
        } else {
          _inc_count(p->second);
        }
      }
    }

    void clear_location(const hobject_t &hoid) {
      auto p = missing_loc.find(hoid);
      if (p != missing_loc.end()) {
	_dec_count(p->second);
        missing_loc.erase(p);
      }
    }

    void add_active_missing(const pg_missing_t &missing) {
      for (map<hobject_t, pg_missing_item>::const_iterator i =
	     missing.get_items().begin();
	   i != missing.get_items().end();
	   ++i) {
	map<hobject_t, pg_missing_item>::const_iterator j =
	  needs_recovery_map.find(i->first);
	if (j == needs_recovery_map.end()) {
	  needs_recovery_map.insert(*i);
	} else {
	  lgeneric_dout(pg->cct, 0) << this << " " << pg->info.pgid << " unexpected need for "
				    << i->first << " have " << j->second
				    << " tried to add " << i->second << dendl;
	  ceph_assert(i->second.need == j->second.need);
	}
      }
    }

    void add_missing(const hobject_t &hoid, eversion_t need, eversion_t have, bool is_delete=false) {
      needs_recovery_map[hoid] = pg_missing_item(need, have, is_delete);
    }
    void revise_need(const hobject_t &hoid, eversion_t need) {
      auto it = needs_recovery_map.find(hoid);
      ceph_assert(it != needs_recovery_map.end());
      it->second.need = need;
    }

    /// Adds info about a possible recovery source
    bool add_source_info(
      pg_shard_t source,           ///< [in] source
      const pg_info_t &oinfo,      ///< [in] info
      const pg_missing_t &omissing, ///< [in] (optional) missing
      ThreadPool::TPHandle* handle  ///< [in] ThreadPool handle
      ); ///< @return whether a new object location was discovered

    /// Adds recovery sources in batch
    void add_batch_sources_info(
      const set<pg_shard_t> &sources,  ///< [in] a set of resources which can be used for all objects
      ThreadPool::TPHandle* handle  ///< [in] ThreadPool handle
      );

    /// Uses osdmap to update structures for now down sources
    void check_recovery_sources(const OSDMapRef& osdmap);

    /// Call when hoid is no longer missing in acting set
    void recovered(const hobject_t &hoid) {
      needs_recovery_map.erase(hoid);
      auto p = missing_loc.find(hoid);
      if (p != missing_loc.end()) {
	_dec_count(p->second);
	missing_loc.erase(p);
      }
    }

    /// Call to update structures for hoid after a change
    void rebuild(
      const hobject_t &hoid,
      pg_shard_t self,
      const set<pg_shard_t> to_recover,
      const pg_info_t &info,
      const pg_missing_t &missing,
      const map<pg_shard_t, pg_missing_t> &pmissing,
      const map<pg_shard_t, pg_info_t> &pinfo) {
      recovered(hoid);
      boost::optional<pg_missing_item> item;
      auto miter = missing.get_items().find(hoid);
      if (miter != missing.get_items().end()) {
	item = miter->second;
      } else {
	for (auto &&i: to_recover) {
	  if (i == self)
	    continue;
	  auto pmiter = pmissing.find(i);
	  ceph_assert(pmiter != pmissing.end());
	  miter = pmiter->second.get_items().find(hoid);
	  if (miter != pmiter->second.get_items().end()) {
	    item = miter->second;
	    break;
	  }
	}
      }
      if (!item)
	return; // recovered!

      needs_recovery_map[hoid] = *item;
      if (item->is_delete())
	return;
      auto mliter =
	missing_loc.emplace(hoid, set<pg_shard_t>()).first;
      ceph_assert(info.last_backfill.is_max());
      ceph_assert(info.last_update >= item->need);
      if (!missing.is_missing(hoid))
	mliter->second.insert(self);
      for (auto &&i: pmissing) {
	if (i.first == self)
	  continue;
	auto pinfoiter = pinfo.find(i.first);
	ceph_assert(pinfoiter != pinfo.end());
	if (item->need <= pinfoiter->second.last_update &&
	    hoid <= pinfoiter->second.last_backfill &&
	    !i.second.is_missing(hoid))
	  mliter->second.insert(i.first);
      }
      _inc_count(mliter->second);
    }

    const set<pg_shard_t> &get_locations(const hobject_t &hoid) const {
      auto it = missing_loc.find(hoid);
      return it == missing_loc.end() ? empty_set : it->second;
    }
    const map<hobject_t, set<pg_shard_t>> &get_missing_locs() const {
      return missing_loc;
    }
    const map<hobject_t, pg_missing_item> &get_needs_recovery() const {
      return needs_recovery_map;
    }
    const map < shard_id_t, map<loc_count_t,int> > &get_missing_by_count() const {
      return missing_by_count;
    }
  } missing_loc;
  
  PastIntervals past_intervals;

  interval_set<snapid_t> snap_trimq;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item stat_queue_item;
  bool scrub_queued;
  bool recovery_queued;

  int recovery_ops_active;
  set<pg_shard_t> waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  multiset<hobject_t> recovering_oids;
#endif

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  uint64_t    state;   // PG_STATE_*

  bool send_notify;    ///< true if we are non-primary and should notify the primary

protected:
  eversion_t  last_update_ondisk;    // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_complete_ondisk;  // last_complete that has committed.
  eversion_t  last_update_applied;

  // entries <= last_rollback_info_trimmed_to_applied have been trimmed
  eversion_t  last_rollback_info_trimmed_to_applied;

  // primary state
protected:
  pg_shard_t primary;
  pg_shard_t pg_whoami;
  pg_shard_t up_primary;
  vector<int> up, acting, want_acting;
  // acting_recovery_backfill contains shards that are acting,
  // async recovery targets, or backfill targets.
  set<pg_shard_t> acting_recovery_backfill, actingset, upset;
  map<pg_shard_t,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  set<int> blocked_by; ///< osds we are blocked by (for pg stats)

protected:
  // [primary only] content recovery state
  struct BufferedRecoveryMessages {
    map<int, map<spg_t, pg_query_t> > query_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > info_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > notify_list;
  };

public:
  bool dne() { return info.dne(); }
  struct RecoveryCtx {
    utime_t start_time;
    map<int, map<spg_t, pg_query_t> > *query_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *info_map;
    map<int, vector<pair<pg_notify_t, PastIntervals> > > *notify_list;
    ObjectStore::Transaction *transaction;
    ThreadPool::TPHandle* handle;
    RecoveryCtx(map<int, map<spg_t, pg_query_t> > *query_map,
		map<int,
		    vector<pair<pg_notify_t, PastIntervals> > > *info_map,
		map<int,
		    vector<pair<pg_notify_t, PastIntervals> > > *notify_list,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map), 
	notify_list(notify_list),
	transaction(transaction),
        handle(NULL) {}

    RecoveryCtx(BufferedRecoveryMessages &buf, RecoveryCtx &rctx)
      : query_map(&(buf.query_map)),
	info_map(&(buf.info_map)),
	notify_list(&(buf.notify_list)),
	transaction(rctx.transaction),
        handle(rctx.handle) {}

    void accept_buffered_messages(BufferedRecoveryMessages &m) {
      ceph_assert(query_map);
      ceph_assert(info_map);
      ceph_assert(notify_list);
      for (map<int, map<spg_t, pg_query_t> >::iterator i = m.query_map.begin();
	   i != m.query_map.end();
	   ++i) {
	map<spg_t, pg_query_t> &omap = (*query_map)[i->first];
	for (map<spg_t, pg_query_t>::iterator j = i->second.begin();
	     j != i->second.end();
	     ++j) {
	  omap[j->first] = j->second;
	}
      }
      for (map<int, vector<pair<pg_notify_t, PastIntervals> > >::iterator i
	     = m.info_map.begin();
	   i != m.info_map.end();
	   ++i) {
	vector<pair<pg_notify_t, PastIntervals> > &ovec =
	  (*info_map)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
      for (map<int, vector<pair<pg_notify_t, PastIntervals> > >::iterator i
	     = m.notify_list.begin();
	   i != m.notify_list.end();
	   ++i) {
	vector<pair<pg_notify_t, PastIntervals> > &ovec =
	  (*notify_list)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
    }

    void send_notify(pg_shard_t to,
		     const pg_notify_t &info, const PastIntervals &pi) {
      ceph_assert(notify_list);
      (*notify_list)[to.osd].emplace_back(info, pi);
    }
  };
protected:

  PGStateHistory pgstate_history;

  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    PG* pg;
    const char *get_state_name() { return state_name; }
    NamedState(PG *pg_, const char *state_name_)
      : state_name(state_name_), enter_time(ceph_clock_now()), pg(pg_) {
        pg->pgstate_history.enter(pg, enter_time, state_name);
      }
    virtual ~NamedState() { pg->pgstate_history.exit(state_name); }
  };



protected:

  /*
   * peer_info    -- projected (updates _before_ replicas ack)
   * peer_missing -- committed (updates _after_ replicas ack)
   */
  
  bool        need_up_thru;
  set<pg_shard_t>    stray_set;   // non-acting osds that have PG data.
  map<pg_shard_t, pg_info_t>    peer_info;   // info from peers (stray or prior)
  map<pg_shard_t, int64_t>    peer_bytes;   // Peer's num_bytes from peer_info
  set<pg_shard_t> peer_purged; // peers purged
  map<pg_shard_t, pg_missing_t> peer_missing;
  set<pg_shard_t> peer_log_requested;  // logs i've requested (and start stamps)
  set<pg_shard_t> peer_missing_requested;

  // i deleted these strays; ignore racing PGInfo from them
  set<pg_shard_t> peer_activated;

  // primary-only, recovery-only state
  set<pg_shard_t> might_have_unfound;  // These osds might have objects on them
                                       // which are unfound on the primary
  epoch_t last_peering_reset;

  epoch_t get_last_peering_reset() const {
    return last_peering_reset;
  }

  /* heartbeat peers */
  void set_probe_targets(const set<pg_shard_t> &probe_set);
  void clear_probe_targets();

  Mutex heartbeat_peer_lock;
  set<int> heartbeat_peers;
  set<int> probe_targets;

public:
  /**
   * BackfillInterval
   *
   * Represents the objects in a range [begin, end)
   *
   * Possible states:
   * 1) begin == end == hobject_t() indicates the the interval is unpopulated
   * 2) Else, objects contains all objects in [begin, end)
   */
  struct BackfillInterval {
    // info about a backfill interval on a peer
    eversion_t version; /// version at which the scan occurred
    map<hobject_t,eversion_t> objects;
    hobject_t begin;
    hobject_t end;

    /// clear content
    void clear() {
      *this = BackfillInterval();
    }

    /// clear objects list only
    void clear_objects() {
      objects.clear();
    }

    /// reinstantiate with a new start+end position and sort order
    void reset(hobject_t start) {
      clear();
      begin = end = start;
    }

    /// true if there are no objects in this interval
    bool empty() const {
      return objects.empty();
    }

    /// true if interval extends to the end of the range
    bool extends_to_end() const {
      return end.is_max();
    }

    /// removes items <= soid and adjusts begin to the first object
    void trim_to(const hobject_t &soid) {
      trim();
      while (!objects.empty() &&
	     objects.begin()->first <= soid) {
	pop_front();
      }
    }

    /// Adjusts begin to the first object
    void trim() {
      if (!objects.empty())
	begin = objects.begin()->first;
      else
	begin = end;
    }

    /// drop first entry, and adjust @begin accordingly
    void pop_front() {
      ceph_assert(!objects.empty());
      objects.erase(objects.begin());
      trim();
    }

    /// dump
    void dump(Formatter *f) const {
      f->dump_stream("begin") << begin;
      f->dump_stream("end") << end;
      f->open_array_section("objects");
      for (map<hobject_t, eversion_t>::const_iterator i =
	     objects.begin();
	   i != objects.end();
	   ++i) {
	f->open_object_section("object");
	f->dump_stream("object") << i->first;
	f->dump_stream("version") << i->second;
	f->close_section();
      }
      f->close_section();
    }
  };

protected:
  BackfillInterval backfill_info;
  map<pg_shard_t, BackfillInterval> peer_backfill_info;
  bool backfill_reserved;
  bool backfill_reserving;

  set<pg_shard_t> backfill_targets,  async_recovery_targets;

  // The primary's num_bytes and local num_bytes for this pg, only valid
  // during backfill for non-primary shards.
  // Both of these are adjusted for EC to reflect the on-disk bytes
  std::atomic<int64_t> primary_num_bytes = 0;
  std::atomic<int64_t> local_num_bytes = 0;

public:
  bool is_backfill_targets(pg_shard_t osd) {
    return backfill_targets.count(osd);
  }

  // Space reserved for backfill is primary_num_bytes - local_num_bytes
  // Don't care that difference itself isn't atomic
  uint64_t get_reserved_num_bytes() {
    int64_t primary = primary_num_bytes.load();
    int64_t local = local_num_bytes.load();
    if (primary > local)
      return primary - local;
    else
      return 0;
  }

  bool is_remote_backfilling() {
    return primary_num_bytes.load() > 0;
  }

  void set_reserved_num_bytes(int64_t primary, int64_t local);
  void clear_reserved_num_bytes();

  // If num_bytes are inconsistent and local_num- goes negative
  // it's ok, because it would then be ignored.

  // The value of num_bytes could be negative,
  // but we don't let local_num_bytes go negative.
  void add_local_num_bytes(int64_t num_bytes) {
    if (num_bytes) {
      int64_t prev_bytes = local_num_bytes.load();
      int64_t new_bytes;
      do {
        new_bytes = prev_bytes + num_bytes;
        if (new_bytes < 0)
          new_bytes = 0;
      } while(!local_num_bytes.compare_exchange_weak(prev_bytes, new_bytes));
    }
  }
  void sub_local_num_bytes(int64_t num_bytes) {
    ceph_assert(num_bytes >= 0);
    if (num_bytes) {
      int64_t prev_bytes = local_num_bytes.load();
      int64_t new_bytes;
      do {
        new_bytes = prev_bytes - num_bytes;
        if (new_bytes < 0)
          new_bytes = 0;
      } while(!local_num_bytes.compare_exchange_weak(prev_bytes, new_bytes));
    }
  }
  // The value of num_bytes could be negative,
  // but we don't let info.stats.stats.sum.num_bytes go negative.
  void add_num_bytes(int64_t num_bytes) {
    ceph_assert(_lock.is_locked_by_me());
    if (num_bytes) {
      info.stats.stats.sum.num_bytes += num_bytes;
      if (info.stats.stats.sum.num_bytes < 0) {
        info.stats.stats.sum.num_bytes = 0;
      }
    }
  }
  void sub_num_bytes(int64_t num_bytes) {
    ceph_assert(_lock.is_locked_by_me());
    ceph_assert(num_bytes >= 0);
    if (num_bytes) {
      info.stats.stats.sum.num_bytes -= num_bytes;
      if (info.stats.stats.sum.num_bytes < 0) {
        info.stats.stats.sum.num_bytes = 0;
      }
    }
  }

  // Only used in testing so not worried about needing the PG lock here
  int64_t get_stats_num_bytes() {
    Mutex::Locker l(_lock);
    int num_bytes = info.stats.stats.sum.num_bytes;
    if (pool.info.is_erasure()) {
      num_bytes /= (int)get_pgbackend()->get_ec_data_chunk_count();
      // Round up each object by a stripe
      num_bytes +=  get_pgbackend()->get_ec_stripe_chunk_size() * info.stats.stats.sum.num_objects;
    }
    int64_t lnb = local_num_bytes.load();
    if (lnb && lnb != num_bytes) {
      lgeneric_dout(cct, 0) << this << " " << info.pgid << " num_bytes mismatch "
			    << lnb << " vs stats "
                            << info.stats.stats.sum.num_bytes << " / chunk "
                            << get_pgbackend()->get_ec_data_chunk_count()
                            << dendl;
    }
    return num_bytes;
  }

protected:

  /*
   * blocked request wait hierarchy
   *
   * In order to preserve request ordering we need to be careful about the
   * order in which blocked requests get requeued.  Generally speaking, we
   * push the requests back up to the op_wq in reverse order (most recent
   * request first) so that they come back out again in the original order.
   * However, because there are multiple wait queues, we need to requeue
   * waitlists in order.  Generally speaking, we requeue the wait lists
   * that are checked first.
   *
   * Here are the various wait lists, in the order they are used during
   * request processing, with notes:
   *
   *  - waiting_for_map
   *    - may start or stop blocking at any time (depending on client epoch)
   *  - waiting_for_peered
   *    - !is_peered()
   *    - only starts blocking on interval change; never restarts
   *  - waiting_for_flush
   *    - flushes_in_progress
   *    - waiting for final flush during activate
   *  - waiting_for_active
   *    - !is_active()
   *    - only starts blocking on interval change; never restarts
   *  - waiting_for_scrub
   *    - starts and stops blocking for varying intervals during scrub
   *  - waiting_for_unreadable_object
   *    - never restarts once object is readable (* except for EIO?)
   *  - waiting_for_degraded_object
   *    - never restarts once object is writeable (* except for EIO?)
   *  - waiting_for_blocked_object
   *    - starts and stops based on proxied op activity
   *  - obc rwlocks
   *    - starts and stops based on read/write activity
   *
   * Notes:
   *
   *  1. During and interval change, we requeue *everything* in the above order.
   *
   *  2. When an obc rwlock is released, we check for a scrub block and requeue
   *     the op there if it applies.  We ignore the unreadable/degraded/blocked
   *     queues because we assume they cannot apply at that time (this is
   *     probably mostly true).
   *
   *  3. The requeue_ops helper will push ops onto the waiting_for_map list if
   *     it is non-empty.
   *
   * These three behaviors are generally sufficient to maintain ordering, with
   * the possible exception of cases where we make an object degraded or
   * unreadable that was previously okay, e.g. when scrub or op processing
   * encounter an unexpected error.  FIXME.
   */

  // pg waiters
  unsigned flushes_in_progress;

  // ops with newer maps than our (or blocked behind them)
  // track these by client, since inter-request ordering doesn't otherwise
  // matter.
  unordered_map<entity_name_t,list<OpRequestRef>> waiting_for_map;

  // ops waiting on peered
  list<OpRequestRef>            waiting_for_peered;

  // ops waiting on active (require peered as well)
  list<OpRequestRef>            waiting_for_active;
  list<OpRequestRef>            waiting_for_flush;
  list<OpRequestRef>            waiting_for_scrub;

  list<OpRequestRef>            waiting_for_cache_not_full;
  list<OpRequestRef>            waiting_for_clean_to_primary_repair;
  map<hobject_t, list<OpRequestRef>> waiting_for_unreadable_object,
			     waiting_for_degraded_object,
			     waiting_for_blocked_object;

  set<hobject_t> objects_blocked_on_cache_full;
  map<hobject_t,snapid_t> objects_blocked_on_degraded_snap;
  map<hobject_t,ObjectContextRef> objects_blocked_on_snap_promotion;

  // Callbacks should assume pg (and nothing else) is locked
  map<hobject_t, list<Context*>> callbacks_for_degraded_object;

  map<eversion_t,
      list<tuple<OpRequestRef, version_t, int> > > waiting_for_ondisk;

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef>>& m);
  void requeue_op(OpRequestRef op);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  void _update_calc_stats();
  void _update_blocked_by();
  friend class TestOpsSocketHook;
  void publish_stats_to_osd();
  void clear_publish_stats();

  void clear_primary_state();

  bool is_acting_recovery_backfill(pg_shard_t osd) const {
    return acting_recovery_backfill.count(osd);
  }
  bool is_acting(pg_shard_t osd) const {
    return has_shard(pool.info.is_erasure(), acting, osd);
  }
  bool is_up(pg_shard_t osd) const {
    return has_shard(pool.info.is_erasure(), up, osd);
  }
  static bool has_shard(bool ec, const vector<int>& v, pg_shard_t osd) {
    if (ec) {
      return v.size() > (unsigned)osd.shard && v[osd.shard] == osd.osd;
    } else {
      return std::find(v.begin(), v.end(), osd.osd) != v.end();
    }
  }
  
  bool needs_recovery() const;
  bool needs_backfill() const;

  /// clip calculated priority to reasonable range
  inline int clamp_recovery_priority(int priority);
  /// get log recovery reservation priority
  unsigned get_recovery_priority();
  /// get backfill reservation priority
  unsigned get_backfill_priority();
  /// get priority for pg deletion
  unsigned get_delete_priority();

  void try_mark_clean();  ///< mark an active pg clean

  /// return [start,end) bounds for required past_intervals
  static pair<epoch_t, epoch_t> get_required_past_interval_bounds(
    const pg_info_t &info,
    epoch_t oldest_map) {
    epoch_t start = std::max(
      info.history.last_epoch_clean ? info.history.last_epoch_clean :
       info.history.epoch_pool_created,
      oldest_map);
    epoch_t end = std::max(
      info.history.same_interval_since,
      info.history.epoch_pool_created);
    return make_pair(start, end);
  }
  void check_past_interval_bounds() const;
  PastIntervals::PriorSet build_prior();

  void remove_down_peer_info(const OSDMapRef osdmap);

  bool adjust_need_up_thru(const OSDMapRef osdmap);

  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  virtual void dump_recovery_info(Formatter *f) const = 0;

  void calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    ceph_assert(!acting_recovery_backfill.empty());
    for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      if (peer_last_complete_ondisk.count(*i) == 0)
	return;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[*i];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return;
    min_last_complete_ondisk = min;
    return;
  }

  virtual void calc_trim_to() = 0;

  virtual void calc_trim_to_aggressive() = 0;

  void proc_replica_log(pg_info_t &oinfo, const pg_log_t &olog,
			pg_missing_t& omissing, pg_shard_t from);
  void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, pg_shard_t from);
  bool proc_replica_info(
    pg_shard_t from, const pg_info_t &info, epoch_t send_epoch);

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    PG *pg;
    ObjectStore::Transaction *t;
    PGLogEntryHandler(PG *pg, ObjectStore::Transaction *t) : pg(pg), t(t) {}

    // LogEntryHandler
    void remove(const hobject_t &hoid) override {
      pg->get_pgbackend()->remove(hoid, t);
    }
    void try_stash(const hobject_t &hoid, version_t v) override {
      pg->get_pgbackend()->try_stash(hoid, v, t);
    }
    void rollback(const pg_log_entry_t &entry) override {
      ceph_assert(entry.can_rollback());
      pg->get_pgbackend()->rollback(entry, t);
    }
    void rollforward(const pg_log_entry_t &entry) override {
      pg->get_pgbackend()->rollforward(entry, t);
    }
    void trim(const pg_log_entry_t &entry) override {
      pg->get_pgbackend()->trim(entry, t);
    }
  };
  
  void update_object_snap_mapping(
    ObjectStore::Transaction *t, const hobject_t &soid,
    const set<snapid_t> &snaps);
  void clear_object_snap_mapping(
    ObjectStore::Transaction *t, const hobject_t &soid);
  void remove_snap_mapped_object(
    ObjectStore::Transaction& t, const hobject_t& soid);
  void merge_log(
    ObjectStore::Transaction& t, pg_info_t &oinfo,
    pg_log_t &olog, pg_shard_t from);
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead);
  bool search_for_missing(
    const pg_info_t &oinfo, const pg_missing_t &omissing,
    pg_shard_t fromosd,
    RecoveryCtx*);

  void discover_all_missing(std::map<int, map<spg_t,pg_query_t> > &query_map);
  
  map<pg_shard_t, pg_info_t>::const_iterator find_best_info(
    const map<pg_shard_t, pg_info_t> &infos,
    bool restrict_to_up_acting,
    bool *history_les_bound) const;
  static void calc_ec_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    ostream &ss);
  static void calc_replicated_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    uint64_t force_auth_primary_missing_objects,
    unsigned size,
    const vector<int> &acting,
    const vector<int> &up,
    pg_shard_t up_primary,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool restrict_to_up_acting,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    const OSDMapRef osdmap,
    ostream &ss);
  void choose_async_recovery_ec(const map<pg_shard_t, pg_info_t> &all_info,
                                const pg_info_t &auth_info,
                                vector<int> *want,
                                set<pg_shard_t> *async_recovery) const;
  void choose_async_recovery_replicated(const map<pg_shard_t, pg_info_t> &all_info,
                                        const pg_info_t &auth_info,
                                        vector<int> *want,
                                        set<pg_shard_t> *async_recovery) const;

  bool recoverable_and_ge_min_size(const vector<int> &want) const;
  bool choose_acting(pg_shard_t &auth_log_shard,
		     bool restrict_to_up_acting,
		     bool *history_les_bound);
  void build_might_have_unfound();
  void activate(
    ObjectStore::Transaction& t,
    epoch_t activation_epoch,
    map<int, map<spg_t,pg_query_t> >& query_map,
    map<int,
      vector<pair<pg_notify_t, PastIntervals> > > *activator_map,
    RecoveryCtx *ctx);

  struct C_PG_ActivateCommitted : public Context {
    PGRef pg;
    epoch_t epoch;
    epoch_t activation_epoch;
    C_PG_ActivateCommitted(PG *p, epoch_t e, epoch_t ae)
      : pg(p), epoch(e), activation_epoch(ae) {}
    void finish(int r) override {
      pg->_activate_committed(epoch, activation_epoch);
    }
  };
  void _activate_committed(epoch_t epoch, epoch_t activation_epoch);
  void all_activated_and_committed();

  void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);

  bool have_unfound() const { 
    return missing_loc.have_unfound();
  }
  uint64_t get_num_unfound() const {
    return missing_loc.num_unfound();
  }
  bool all_missing_unfound() const {
    const auto& missing = pg_log.get_missing();
    if (!missing.have_missing())
      return false;
    for (auto& m : missing.get_items()) {
      if (!missing_loc.is_unfound(m.first))
        return false;
    }
    return true;
  }

  virtual void check_local() = 0;

  void purge_strays();

  void update_heartbeat_peers();

  Context *finish_sync_event;

  Context *finish_recovery();
  void _finish_recovery(Context *c);
  struct C_PG_FinishRecovery : public Context {
    PGRef pg;
    explicit C_PG_FinishRecovery(PG *p) : pg(p) {}
    void finish(int r) override {
      pg->_finish_recovery(this);
    }
  };
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  virtual void check_recovery_sources(const OSDMapRef& newmap) = 0;
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits) = 0;

  friend class C_OSD_RepModify_Commit;
  friend class C_DeleteMore;

  // -- backoff --
  Mutex backoff_lock;  // orders inside Backoff::lock
  map<hobject_t,set<BackoffRef>> backoffs;

  void add_backoff(SessionRef s, const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& o) {
    release_backoffs(o, o);
  }
  void clear_backoffs();

  void add_pg_backoff(SessionRef s) {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    add_backoff(s, begin, end);
  }
  void release_pg_backoffs() {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    release_backoffs(begin, end);
  }

  // -- scrub --
public:
  struct Scrubber {
    Scrubber();
    ~Scrubber();

    // metadata
    set<pg_shard_t> reserved_peers;
    bool reserved, reserve_failed;
    epoch_t epoch_start;

    // common to both scrubs
    bool active;
    set<pg_shard_t> waiting_on_whom;
    int shallow_errors;
    int deep_errors;
    int fixed;
    ScrubMap primary_scrubmap;
    ScrubMapBuilder primary_scrubmap_pos;
    epoch_t replica_scrub_start = 0;
    ScrubMap replica_scrubmap;
    ScrubMapBuilder replica_scrubmap_pos;
    map<pg_shard_t, ScrubMap> received_maps;
    OpRequestRef active_rep_scrub;
    utime_t scrub_reg_stamp;  // stamp we registered for

    omap_stat_t omap_stats  = (const struct omap_stat_t){ 0 };

    // For async sleep
    bool sleeping = false;
    bool needs_sleep = true;
    utime_t sleep_start;

    // flags to indicate explicitly requested scrubs (by admin)
    bool must_scrub, must_deep_scrub, must_repair;

    // Priority to use for scrub scheduling
    unsigned priority = 0;

    // this flag indicates whether we would like to do auto-repair of the PG or not
    bool auto_repair;
    // this flag indicates that we are scrubbing post repair to verify everything is fixed
    bool check_repair;
    // this flag indicates that if a regular scrub detects errors <= osd_scrub_auto_repair_num_errors,
    // we should deep scrub in order to auto repair
    bool deep_scrub_on_error;

    // Maps from objects with errors to missing/inconsistent peers
    map<hobject_t, set<pg_shard_t>> missing;
    map<hobject_t, set<pg_shard_t>> inconsistent;

    // Map from object with errors to good peers
    map<hobject_t, list<pair<ScrubMap::object, pg_shard_t> >> authoritative;

    // Cleaned map pending snap metadata scrub
    ScrubMap cleaned_meta_map;

    void clean_meta_map(ScrubMap &for_meta_scrub) {
      if (end.is_max() ||
          cleaned_meta_map.objects.empty()) {
         cleaned_meta_map.swap(for_meta_scrub);
      } else {
        auto iter = cleaned_meta_map.objects.end();
        --iter; // not empty, see if clause
        auto begin = cleaned_meta_map.objects.begin();
        if (iter->first.has_snapset()) {
          ++iter;
        } else {
          while (iter != begin) {
            auto next = iter--;
            if (next->first.get_head() != iter->first.get_head()) {
	      ++iter;
	      break;
            }
          }
        }
        for_meta_scrub.objects.insert(begin, iter);
        cleaned_meta_map.objects.erase(begin, iter);
      }
    }

    // digest updates which we are waiting on
    int num_digest_updates_pending;

    // chunky scrub
    hobject_t start, end;    // [start,end)
    hobject_t max_end;       // Largest end that may have been sent to replicas
    eversion_t subset_last_update;

    // chunky scrub state
    enum State {
      INACTIVE,
      NEW_CHUNK,
      WAIT_PUSHES,
      WAIT_LAST_UPDATE,
      BUILD_MAP,
      BUILD_MAP_DONE,
      WAIT_REPLICAS,
      COMPARE_MAPS,
      WAIT_DIGEST_UPDATES,
      FINISH,
      BUILD_MAP_REPLICA,
    } state;

    std::unique_ptr<Scrub::Store> store;
    // deep scrub
    bool deep;
    int preempt_left;
    int preempt_divisor;

    list<Context*> callbacks;
    void add_callback(Context *context) {
      callbacks.push_back(context);
    }
    void run_callbacks() {
      list<Context*> to_run;
      to_run.swap(callbacks);
      for (list<Context*>::iterator i = to_run.begin();
	   i != to_run.end();
	   ++i) {
	(*i)->complete(0);
      }
    }

    static const char *state_string(const PG::Scrubber::State& state) {
      const char *ret = NULL;
      switch( state )
      {
        case INACTIVE: ret = "INACTIVE"; break;
        case NEW_CHUNK: ret = "NEW_CHUNK"; break;
        case WAIT_PUSHES: ret = "WAIT_PUSHES"; break;
        case WAIT_LAST_UPDATE: ret = "WAIT_LAST_UPDATE"; break;
        case BUILD_MAP: ret = "BUILD_MAP"; break;
        case BUILD_MAP_DONE: ret = "BUILD_MAP_DONE"; break;
        case WAIT_REPLICAS: ret = "WAIT_REPLICAS"; break;
        case COMPARE_MAPS: ret = "COMPARE_MAPS"; break;
        case WAIT_DIGEST_UPDATES: ret = "WAIT_DIGEST_UPDATES"; break;
        case FINISH: ret = "FINISH"; break;
        case BUILD_MAP_REPLICA: ret = "BUILD_MAP_REPLICA"; break;
      }
      return ret;
    }

    bool is_chunky_scrub_active() const { return state != INACTIVE; }

    // clear all state
    void reset() {
      active = false;
      waiting_on_whom.clear();
      if (active_rep_scrub) {
        active_rep_scrub = OpRequestRef();
      }
      received_maps.clear();

      must_scrub = false;
      must_deep_scrub = false;
      must_repair = false;
      auto_repair = false;
      check_repair = false;
      deep_scrub_on_error = false;

      state = PG::Scrubber::INACTIVE;
      start = hobject_t();
      end = hobject_t();
      max_end = hobject_t();
      subset_last_update = eversion_t();
      shallow_errors = 0;
      deep_errors = 0;
      fixed = 0;
      omap_stats = (const struct omap_stat_t){ 0 };
      deep = false;
      run_callbacks();
      inconsistent.clear();
      missing.clear();
      authoritative.clear();
      num_digest_updates_pending = 0;
      primary_scrubmap = ScrubMap();
      primary_scrubmap_pos.reset();
      replica_scrubmap = ScrubMap();
      replica_scrubmap_pos.reset();
      cleaned_meta_map = ScrubMap();
      sleeping = false;
      needs_sleep = true;
      sleep_start = utime_t();
    }

    void create_results(const hobject_t& obj);
    void cleanup_store(ObjectStore::Transaction *t);
  } scrubber;

protected:
  bool scrub_after_recovery;

  int active_pushes;

  bool scrub_can_preempt = false;
  bool scrub_preempted = false;

  // we allow some number of preemptions of the scrub, which mean we do
  // not block.  then we start to block.  once we start blocking, we do
  // not stop until the scrub range is completed.
  bool write_blocked_by_scrub(const hobject_t &soid);

  /// true if the given range intersects the scrub interval in any way
  bool range_intersects_scrub(const hobject_t &start, const hobject_t& end);

  void repair_object(
    const hobject_t& soid, list<pair<ScrubMap::object, pg_shard_t> > *ok_peers,
    pg_shard_t bad_peer);

  void chunky_scrub(ThreadPool::TPHandle &handle);
  void scrub_compare_maps();
  /**
   * return true if any inconsistency/missing is repaired, false otherwise
   */
  bool scrub_process_inconsistent();
  bool ops_blocked_by_scrub() const;
  void scrub_finish();
  void scrub_clear_state(bool keep_repair = false);
  void _scan_snaps(ScrubMap &map);
  void _repair_oinfo_oid(ScrubMap &map);
  void _scan_rollback_obs(const vector<ghobject_t> &rollback_obs);
  void _request_scrub_map(pg_shard_t replica, eversion_t version,
                          hobject_t start, hobject_t end, bool deep,
			  bool allow_preemption);
  int build_scrub_map_chunk(
    ScrubMap &map,
    ScrubMapBuilder &pos,
    hobject_t start, hobject_t end, bool deep,
    ThreadPool::TPHandle &handle);
  /**
   * returns true if [begin, end) is good to scrub at this time
   * a false return value obliges the implementer to requeue scrub when the
   * condition preventing scrub clears
   */
  virtual bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end) = 0;
  virtual void scrub_snapshot_metadata(
    ScrubMap &map,
    const std::map<hobject_t,
                   pair<boost::optional<uint32_t>,
                        boost::optional<uint32_t>>> &missing_digest) { }
  virtual void _scrub_clear_state() { }
  virtual void _scrub_finish() { }
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;

  void replica_scrub(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_replica_scrub_map(OpRequestRef op);

  void handle_scrub_reserve_request(OpRequestRef op);
  void handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from);
  void handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from);
  void handle_scrub_reserve_release(OpRequestRef op);

  void reject_reservation();
  void schedule_backfill_retry(float retry);
  void schedule_recovery_retry(float retry);

  // -- recovery state --

  template <class EVT>
  struct QueuePeeringEvt : Context {
    PGRef pg;
    epoch_t epoch;
    EVT evt;
    QueuePeeringEvt(PG *pg, epoch_t epoch, EVT evt) :
      pg(pg), epoch(epoch), evt(evt) {}
    void finish(int r) override {
      pg->lock();
      pg->queue_peering_event(PGPeeringEventRef(
				new PGPeeringEvent(
				  epoch,
				  epoch,
				  evt)));
      pg->unlock();
    }
  };


  struct QueryState : boost::statechart::event< QueryState > {
    Formatter *f;
    explicit QueryState(Formatter *f) : f(f) {}
    void print(std::ostream *out) const {
      *out << "Query";
    }
  };

public:
  int pg_stat_adjust(osd_stat_t *new_stat);
protected:

  struct AdvMap : boost::statechart::event< AdvMap > {
    OSDMapRef osdmap;
    OSDMapRef lastmap;
    vector<int> newup, newacting;
    int up_primary, acting_primary;
    AdvMap(
      OSDMapRef osdmap, OSDMapRef lastmap,
      vector<int>& newup, int up_primary,
      vector<int>& newacting, int acting_primary):
      osdmap(osdmap), lastmap(lastmap),
      newup(newup),
      newacting(newacting),
      up_primary(up_primary),
      acting_primary(acting_primary) {}
    void print(std::ostream *out) const {
      *out << "AdvMap";
    }
  };

  struct ActMap : boost::statechart::event< ActMap > {
    ActMap() : boost::statechart::event< ActMap >() {}
    void print(std::ostream *out) const {
      *out << "ActMap";
    }
  };
  struct Activate : boost::statechart::event< Activate > {
    epoch_t activation_epoch;
    explicit Activate(epoch_t q) : boost::statechart::event< Activate >(),
			  activation_epoch(q) {}
    void print(std::ostream *out) const {
      *out << "Activate from " << activation_epoch;
    }
  };
public:
  struct UnfoundBackfill : boost::statechart::event<UnfoundBackfill> {
    explicit UnfoundBackfill() {}
    void print(std::ostream *out) const {
      *out << "UnfoundBackfill";
    }
  };
  struct UnfoundRecovery : boost::statechart::event<UnfoundRecovery> {
    explicit UnfoundRecovery() {}
    void print(std::ostream *out) const {
      *out << "UnfoundRecovery";
    }
  };

  struct RequestScrub : boost::statechart::event<RequestScrub> {
    bool deep;
    bool repair;
    explicit RequestScrub(bool d, bool r) : deep(d), repair(r) {}
    void print(std::ostream *out) const {
      *out << "RequestScrub(" << (deep ? "deep" : "shallow")
	   << (repair ? " repair" : "");
    }
  };

protected:
  TrivialEvent(Initialize)
  TrivialEvent(GotInfo)
  TrivialEvent(NeedUpThru)
  TrivialEvent(Backfilled)
  TrivialEvent(LocalBackfillReserved)
  TrivialEvent(RejectRemoteReservation)
  public:
  TrivialEvent(RequestBackfill)
  protected:
  TrivialEvent(RemoteRecoveryPreempted)
  TrivialEvent(RemoteBackfillPreempted)
  TrivialEvent(BackfillTooFull)
  TrivialEvent(RecoveryTooFull)

  TrivialEvent(MakePrimary)
  TrivialEvent(MakeStray)
  TrivialEvent(NeedActingChange)
  TrivialEvent(IsIncomplete)
  TrivialEvent(IsDown)

  TrivialEvent(AllReplicasRecovered)
  TrivialEvent(DoRecovery)
  TrivialEvent(LocalRecoveryReserved)
  public:
  protected:
  TrivialEvent(AllRemotesReserved)
  TrivialEvent(AllBackfillsReserved)
  TrivialEvent(GoClean)

  TrivialEvent(AllReplicasActivated)

  TrivialEvent(IntervalFlush)

  public:
  TrivialEvent(DeleteStart)
  TrivialEvent(DeleteSome)

  TrivialEvent(SetForceRecovery)
  TrivialEvent(UnsetForceRecovery)
  TrivialEvent(SetForceBackfill)
  TrivialEvent(UnsetForceBackfill)

  protected:
  TrivialEvent(DeleteReserved)
  TrivialEvent(DeleteInterrupted)

  /* Encapsulates PG recovery process */
  class RecoveryState {
    void start_handle(RecoveryCtx *new_ctx);
    void end_handle();
  public:
    void begin_block_outgoing();
    void end_block_outgoing();
    void clear_blocked_outgoing();
  private:

    /* States */
    struct Initial;
    class RecoveryMachine : public boost::statechart::state_machine< RecoveryMachine, Initial > {
      RecoveryState *state;
    public:
      PG *pg;

      utime_t event_time;
      uint64_t event_count;
      
      void clear_event_counters() {
	event_time = utime_t();
	event_count = 0;
      }

      void log_enter(const char *state_name);
      void log_exit(const char *state_name, utime_t duration);

      RecoveryMachine(RecoveryState *state, PG *pg) : state(state), pg(pg), event_count(0) {}

      /* Accessor functions for state methods */
      ObjectStore::Transaction* get_cur_transaction() {
	ceph_assert(state->rctx);
	ceph_assert(state->rctx->transaction);
	return state->rctx->transaction;
      }

      void send_query(pg_shard_t to, const pg_query_t &query) {
	ceph_assert(state->rctx);
	ceph_assert(state->rctx->query_map);
	(*state->rctx->query_map)[to.osd][spg_t(pg->info.pgid.pgid, to.shard)] =
	  query;
      }

      map<int, map<spg_t, pg_query_t> > *get_query_map() {
	ceph_assert(state->rctx);
	ceph_assert(state->rctx->query_map);
	return state->rctx->query_map;
      }

      map<int, vector<pair<pg_notify_t, PastIntervals> > > *get_info_map() {
	ceph_assert(state->rctx);
	ceph_assert(state->rctx->info_map);
	return state->rctx->info_map;
      }

      RecoveryCtx *get_recovery_ctx() { return &*(state->rctx); }

      void send_notify(pg_shard_t to,
		       const pg_notify_t &info, const PastIntervals &pi) {
	ceph_assert(state->rctx);
	state->rctx->send_notify(to, info, pi);
      }
    };
    friend class RecoveryMachine;

    /* States */
    // Initial
    // Reset
    // Start
    //   Started
    //     Primary
    //       WaitActingChange
    //       Peering
    //         GetInfo
    //         GetLog
    //         GetMissing
    //         WaitUpThru
    //         Incomplete
    //       Active
    //         Activating
    //         Clean
    //         Recovered
    //         Backfilling
    //         WaitRemoteBackfillReserved
    //         WaitLocalBackfillReserved
    //         NotBackfilling
    //         NotRecovering
    //         Recovering
    //         WaitRemoteRecoveryReserved
    //         WaitLocalRecoveryReserved
    //     ReplicaActive
    //       RepNotRecovering
    //       RepRecovering
    //       RepWaitBackfillReserved
    //       RepWaitRecoveryReserved
    //     Stray
    //     ToDelete
    //       WaitDeleteReserved
    //       Deleting
    // Crashed

    struct Crashed : boost::statechart::state< Crashed, RecoveryMachine >, NamedState {
      explicit Crashed(my_context ctx);
    };

    struct Reset;

    struct Initial : boost::statechart::state< Initial, RecoveryMachine >, NamedState {
      explicit Initial(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< Initialize, Reset >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;

      boost::statechart::result react(const MNotifyRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MLogRec&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Reset : boost::statechart::state< Reset, RecoveryMachine >, NamedState {
      explicit Reset(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction< IntervalFlush >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const IntervalFlush&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Start;

    struct Started : boost::statechart::state< Started, RecoveryMachine, Start >, NamedState {
      explicit Started(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< IntervalFlush >,
	// ignored
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction<SetForceRecovery>,
	boost::statechart::custom_reaction<UnsetForceRecovery>,
	boost::statechart::custom_reaction<SetForceBackfill>,
	boost::statechart::custom_reaction<UnsetForceBackfill>,
	boost::statechart::custom_reaction<RequestScrub>,
	// crash
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const IntervalFlush&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Primary;
    struct Stray;

    struct Start : boost::statechart::state< Start, Started >, NamedState {
      explicit Start(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< MakePrimary, Primary >,
	boost::statechart::transition< MakeStray, Stray >
	> reactions;
    };

    struct Peering;
    struct WaitActingChange;
    struct Incomplete;
    struct Down;

    struct Primary : boost::statechart::state< Primary, Started, Peering >, NamedState {
      explicit Primary(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::transition< NeedActingChange, WaitActingChange >,
	boost::statechart::custom_reaction<SetForceRecovery>,
	boost::statechart::custom_reaction<UnsetForceRecovery>,
	boost::statechart::custom_reaction<SetForceBackfill>,
	boost::statechart::custom_reaction<UnsetForceBackfill>,
	boost::statechart::custom_reaction<RequestScrub>
	> reactions;
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MNotifyRec&);
      boost::statechart::result react(const SetForceRecovery&);
      boost::statechart::result react(const UnsetForceRecovery&);
      boost::statechart::result react(const SetForceBackfill&);
      boost::statechart::result react(const UnsetForceBackfill&);
      boost::statechart::result react(const RequestScrub&);
    };

    struct WaitActingChange : boost::statechart::state< WaitActingChange, Primary>,
			      NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      explicit WaitActingChange(my_context ctx);
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MLogRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MNotifyRec&);
      void exit();
    };

    struct GetInfo;
    struct Active;

    struct Peering : boost::statechart::state< Peering, Primary, GetInfo >, NamedState {
      PastIntervals::PriorSet prior_set;
      bool history_les_bound;  //< need osd_find_best_info_ignore_history_les

      explicit Peering(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::transition< Activate, Active >,
	boost::statechart::custom_reaction< AdvMap >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap &advmap);
    };

    struct WaitLocalRecoveryReserved;
    struct Activating;
    struct Active : boost::statechart::state< Active, Primary, Activating >, NamedState {
      explicit Active(my_context ctx);
      void exit();

      const set<pg_shard_t> remote_shards_to_reserve_recovery;
      const set<pg_shard_t> remote_shards_to_reserve_backfill;
      bool all_replicas_activated;

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MTrim >,
	boost::statechart::custom_reaction< Backfilled >,
	boost::statechart::custom_reaction< AllReplicasActivated >,
	boost::statechart::custom_reaction< DeferRecovery >,
	boost::statechart::custom_reaction< DeferBackfill >,
	boost::statechart::custom_reaction< UnfoundRecovery >,
	boost::statechart::custom_reaction< UnfoundBackfill >,
	boost::statechart::custom_reaction< RemoteReservationRevokedTooFull>,
	boost::statechart::custom_reaction< RemoteReservationRevoked>,
	boost::statechart::custom_reaction< DoRecovery>
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MNotifyRec& notevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MTrim& trimevt);
      boost::statechart::result react(const Backfilled&) {
	return discard_event();
      }
      boost::statechart::result react(const AllReplicasActivated&);
      boost::statechart::result react(const DeferRecovery& evt) {
	return discard_event();
      }
      boost::statechart::result react(const DeferBackfill& evt) {
	return discard_event();
      }
      boost::statechart::result react(const UnfoundRecovery& evt) {
	return discard_event();
      }
      boost::statechart::result react(const UnfoundBackfill& evt) {
	return discard_event();
      }
      boost::statechart::result react(const RemoteReservationRevokedTooFull&) {
	return discard_event();
      }
      boost::statechart::result react(const RemoteReservationRevoked&) {
	return discard_event();
      }
      boost::statechart::result react(const DoRecovery&) {
	return discard_event();
      }
    };

    struct Clean : boost::statechart::state< Clean, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::custom_reaction<SetForceRecovery>,
	boost::statechart::custom_reaction<SetForceBackfill>
      > reactions;
      explicit Clean(my_context ctx);
      void exit();
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Recovered : boost::statechart::state< Recovered, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< GoClean, Clean >,
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::custom_reaction< AllReplicasActivated >
      > reactions;
      explicit Recovered(my_context ctx);
      void exit();
      boost::statechart::result react(const AllReplicasActivated&) {
	post_event(GoClean());
	return forward_event();
      }
    };

    struct Backfilling : boost::statechart::state< Backfilling, Active >, NamedState {
      typedef boost::mpl::list<
        boost::statechart::custom_reaction< Backfilled >,
	boost::statechart::custom_reaction< DeferBackfill >,
	boost::statechart::custom_reaction< UnfoundBackfill >,
	boost::statechart::custom_reaction< RemoteReservationRejected >,
	boost::statechart::custom_reaction< RemoteReservationRevokedTooFull>,
	boost::statechart::custom_reaction< RemoteReservationRevoked>
	> reactions;
      explicit Backfilling(my_context ctx);
      boost::statechart::result react(const RemoteReservationRejected& evt) {
	// for compat with old peers
	post_event(RemoteReservationRevokedTooFull());
	return discard_event();
      }
      void backfill_release_reservations();
      boost::statechart::result react(const Backfilled& evt);
      boost::statechart::result react(const RemoteReservationRevokedTooFull& evt);
      boost::statechart::result react(const RemoteReservationRevoked& evt);
      boost::statechart::result react(const DeferBackfill& evt);
      boost::statechart::result react(const UnfoundBackfill& evt);
      void cancel_backfill();
      void exit();
    };

    struct WaitRemoteBackfillReserved : boost::statechart::state< WaitRemoteBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >,
	boost::statechart::custom_reaction< RemoteReservationRevoked >,
	boost::statechart::transition< AllBackfillsReserved, Backfilling >
	> reactions;
      set<pg_shard_t>::const_iterator backfill_osd_it;
      explicit WaitRemoteBackfillReserved(my_context ctx);
      void retry();
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved& evt);
      boost::statechart::result react(const RemoteReservationRejected& evt);
      boost::statechart::result react(const RemoteReservationRevoked& evt);
    };

    struct WaitLocalBackfillReserved : boost::statechart::state< WaitLocalBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< LocalBackfillReserved, WaitRemoteBackfillReserved >
	> reactions;
      explicit WaitLocalBackfillReserved(my_context ctx);
      void exit();
    };

    struct NotBackfilling : boost::statechart::state< NotBackfilling, Active>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved>,
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      explicit NotBackfilling(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved& evt);
      boost::statechart::result react(const RemoteReservationRejected& evt);
    };

    struct NotRecovering : boost::statechart::state< NotRecovering, Active>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::custom_reaction< DeferRecovery >,
	boost::statechart::custom_reaction< UnfoundRecovery >
	> reactions;
      explicit NotRecovering(my_context ctx);
      boost::statechart::result react(const DeferRecovery& evt) {
	/* no-op */
	return discard_event();
      }
      boost::statechart::result react(const UnfoundRecovery& evt) {
	/* no-op */
	return discard_event();
      }
      void exit();
    };

    struct ToDelete;
    struct RepNotRecovering;
    struct ReplicaActive : boost::statechart::state< ReplicaActive, Started, RepNotRecovering >, NamedState {
      explicit ReplicaActive(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MTrim >,
	boost::statechart::custom_reaction< Activate >,
	boost::statechart::custom_reaction< DeferRecovery >,
	boost::statechart::custom_reaction< DeferBackfill >,
	boost::statechart::custom_reaction< UnfoundRecovery >,
	boost::statechart::custom_reaction< UnfoundBackfill >,
	boost::statechart::custom_reaction< RemoteBackfillPreempted >,
	boost::statechart::custom_reaction< RemoteRecoveryPreempted >,
	boost::statechart::custom_reaction< RecoveryDone >,
	boost::statechart::transition<DeleteStart, ToDelete>
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MTrim& trimevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MQuery&);
      boost::statechart::result react(const Activate&);
      boost::statechart::result react(const RecoveryDone&) {
	return discard_event();
      }
      boost::statechart::result react(const DeferRecovery& evt) {
	return discard_event();
      }
      boost::statechart::result react(const DeferBackfill& evt) {
	return discard_event();
      }
      boost::statechart::result react(const UnfoundRecovery& evt) {
	return discard_event();
      }
      boost::statechart::result react(const UnfoundBackfill& evt) {
	return discard_event();
      }
      boost::statechart::result react(const RemoteBackfillPreempted& evt) {
	return discard_event();
      }
      boost::statechart::result react(const RemoteRecoveryPreempted& evt) {
	return discard_event();
      }
    };

    struct RepRecovering : boost::statechart::state< RepRecovering, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RecoveryDone, RepNotRecovering >,
	// for compat with old peers
	boost::statechart::transition< RemoteReservationRejected, RepNotRecovering >,
	boost::statechart::transition< RemoteReservationCanceled, RepNotRecovering >,
	boost::statechart::custom_reaction< BackfillTooFull >,
	boost::statechart::custom_reaction< RemoteRecoveryPreempted >,
	boost::statechart::custom_reaction< RemoteBackfillPreempted >
	> reactions;
      explicit RepRecovering(my_context ctx);
      boost::statechart::result react(const RemoteRecoveryPreempted &evt);
      boost::statechart::result react(const BackfillTooFull &evt);
      boost::statechart::result react(const RemoteBackfillPreempted &evt);
      void exit();
    };

    struct RepWaitBackfillReserved : boost::statechart::state< RepWaitBackfillReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RejectRemoteReservation >,
	boost::statechart::custom_reaction< RemoteReservationRejected >,
	boost::statechart::custom_reaction< RemoteReservationCanceled >
	> reactions;
      explicit RepWaitBackfillReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved &evt);
      boost::statechart::result react(const RejectRemoteReservation &evt);
      boost::statechart::result react(const RemoteReservationRejected &evt);
      boost::statechart::result react(const RemoteReservationCanceled &evt);
    };

    struct RepWaitRecoveryReserved : boost::statechart::state< RepWaitRecoveryReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteRecoveryReserved >,
	// for compat with old peers
	boost::statechart::custom_reaction< RemoteReservationRejected >,
	boost::statechart::custom_reaction< RemoteReservationCanceled >
	> reactions;
      explicit RepWaitRecoveryReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
      boost::statechart::result react(const RemoteReservationRejected &evt) {
	// for compat with old peers
	post_event(RemoteReservationCanceled());
	return discard_event();
      }
      boost::statechart::result react(const RemoteReservationCanceled &evt);
    };

    struct RepNotRecovering : boost::statechart::state< RepNotRecovering, ReplicaActive>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RequestRecoveryPrio >,
	boost::statechart::custom_reaction< RequestBackfillPrio >,
	boost::statechart::custom_reaction< RejectRemoteReservation >,
	boost::statechart::transition< RemoteReservationRejected, RepNotRecovering >,
	boost::statechart::transition< RemoteReservationCanceled, RepNotRecovering >,
	boost::statechart::custom_reaction< RemoteRecoveryReserved >,
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::transition< RecoveryDone, RepNotRecovering >  // for compat with pre-reservation peers
	> reactions;
      explicit RepNotRecovering(my_context ctx);
      boost::statechart::result react(const RequestRecoveryPrio &evt);
      boost::statechart::result react(const RequestBackfillPrio &evt);
      boost::statechart::result react(const RemoteBackfillReserved &evt) {
	// my reservation completion raced with a RELEASE from primary
	return discard_event();
      }
      boost::statechart::result react(const RemoteRecoveryReserved &evt) {
	// my reservation completion raced with a RELEASE from primary
	return discard_event();
      }
      boost::statechart::result react(const RejectRemoteReservation &evt);
      void exit();
    };

    struct Recovering : boost::statechart::state< Recovering, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AllReplicasRecovered >,
	boost::statechart::custom_reaction< DeferRecovery >,
	boost::statechart::custom_reaction< UnfoundRecovery >,
	boost::statechart::custom_reaction< RequestBackfill >
	> reactions;
      explicit Recovering(my_context ctx);
      void exit();
      void release_reservations(bool cancel = false);
      boost::statechart::result react(const AllReplicasRecovered &evt);
      boost::statechart::result react(const DeferRecovery& evt);
      boost::statechart::result react(const UnfoundRecovery& evt);
      boost::statechart::result react(const RequestBackfill &evt);
    };

    struct WaitRemoteRecoveryReserved : boost::statechart::state< WaitRemoteRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< RemoteRecoveryReserved >,
	boost::statechart::transition< AllRemotesReserved, Recovering >
	> reactions;
      set<pg_shard_t>::const_iterator remote_recovery_reservation_it;
      explicit WaitRemoteRecoveryReserved(my_context ctx);
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
      void exit();
    };

    struct WaitLocalRecoveryReserved : boost::statechart::state< WaitLocalRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< LocalRecoveryReserved, WaitRemoteRecoveryReserved >,
	boost::statechart::custom_reaction< RecoveryTooFull >
	> reactions;
      explicit WaitLocalRecoveryReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RecoveryTooFull &evt);
    };

    struct Activating : boost::statechart::state< Activating, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< AllReplicasRecovered, Recovered >,
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved >
	> reactions;
      explicit Activating(my_context ctx);
      void exit();
    };

    struct Stray : boost::statechart::state< Stray, Started >,
	      NamedState {
      explicit Stray(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< RecoveryDone >,
	boost::statechart::transition<DeleteStart, ToDelete>
	> reactions;
      boost::statechart::result react(const MQuery& query);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const RecoveryDone&) {
	return discard_event();
      }
    };

    struct WaitDeleteReserved;
    struct ToDelete : boost::statechart::state<ToDelete, Started, WaitDeleteReserved>, NamedState {
      unsigned priority = 0;
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< DeleteSome >
	> reactions;
      explicit ToDelete(my_context ctx);
      boost::statechart::result react(const ActMap &evt);
      boost::statechart::result react(const DeleteSome &evt) {
	// happens if we drop out of Deleting due to reprioritization etc.
	return discard_event();
      }
      void exit();
    };

    struct Deleting;
    struct WaitDeleteReserved : boost::statechart::state<WaitDeleteReserved,
							 ToDelete>, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition<DeleteReserved, Deleting>
	> reactions;
      explicit WaitDeleteReserved(my_context ctx);
      void exit();
    };

    struct Deleting : boost::statechart::state<Deleting,
					       ToDelete>, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< DeleteSome >,
	boost::statechart::transition<DeleteInterrupted, WaitDeleteReserved>
	> reactions;
      explicit Deleting(my_context ctx);
      boost::statechart::result react(const DeleteSome &evt);
      void exit();
    };

    struct GetLog;

    struct GetInfo : boost::statechart::state< GetInfo, Peering >, NamedState {
      set<pg_shard_t> peer_info_requested;

      explicit GetInfo(my_context ctx);
      void exit();
      void get_infos();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::transition< GotInfo, GetLog >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::transition< IsDown, Down >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MNotifyRec& infoevt);
    };

    struct GotLog : boost::statechart::event< GotLog > {
      GotLog() : boost::statechart::event< GotLog >() {}
    };

    struct GetLog : boost::statechart::state< GetLog, Peering >, NamedState {
      pg_shard_t auth_log_shard;
      boost::intrusive_ptr<MOSDPGLog> msg;

      explicit GetLog(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< GotLog >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::transition< IsIncomplete, Incomplete >
	> reactions;
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const GotLog&);
    };

    struct WaitUpThru;

    struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
      set<pg_shard_t> peer_missing_requested;

      explicit GetMissing(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::transition< NeedUpThru, WaitUpThru >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MLogRec& logevt);
    };

    struct WaitUpThru : boost::statechart::state< WaitUpThru, Peering >, NamedState {
      explicit WaitUpThru(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MLogRec >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const ActMap& am);
      boost::statechart::result react(const MLogRec& logrec);
    };

    struct Down : boost::statechart::state< Down, Peering>, NamedState {
      explicit Down(my_context ctx);
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MNotifyRec& infoevt);
      void exit();
    };

    struct Incomplete : boost::statechart::state< Incomplete, Peering>, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< QueryState >
	> reactions;
      explicit Incomplete(my_context ctx);
      boost::statechart::result react(const AdvMap &advmap);
      boost::statechart::result react(const MNotifyRec& infoevt);
      boost::statechart::result react(const QueryState& q);
      void exit();
    };

    RecoveryMachine machine;
    PG *pg;

    /// context passed in by state machine caller
    RecoveryCtx *orig_ctx;

    /// populated if we are buffering messages pending a flush
    boost::optional<BufferedRecoveryMessages> messages_pending_flush;

    /**
     * populated between start_handle() and end_handle(), points into
     * the message lists for messages_pending_flush while blocking messages
     * or into orig_ctx otherwise
     */
    boost::optional<RecoveryCtx> rctx;

  public:
    explicit RecoveryState(PG *pg)
      : machine(this, pg), pg(pg), orig_ctx(0) {
      machine.initiate();
    }

    void handle_event(const boost::statechart::event_base &evt,
		      RecoveryCtx *rctx) {
      start_handle(rctx);
      machine.process_event(evt);
      end_handle();
    }

    void handle_event(PGPeeringEventRef evt,
		      RecoveryCtx *rctx) {
      start_handle(rctx);
      machine.process_event(evt->get_event());
      end_handle();
    }

  } recovery_state;



  uint64_t peer_features;
  uint64_t acting_features;
  uint64_t upacting_features;

  epoch_t last_epoch;

  /// most recently consumed osdmap's require_osd_version
  unsigned last_require_osd_release = 0;
  bool delete_needs_sleep = false;

protected:
  void reset_min_peer_features() {
    peer_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  }
  uint64_t get_min_peer_features() const { return peer_features; }
  void apply_peer_features(uint64_t f) { peer_features &= f; }

  uint64_t get_min_acting_features() const { return acting_features; }
  uint64_t get_min_upacting_features() const { return upacting_features; }
  bool perform_deletes_during_peering() const {
    return !(get_osdmap()->test_flag(CEPH_OSDMAP_RECOVERY_DELETES));
  }

  bool hard_limit_pglog() const {
    return (get_osdmap()->test_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT));
  }

  void init_primary_up_acting(
    const vector<int> &newup,
    const vector<int> &newacting,
    int new_up_primary,
    int new_acting_primary) {
    actingset.clear();
    acting = newacting;
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] != CRUSH_ITEM_NONE)
	actingset.insert(
	  pg_shard_t(
	    acting[i],
	    pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    upset.clear();
    up = newup;
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] != CRUSH_ITEM_NONE)
	upset.insert(
	  pg_shard_t(
	    up[i],
	    pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    if (!pool.info.is_erasure()) {
      up_primary = pg_shard_t(new_up_primary, shard_id_t::NO_SHARD);
      primary = pg_shard_t(new_acting_primary, shard_id_t::NO_SHARD);
      return;
    }
    up_primary = pg_shard_t();
    primary = pg_shard_t();
    for (uint8_t i = 0; i < up.size(); ++i) {
      if (up[i] == new_up_primary) {
	up_primary = pg_shard_t(up[i], shard_id_t(i));
	break;
      }
    }
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] == new_acting_primary) {
	primary = pg_shard_t(acting[i], shard_id_t(i));
	break;
      }
    }
    ceph_assert(up_primary.osd == new_up_primary);
    ceph_assert(primary.osd == new_acting_primary);
  }

  void set_role(int r) {
    role = r;
  }

  bool state_test(uint64_t m) const { return (state & m) != 0; }
  void state_set(uint64_t m) { state |= m; }
  void state_clear(uint64_t m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }
  bool should_send_notify() const { return send_notify; }

  uint64_t get_state() const { return state; }
  bool is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool is_activating() const { return state_test(PG_STATE_ACTIVATING); }
  bool is_peering() const { return state_test(PG_STATE_PEERING); }
  bool is_down() const { return state_test(PG_STATE_DOWN); }
  bool is_recovery_unfound() const { return state_test(PG_STATE_RECOVERY_UNFOUND); }
  bool is_backfill_unfound() const { return state_test(PG_STATE_BACKFILL_UNFOUND); }
  bool is_incomplete() const { return state_test(PG_STATE_INCOMPLETE); }
  bool is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool is_undersized() const { return state_test(PG_STATE_UNDERSIZED); }
  bool is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }
  bool is_remapped() const { return state_test(PG_STATE_REMAPPED); }
  bool is_peered() const {
    return state_test(PG_STATE_ACTIVE) || state_test(PG_STATE_PEERED);
  }
  bool is_recovering() const { return state_test(PG_STATE_RECOVERING); }
  bool is_premerge() const { return state_test(PG_STATE_PREMERGE); }
  bool is_repair() const { return state_test(PG_STATE_REPAIR); }

  bool is_empty() const { return info.last_update == eversion_t(0,0); }

  // pg on-disk state
  void do_pending_flush();

public:
  static void _create(ObjectStore::Transaction& t, spg_t pgid, int bits);
  static void _init(ObjectStore::Transaction& t,
		    spg_t pgid, const pg_pool_t *pool);

protected:
  void prepare_write_info(map<string,bufferlist> *km);

  void update_store_with_options();

public:
  static int _prepare_write_info(
    CephContext* cct,
    map<string,bufferlist> *km,
    epoch_t epoch,
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    bool dirty_big_info,
    bool dirty_epoch,
    bool try_fast_info,
    PerfCounters *logger = nullptr);

  void write_if_dirty(RecoveryCtx *rctx) {
    write_if_dirty(*rctx->transaction);
  }
protected:
  void write_if_dirty(ObjectStore::Transaction& t);

  PGLog::IndexedLog projected_log;
  bool check_in_progress_op(
    const osd_reqid_t &r,
    eversion_t *version,
    version_t *user_version,
    int *return_code) const;
  eversion_t projected_last_update;
  eversion_t get_next_version() const {
    eversion_t at_version(
      get_osdmap_epoch(),
      projected_last_update.version+1);
    ceph_assert(at_version > info.last_update);
    ceph_assert(at_version > pg_log.get_head());
    ceph_assert(at_version > projected_last_update);
    return at_version;
  }

  void add_log_entry(const pg_log_entry_t& e, bool applied);
  void append_log(
    const vector<pg_log_entry_t>& logv,
    eversion_t trim_to,
    eversion_t roll_forward_to,
    ObjectStore::Transaction &t,
    bool transaction_applied = true,
    bool async = false);
  bool check_log_for_corruption(ObjectStore *store);

  std::string get_corrupt_pg_log_name() const;

  void update_snap_map(
    const vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(vector<snapid_t> &snaps);

  void log_weirdness();

  virtual void kick_snap_trim() = 0;
  virtual void snap_trimmer_scrub_complete() = 0;
  bool requeue_scrub(bool high_priority = false);
  void queue_recovery();
  bool queue_scrub();
  unsigned get_scrub_priority();

  /// share pg info after a pg is active
  void share_pg_info();


  bool append_log_entries_update_missing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    boost::optional<eversion_t> trim_to,
    boost::optional<eversion_t> roll_forward_to);

  /**
   * Merge entries updating missing as necessary on all
   * acting_recovery_backfill logs and missings (also missing_loc)
   */
  void merge_new_log_entries(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObjectStore::Transaction &t,
    boost::optional<eversion_t> trim_to,
    boost::optional<eversion_t> roll_forward_to);

  void reset_interval_flush();
  void start_peering_interval(
    const OSDMapRef lastmap,
    const vector<int>& newup, int up_primary,
    const vector<int>& newacting, int acting_primary,
    ObjectStore::Transaction *t);
  void on_new_interval();
  virtual void _on_new_interval() = 0;
  void start_flush(ObjectStore::Transaction *t);
  void set_last_peering_reset();

  void update_history(const pg_history_t& history);
  void fulfill_info(pg_shard_t from, const pg_query_t &query,
		    pair<pg_shard_t, pg_info_t> &notify_info);
  void fulfill_log(pg_shard_t from, const pg_query_t &query, epoch_t query_epoch);
  void fulfill_query(const MQuery& q, RecoveryCtx *rctx);
  void check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap);

  bool should_restart_peering(
    int newupprimary,
    int newactingprimary,
    const vector<int>& newup,
    const vector<int>& newacting,
    OSDMapRef lastmap,
    OSDMapRef osdmap);

  // OpRequest queueing
  bool can_discard_op(OpRequestRef& op);
  bool can_discard_scan(OpRequestRef op);
  bool can_discard_backfill(OpRequestRef op);
  bool can_discard_request(OpRequestRef& op);

  template<typename T, int MSGTYPE>
  bool can_discard_replica_op(OpRequestRef& op);

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);
  bool old_peering_evt(PGPeeringEventRef evt) {
    return old_peering_msg(evt->get_epoch_sent(), evt->get_epoch_requested());
  }
  static bool have_same_or_newer_map(epoch_t cur_epoch, epoch_t e) {
    return e <= cur_epoch;
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef& op);


  // recovery bits
  void take_waiters();


  // abstract bits
  friend class FlushState;

  virtual void on_role_change() = 0;
  virtual void on_pool_change() = 0;
  virtual void on_change(ObjectStore::Transaction *t) = 0;
  virtual void on_activate() = 0;
  virtual void on_flushed() = 0;
  virtual void check_blacklisted_watchers() = 0;

  friend ostream& operator<<(ostream& out, const PG& pg);
};


ostream& operator<<(ostream& out, const PG::BackfillInterval& bi);

#endif
