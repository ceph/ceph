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
#include <tr1/memory>

// re-include our assert to clobber boost's
#include "include/assert.h" 

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "include/atomic.h"
#include "SnapMapper.h"

#include "PGLog.h"
#include "OpRequest.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGLog.h"
#include "common/tracked_int_ptr.hpp"
#include "common/WorkQueue.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


//#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs

class OSD;
class OSDService;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGScan;
class MOSDPGBackfill;
class MOSDPGInfo;

class PG;

void intrusive_ptr_add_ref(PG *pg);
void intrusive_ptr_release(PG *pg);

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg);
  void put_with_id(PG *pg, uint64_t id);
  typedef TrackedIntPtr<PG> PGRef;
#else
  typedef boost::intrusive_ptr<PG> PGRef;
#endif

struct PGRecoveryStats {
  struct per_state_info {
    uint64_t enter, exit;     // enter/exit counts
    uint64_t events;
    utime_t event_time;       // time spent processing events
    utime_t total_time;       // total time in state
    utime_t min_time, max_time;

    per_state_info() : enter(0), exit(0), events(0) {}
  };
  map<const char *,per_state_info> info;
  Mutex lock;

  PGRecoveryStats() : lock("PGRecoverStats::lock") {}

  void reset() {
    Mutex::Locker l(lock);
    info.clear();
  }
  void dump(ostream& out) {
    Mutex::Locker l(lock);
    for (map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); ++p) {
      per_state_info& i = p->second;
      out << i.enter << "\t" << i.exit << "\t"
	  << i.events << "\t" << i.event_time << "\t"
	  << i.total_time << "\t"
	  << i.min_time << "\t" << i.max_time << "\t"
	  << p->first << "\n";
	       
    }
  }

  void log_enter(const char *s) {
    Mutex::Locker l(lock);
    info[s].enter++;
  }
  void log_exit(const char *s, utime_t dur, uint64_t events, utime_t event_dur) {
    Mutex::Locker l(lock);
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
  int64_t id;
  string name;
  uint64_t auid;

  pg_pool_t info;      
  SnapContext snapc;   // the default pool snapc, ready to go.

  interval_set<snapid_t> cached_removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(int64_t i, const char *_name, uint64_t au) :
    id(i), auid(au) {
    if (_name)
      name = _name;
  }

  void update(OSDMapRef map);
};

/** PG - Replica Placement Group
 *
 */

class PG {
public:
  std::string gen_prefix() const;

  /*** PG ****/
protected:
  OSDService *osd;
  OSDriver osdriver;
  SnapMapper snap_mapper;
public:
  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
protected:
  // Ops waiting for map, should be queued at back
  Mutex map_lock;
  list<OpRequestRef> waiting_for_map;
  OSDMapRef osdmap_ref;
  OSDMapRef last_persisted_osdmap_ref;
  PGPool pool;

  void queue_op(OpRequestRef op);
  void take_op_map_waiters();

  void update_osdmap_ref(OSDMapRef newmap) {
    assert(_lock.is_locked_by_me());
    Mutex::Locker l(map_lock);
    osdmap_ref = newmap;
  }

  OSDMapRef get_osdmap_with_maplock() const {
    assert(map_lock.is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  OSDMapRef get_osdmap() const {
    assert(is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  Cond _cond;
  atomic_t ref;

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock;
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id;
#endif

public:
  bool deleting;  // true while in removing or OSD is shutting down

  void lock(bool no_lockdep = false);
  void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    assert(!dirty_info);
    assert(!dirty_big_info);
    assert(!dirty_log);
    _lock.Unlock();
  }

  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() const {
    return _lock.is_locked();
  }
  void wait() {
    assert(_lock.is_locked());
    _cond.Wait(_lock);
  }
  void kick() {
    assert(_lock.is_locked());
    _cond.Signal();
  }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  void get(const string &tag);
  void put(const string &tag);

  bool dirty_info, dirty_big_info, dirty_log;

public:
  // pg state
  pg_info_t        info;
  __u8 info_struct_v;
  static const __u8 cur_struct_v = 7;
  bool must_upgrade() {
    return info_struct_v < 7;
  }
  void upgrade(
    ObjectStore *store,
    const interval_set<snapid_t> &snapcolls);

  const coll_t coll;
  PGLog  pg_log;
  static string get_info_key(pg_t pgid) {
    return stringify(pgid) + "_info";
  }
  static string get_biginfo_key(pg_t pgid) {
    return stringify(pgid) + "_biginfo";
  }
  static string get_epoch_key(pg_t pgid) {
    return stringify(pgid) + "_epoch";
  }
  hobject_t    log_oid;
  hobject_t    biginfo_oid;
  map<hobject_t, set<int> > missing_loc;
  set<int> missing_loc_sources;           // superset of missing_loc locations
  
  interval_set<snapid_t> snap_collections; // obsolete
  map<epoch_t,pg_interval_t> past_intervals;

  interval_set<snapid_t> snap_trimq;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item recovery_item, scrub_item, scrub_finalize_item, snap_trim_item, stat_queue_item;
  int recovery_ops_active;
  bool waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  set<hobject_t> recovering_oids;
#endif

  utime_t replay_until;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  unsigned    state;   // PG_STATE_*

  bool send_notify;    ///< true if we are non-primary and should notify the primary

public:
  eversion_t  last_update_ondisk;    // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_complete_ondisk;  // last_complete that has committed.
  eversion_t  last_update_applied;

  // primary state
 public:
  vector<int> up, acting, want_acting;
  map<int,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  // [primary only] content recovery state
 protected:
  struct PriorSet {
    set<int> probe; /// current+prior OSDs we need to probe.
    set<int> down;  /// down osds that would normally be in @a probe and might be interesting.
    map<int,epoch_t> blocked_by;  /// current lost_at values for any OSDs in cur set for which (re)marking them lost would affect cur set

    bool pg_down;   /// some down osds are included in @a cur; the DOWN pg state bit should be set.
    PriorSet(const OSDMap &osdmap,
	     const map<epoch_t, pg_interval_t> &past_intervals,
	     const vector<int> &up,
	     const vector<int> &acting,
	     const pg_info_t &info,
	     const PG *debug_pg=NULL);

    bool affected_by_map(const OSDMapRef osdmap, const PG *debug_pg=0) const;
  };

  friend std::ostream& operator<<(std::ostream& oss,
				  const struct PriorSet &prior);

  bool may_need_replay(const OSDMapRef osdmap) const;


public:    
  struct RecoveryCtx {
    utime_t start_time;
    map< int, map<pg_t, pg_query_t> > *query_map;
    map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map;
    map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list;
    C_Contexts *on_applied;
    C_Contexts *on_safe;
    ObjectStore::Transaction *transaction;
    RecoveryCtx(map< int, map<pg_t, pg_query_t> > *query_map,
		map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map,
		map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list,
		C_Contexts *on_applied,
		C_Contexts *on_safe,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map), 
	notify_list(notify_list),
	on_applied(on_applied),
	on_safe(on_safe),
	transaction(transaction) {}
  };

  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    const char *get_state_name() { return state_name; }
    NamedState() : state_name(0), enter_time(ceph_clock_now(g_ceph_context)) {}
    virtual ~NamedState() {}
  };



protected:

  /*
   * peer_info    -- projected (updates _before_ replicas ack)
   * peer_missing -- committed (updates _after_ replicas ack)
   */
  
  bool        need_up_thru;
  set<int>    stray_set;   // non-acting osds that have PG data.
  eversion_t  oldest_update; // acting: lowest (valid) last_update in active set
  map<int,pg_info_t>    peer_info;   // info from peers (stray or prior)
  set<int> peer_purged; // peers purged
  map<int,pg_missing_t> peer_missing;
  set<int>             peer_log_requested;  // logs i've requested (and start stamps)
  set<int>             peer_missing_requested;
  set<int>             stray_purged;  // i deleted these strays; ignore racing PGInfo from them
  set<int>             peer_activated;

  // primary-only, recovery-only state
  set<int>             might_have_unfound;  // These osds might have objects on them
					    // which are unfound on the primary
  bool need_flush;     // need to flush before any new activity

  epoch_t last_peering_reset;


  /* heartbeat peers */
  void set_probe_targets(const set<int> &probe_set);
  void clear_probe_targets();
public:
  Mutex heartbeat_peer_lock;
  set<int> heartbeat_peers;
  set<int> probe_targets;

protected:
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
    map<hobject_t,eversion_t> objects;
    hobject_t begin;
    hobject_t end;
    
    /// clear content
    void clear() {
      objects.clear();
      begin = end = hobject_t();
    }

    void reset(hobject_t start) {
      clear();
      begin = end = start;
    }

    /// true if there are no objects in this interval
    bool empty() {
      return objects.empty();
    }

    /// true if interval extends to the end of the range
    bool extends_to_end() {
      return end == hobject_t::get_max();
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
      assert(!objects.empty());
      objects.erase(objects.begin());
      if (objects.empty())
	begin = end;
      else
	begin = objects.begin()->first;
    }

    /// dump
    void dump(Formatter *f) const {
      f->dump_stream("begin") << begin;
      f->dump_stream("end") << end;
      f->open_array_section("objects");
      for (map<hobject_t, eversion_t>::const_iterator i = objects.begin();
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
  
  BackfillInterval backfill_info;
  BackfillInterval peer_backfill_info;
  int backfill_target;
  bool backfill_reserved;
  bool backfill_reserving;

  friend class OSD;

public:
  int get_backfill_target() const {
    return backfill_target;
  }

protected:


  // pg waiters
  bool flushed;

  // Ops waiting on backfill_pos to change
  list<OpRequestRef> waiting_for_backfill_pos;
  list<OpRequestRef>            waiting_for_active;
  list<OpRequestRef>            waiting_for_all_missing;
  map<hobject_t, list<OpRequestRef> > waiting_for_missing_object,
                                        waiting_for_degraded_object;
  // Callbacks should assume pg (and nothing else) is locked
  map<hobject_t, list<Context*> > callbacks_for_degraded_object;
  map<eversion_t,list<OpRequestRef> > waiting_for_ack, waiting_for_ondisk;
  map<eversion_t,OpRequestRef>   replay_queue;
  void split_ops(PG *child, unsigned split_bits);

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  // for ordering writes
  std::tr1::shared_ptr<ObjectStore::Sequencer> osr;

  void publish_stats_to_osd();
  void clear_publish_stats();

public:
  void clear_primary_state();

 public:
  bool is_acting(int osd) const { 
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] == osd) return true;
    return false;
  }
  bool is_up(int osd) const { 
    for (unsigned i=0; i<up.size(); i++)
      if (up[i] == osd) return true;
    return false;
  }
  
  bool needs_recovery() const;
  bool needs_backfill() const;

  void mark_clean();  ///< mark an active pg clean

  bool _calc_past_interval_range(epoch_t *start, epoch_t *end);
  void generate_past_intervals();
  void trim_past_intervals();
  void build_prior(std::auto_ptr<PriorSet> &prior_set);

  void remove_down_peer_info(const OSDMapRef osdmap);

  bool adjust_need_up_thru(const OSDMapRef osdmap);

  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  virtual void mark_all_unfound_lost(int how) = 0;
  virtual void dump_recovery_info(Formatter *f) const = 0;

  bool calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    for (unsigned i=1; i<acting.size(); i++) {
      if (peer_last_complete_ondisk.count(acting[i]) == 0)
	return false;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[acting[i]];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return false;
    min_last_complete_ondisk = min;
    return true;
  }

  virtual void calc_trim_to() = 0;

  void proc_replica_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
			pg_missing_t& omissing, int from);
  void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, int from);
  bool proc_replica_info(int from, const pg_info_t &info);
  void remove_snap_mapped_object(
    ObjectStore::Transaction& t, const hobject_t& soid);
  void merge_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, int from);
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead);
  bool search_for_missing(const pg_info_t &oinfo, const pg_missing_t *omissing,
			  int fromosd);

  void check_for_lost_objects();
  void forget_lost_objects();

  void discover_all_missing(std::map< int, map<pg_t,pg_query_t> > &query_map);
  
  void trim_write_ahead();

  map<int, pg_info_t>::const_iterator find_best_info(const map<int, pg_info_t> &infos) const;
  bool calc_acting(int& newest_update_osd, vector<int>& want) const;
  bool choose_acting(int& newest_update_osd);
  void build_might_have_unfound();
  void replay_queued_ops();
  void activate(ObjectStore::Transaction& t,
		epoch_t query_epoch,
		list<Context*>& tfin,
		map< int, map<pg_t,pg_query_t> >& query_map,
		map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map=0);
  void _activate_committed(epoch_t e);
  void all_activated_and_committed();

  void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);

  bool have_unfound() const { 
    return pg_log.get_missing().num_missing() > missing_loc.size();
  }
  int get_num_unfound() const {
    return pg_log.get_missing().num_missing() - missing_loc.size();
  }

  virtual void clean_up_local(ObjectStore::Transaction& t) = 0;

  virtual int start_recovery_ops(int max, RecoveryCtx *prctx) = 0;

  void purge_strays();

  void update_heartbeat_peers();

  Context *finish_sync_event;

  void finish_recovery(list<Context*>& tfin);
  void _finish_recovery(Context *c);
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  virtual void check_recovery_sources(const OSDMapRef newmap) = 0;
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  void split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits) = 0;

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;


  // -- scrub --
  struct Scrubber {
    Scrubber() :
      reserved(false), reserve_failed(false),
      epoch_start(0),
      block_writes(false), active(false), queue_snap_trim(false),
      waiting_on(0), shallow_errors(0), deep_errors(0), fixed(0),
      active_rep_scrub(0),
      must_scrub(false), must_deep_scrub(false), must_repair(false),
      classic(false),
      finalizing(false), is_chunky(false), state(INACTIVE),
      deep(false)
    {
    }

    // metadata
    set<int> reserved_peers;
    bool reserved, reserve_failed;
    epoch_t epoch_start;

    // common to both scrubs
    bool block_writes;
    bool active;
    bool queue_snap_trim;
    int waiting_on;
    set<int> waiting_on_whom;
    int shallow_errors;
    int deep_errors;
    int fixed;
    ScrubMap primary_scrubmap;
    map<int,ScrubMap> received_maps;
    MOSDRepScrub *active_rep_scrub;
    utime_t scrub_reg_stamp;  // stamp we registered for

    // flags to indicate explicitly requested scrubs (by admin)
    bool must_scrub, must_deep_scrub, must_repair;

    // Maps from objects with errors to missing/inconsistent peers
    map<hobject_t, set<int> > missing;
    map<hobject_t, set<int> > inconsistent;
    map<hobject_t, set<int> > inconsistent_snapcolls;

    // Map from object with errors to good peer
    map<hobject_t, pair<ScrubMap::object, int> > authoritative;

    // classic scrub
    bool classic;
    bool finalizing;

    // chunky scrub
    bool is_chunky;
    hobject_t start, end;
    eversion_t subset_last_update;

    // chunky scrub state
    enum State {
      INACTIVE,
      NEW_CHUNK,
      WAIT_PUSHES,
      WAIT_LAST_UPDATE,
      BUILD_MAP,
      WAIT_REPLICAS,
      COMPARE_MAPS,
      FINISH,
    } state;

    // deep scrub
    bool deep;

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
        case WAIT_REPLICAS: ret = "WAIT_REPLICAS"; break;
        case COMPARE_MAPS: ret = "COMPARE_MAPS"; break;
        case FINISH: ret = "FINISH"; break;
      }
      return ret;
    }

    bool is_chunky_scrub_active() const { return state != INACTIVE; }

    // classic (non chunk) scrubs block all writes
    // chunky scrubs only block writes to a range
    bool write_blocked_by_scrub(const hobject_t &soid) {
      if (!block_writes)
	return false;

      if (!is_chunky)
	return true;

      if (soid >= start && soid < end)
	return true;

      return false;
    }

    // clear all state
    void reset() {
      classic = false;
      finalizing = false;
      block_writes = false;
      active = false;
      queue_snap_trim = false;
      waiting_on = 0;
      waiting_on_whom.clear();
      if (active_rep_scrub) {
        active_rep_scrub->put();
        active_rep_scrub = NULL;
      }
      received_maps.clear();

      must_scrub = false;
      must_deep_scrub = false;
      must_repair = false;

      state = PG::Scrubber::INACTIVE;
      start = hobject_t();
      end = hobject_t();
      subset_last_update = eversion_t();
      shallow_errors = 0;
      deep_errors = 0;
      fixed = 0;
      deep = false;
      run_callbacks();
      inconsistent.clear();
      missing.clear();
      authoritative.clear();
    }

  } scrubber;

  bool scrub_after_recovery;

  int active_pushes;

  void repair_object(const hobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer);
  map<int, ScrubMap *>::const_iterator _select_auth_object(
    const hobject_t &obj,
    const map<int,ScrubMap*> &maps);

  enum error_type {
    CLEAN,
    DEEP_ERROR,
    SHALLOW_ERROR
  };
  enum error_type _compare_scrub_objects(ScrubMap::object &auth,
			      ScrubMap::object &candidate,
			      ostream &errorstream);
  void _compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			  map<hobject_t, set<int> > &missing,
			  map<hobject_t, set<int> > &inconsistent,
			  map<hobject_t, int> &authoritative,
			  map<hobject_t, set<int> > &inconsistent_snapcolls,
			  ostream &errorstream);
  void scrub(ThreadPool::TPHandle &handle);
  void classic_scrub(ThreadPool::TPHandle &handle);
  void chunky_scrub(ThreadPool::TPHandle &handle);
  void scrub_compare_maps();
  void scrub_process_inconsistent();
  void scrub_finalize();
  void scrub_finish();
  void scrub_clear_state();
  bool scrub_gather_replica_maps();
  void _scan_list(
    ScrubMap &map, vector<hobject_t> &ls, bool deep,
    ThreadPool::TPHandle &handle);
  void _scan_snaps(ScrubMap &map);
  void _request_scrub_map_classic(int replica, eversion_t version);
  void _request_scrub_map(int replica, eversion_t version,
                          hobject_t start, hobject_t end, bool deep);
  int build_scrub_map_chunk(
    ScrubMap &map,
    hobject_t start, hobject_t end, bool deep,
    ThreadPool::TPHandle &handle);
  void build_scrub_map(ScrubMap &map, ThreadPool::TPHandle &handle);
  void build_inc_scrub_map(
    ScrubMap &map, eversion_t v, ThreadPool::TPHandle &handle);
  virtual void _scrub(ScrubMap &map) { }
  virtual void _scrub_clear_state() { }
  virtual void _scrub_finish() { }
  virtual coll_t get_temp_coll() = 0;
  virtual bool have_temp_coll() = 0;
  virtual bool _report_snap_collection_errors(
    const hobject_t &hoid,
    const map<string, bufferptr> &attrs,
    int osd,
    ostream &out) { return false; };
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;
  bool sched_scrub();
  void reg_next_scrub();
  void unreg_next_scrub();

  void replica_scrub(
    class MOSDRepScrub *op,
    ThreadPool::TPHandle &handle);
  void sub_op_scrub_map(OpRequestRef op);
  void sub_op_scrub_reserve(OpRequestRef op);
  void sub_op_scrub_reserve_reply(OpRequestRef op);
  void sub_op_scrub_unreserve(OpRequestRef op);
  void sub_op_scrub_stop(OpRequestRef op);

  void reject_reservation();
  void schedule_backfill_full_retry();

  // -- recovery state --

  template <class EVT>
  struct QueuePeeringEvt : Context {
    PGRef pg;
    epoch_t epoch;
    EVT evt;
    QueuePeeringEvt(PG *pg, epoch_t epoch, EVT evt) :
      pg(pg), epoch(epoch), evt(evt) {}
    void finish(int r) {
      pg->lock();
      pg->queue_peering_event(PG::CephPeeringEvtRef(
				new PG::CephPeeringEvt(
				  epoch,
				  epoch,
				  evt)));
      pg->unlock();
    }
  };

  class CephPeeringEvt {
    epoch_t epoch_sent;
    epoch_t epoch_requested;
    boost::intrusive_ptr< const boost::statechart::event_base > evt;
    string desc;
  public:
    template <class T>
    CephPeeringEvt(epoch_t epoch_sent,
		   epoch_t epoch_requested,
		   const T &evt_) :
      epoch_sent(epoch_sent), epoch_requested(epoch_requested),
      evt(evt_.intrusive_from_this()) {
      stringstream out;
      out << "epoch_sent: " << epoch_sent
	  << " epoch_requested: " << epoch_requested << " ";
      evt_.print(&out);
      desc = out.str();
    }
    epoch_t get_epoch_sent() { return epoch_sent; }
    epoch_t get_epoch_requested() { return epoch_requested; }
    const boost::statechart::event_base &get_event() { return *evt; }
    string get_desc() { return desc; }
  };
  typedef std::tr1::shared_ptr<CephPeeringEvt> CephPeeringEvtRef;
  list<CephPeeringEvtRef> peering_queue;  // op queue
  list<CephPeeringEvtRef> peering_waiters;

  struct QueryState : boost::statechart::event< QueryState > {
    Formatter *f;
    QueryState(Formatter *f) : f(f) {}
    void print(std::ostream *out) const {
      *out << "Query";
    }
  };

  struct MInfoRec : boost::statechart::event< MInfoRec > {
    int from;
    pg_info_t info;
    epoch_t msg_epoch;
    MInfoRec(int from, pg_info_t &info, epoch_t msg_epoch) :
      from(from), info(info), msg_epoch(msg_epoch) {}
    void print(std::ostream *out) const {
      *out << "MInfoRec from " << from << " info: " << info;
    }
  };

  struct MLogRec : boost::statechart::event< MLogRec > {
    int from;
    boost::intrusive_ptr<MOSDPGLog> msg;
    MLogRec(int from, MOSDPGLog *msg) :
      from(from), msg(msg) {}
    void print(std::ostream *out) const {
      *out << "MLogRec from " << from;
    }
  };

  struct MNotifyRec : boost::statechart::event< MNotifyRec > {
    int from;
    pg_notify_t notify;
    MNotifyRec(int from, pg_notify_t &notify) :
      from(from), notify(notify) {}
    void print(std::ostream *out) const {
      *out << "MNotifyRec from " << from << " notify: " << notify;
    }
  };

  struct MQuery : boost::statechart::event< MQuery > {
    int from;
    pg_query_t query;
    epoch_t query_epoch;
    MQuery(int from, const pg_query_t &query, epoch_t query_epoch):
      from(from), query(query), query_epoch(query_epoch) {}
    void print(std::ostream *out) const {
      *out << "MQuery from " << from
	   << " query_epoch " << query_epoch
	   << " query: " << query;
    }
  };

  struct AdvMap : boost::statechart::event< AdvMap > {
    OSDMapRef osdmap;
    OSDMapRef lastmap;
    vector<int> newup, newacting;
    AdvMap(OSDMapRef osdmap, OSDMapRef lastmap, vector<int>& newup, vector<int>& newacting):
      osdmap(osdmap), lastmap(lastmap), newup(newup), newacting(newacting) {}
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
    epoch_t query_epoch;
    Activate(epoch_t q) : boost::statechart::event< Activate >(),
			  query_epoch(q) {}
    void print(std::ostream *out) const {
      *out << "Activate from " << query_epoch;
    }
  };
  struct RequestBackfillPrio : boost::statechart::event< RequestBackfillPrio > {
    unsigned priority;
    RequestBackfillPrio(unsigned prio) :
              boost::statechart::event< RequestBackfillPrio >(),
			  priority(prio) {}
    void print(std::ostream *out) const {
      *out << "RequestBackfillPrio: priority " << priority;
    }
  };
#define TrivialEvent(T) struct T : boost::statechart::event< T > { \
    T() : boost::statechart::event< T >() {}			   \
    void print(std::ostream *out) const {			   \
      *out << #T;						   \
    }								   \
  };
  TrivialEvent(Initialize)
  TrivialEvent(Load)
  TrivialEvent(GotInfo)
  TrivialEvent(NeedUpThru)
  TrivialEvent(CheckRepops)
  TrivialEvent(NullEvt)
  TrivialEvent(FlushedEvt)
  TrivialEvent(Backfilled)
  TrivialEvent(LocalBackfillReserved)
  TrivialEvent(RemoteBackfillReserved)
  TrivialEvent(RemoteReservationRejected)
  TrivialEvent(RequestBackfill)
  TrivialEvent(RequestRecovery)
  TrivialEvent(RecoveryDone)
  TrivialEvent(BackfillTooFull)

  TrivialEvent(AllReplicasRecovered)
  TrivialEvent(DoRecovery)
  TrivialEvent(LocalRecoveryReserved)
  TrivialEvent(RemoteRecoveryReserved)
  TrivialEvent(AllRemotesReserved)
  TrivialEvent(Recovering)
  TrivialEvent(WaitRemoteBackfillReserved)
  TrivialEvent(GoClean)

  TrivialEvent(AllReplicasActivated)

  /* Encapsulates PG recovery process */
  class RecoveryState {
    void start_handle(RecoveryCtx *new_ctx) {
      assert(!rctx);
      rctx = new_ctx;
      if (rctx)
	rctx->start_time = ceph_clock_now(g_ceph_context);
    }

    void end_handle() {
      if (rctx) {
	utime_t dur = ceph_clock_now(g_ceph_context) - rctx->start_time;
	machine.event_time += dur;
      }
      machine.event_count++;
      rctx = 0;
    }

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
	assert(state->rctx->transaction);
	return state->rctx->transaction;
      }

      void send_query(int to, const pg_query_t &query) {
	assert(state->rctx->query_map);
	(*state->rctx->query_map)[to][pg->info.pgid] = query;
      }

      map<int, map<pg_t, pg_query_t> > *get_query_map() {
	assert(state->rctx->query_map);
	return state->rctx->query_map;
      }

      map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *get_info_map() {
	assert(state->rctx->info_map);
	return state->rctx->info_map;
      }

      list< Context* > *get_on_safe_context_list() {
	assert(state->rctx->on_safe);
	return &(state->rctx->on_safe->contexts);
      }

      list< Context * > *get_on_applied_context_list() {
	assert(state->rctx->on_applied);
	return &(state->rctx->on_applied->contexts);
      }

      void send_notify(int to, const pg_notify_t &info, const pg_interval_map_t &pi) {
	assert(state->rctx->notify_list);
	(*state->rctx->notify_list)[to].push_back(make_pair(info, pi));
      }
    };
    friend class RecoveryMachine;

    /* States */

    struct Crashed : boost::statechart::state< Crashed, RecoveryMachine >, NamedState {
      Crashed(my_context ctx);
    };

    struct Started;
    struct Reset;

    struct Initial : boost::statechart::state< Initial, RecoveryMachine >, NamedState {
      Initial(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< Initialize, Reset >,
	boost::statechart::custom_reaction< Load >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;

      boost::statechart::result react(const Load&);
      boost::statechart::result react(const MNotifyRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MLogRec&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Reset : boost::statechart::state< Reset, RecoveryMachine >, NamedState {
      Reset(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction< FlushedEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct Start;

    struct Started : boost::statechart::state< Started, RecoveryMachine, Start >, NamedState {
      Started(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< NullEvt >,
	boost::statechart::custom_reaction< FlushedEvt >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const boost::statechart::event_base&) {
	return discard_event();
      }
    };

    struct MakePrimary : boost::statechart::event< MakePrimary > {
      MakePrimary() : boost::statechart::event< MakePrimary >() {}
    };
    struct MakeStray : boost::statechart::event< MakeStray > {
      MakeStray() : boost::statechart::event< MakeStray >() {}
    };
    struct Primary;
    struct Stray;

    struct Start : boost::statechart::state< Start, Started >, NamedState {
      Start(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< MakePrimary, Primary >,
	boost::statechart::transition< MakeStray, Stray >
	> reactions;
    };

    struct Peering;
    struct WaitActingChange;
    struct NeedActingChange : boost::statechart::event< NeedActingChange > {
      NeedActingChange() : boost::statechart::event< NeedActingChange >() {}
    };
    struct Incomplete;
    struct IsIncomplete : boost::statechart::event< IsIncomplete > {
      IsIncomplete() : boost::statechart::event< IsIncomplete >() {}
    };

    struct Primary : boost::statechart::state< Primary, Started, Peering >, NamedState {
      Primary(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::transition< NeedActingChange, WaitActingChange >,
	boost::statechart::custom_reaction< AdvMap>
	> reactions;
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MNotifyRec&);
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
      WaitActingChange(my_context ctx);
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
      std::auto_ptr< PriorSet > prior_set;
      bool flushed;

      Peering(my_context ctx);
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
      Active(my_context ctx);
      void exit();

      const set<int> sorted_acting_set;
      bool all_replicas_activated;

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< Backfilled >,
	boost::statechart::custom_reaction< AllReplicasActivated >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MNotifyRec& notevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const Backfilled&) {
	return discard_event();
      }
      boost::statechart::result react(const AllReplicasActivated&);
    };

    struct Clean : boost::statechart::state< Clean, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >
      > reactions;
      Clean(my_context ctx);
      void exit();
    };

    struct Recovered : boost::statechart::state< Recovered, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< GoClean, Clean >,
	boost::statechart::custom_reaction< AllReplicasActivated >
      > reactions;
      Recovered(my_context ctx);
      void exit();
      boost::statechart::result react(const AllReplicasActivated&) {
	post_event(GoClean());
	return forward_event();
      }
    };

    struct Backfilling : boost::statechart::state< Backfilling, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< Backfilled, Recovered >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      Backfilling(my_context ctx);
      boost::statechart::result react(const RemoteReservationRejected& evt);
      void exit();
    };

    struct WaitRemoteBackfillReserved : boost::statechart::state< WaitRemoteBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      WaitRemoteBackfillReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved& evt);
      boost::statechart::result react(const RemoteReservationRejected& evt);
    };

    struct WaitLocalBackfillReserved : boost::statechart::state< WaitLocalBackfillReserved, Active >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< LocalBackfillReserved, WaitRemoteBackfillReserved >
	> reactions;
      WaitLocalBackfillReserved(my_context ctx);
      void exit();
    };

    struct NotBackfilling : boost::statechart::state< NotBackfilling, Active>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved>
	> reactions;
      NotBackfilling(my_context ctx);
      void exit();
    };

    struct RepNotRecovering;
    struct ReplicaActive : boost::statechart::state< ReplicaActive, Started, RepNotRecovering >, NamedState {
      ReplicaActive(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< Activate >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MQuery&);
      boost::statechart::result react(const Activate&);
    };

    struct RepRecovering : boost::statechart::state< RepRecovering, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::transition< RecoveryDone, RepNotRecovering >,
	boost::statechart::custom_reaction< BackfillTooFull >
	> reactions;
      RepRecovering(my_context ctx);
      boost::statechart::result react(const BackfillTooFull &evt);
      void exit();
    };

    struct RepWaitBackfillReserved : boost::statechart::state< RepWaitBackfillReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      RepWaitBackfillReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved &evt);
      boost::statechart::result react(const RemoteReservationRejected &evt);
    };

    struct RepWaitRecoveryReserved : boost::statechart::state< RepWaitRecoveryReserved, ReplicaActive >, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RemoteRecoveryReserved >
	> reactions;
      RepWaitRecoveryReserved(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
    };

    struct RepNotRecovering : boost::statechart::state< RepNotRecovering, ReplicaActive>, NamedState {
      typedef boost::mpl::list<
	boost::statechart::custom_reaction< RequestBackfillPrio >,
        boost::statechart::transition< RequestRecovery, RepWaitRecoveryReserved >,
	boost::statechart::transition< RecoveryDone, RepNotRecovering >  // for compat with pre-reservation peers
	> reactions;
      RepNotRecovering(my_context ctx);
      boost::statechart::result react(const RequestBackfillPrio &evt);
      void exit();
    };

    struct Recovering : boost::statechart::state< Recovering, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AllReplicasRecovered >,
	boost::statechart::custom_reaction< RequestBackfill >
	> reactions;
      Recovering(my_context ctx);
      void exit();
      void release_reservations();
      boost::statechart::result react(const AllReplicasRecovered &evt);
      boost::statechart::result react(const RequestBackfill &evt);
    };

    struct WaitRemoteRecoveryReserved : boost::statechart::state< WaitRemoteRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< RemoteRecoveryReserved >,
	boost::statechart::transition< AllRemotesReserved, Recovering >
	> reactions;
      set<int>::const_iterator acting_osd_it;
      WaitRemoteRecoveryReserved(my_context ctx);
      boost::statechart::result react(const RemoteRecoveryReserved &evt);
      void exit();
    };

    struct WaitLocalRecoveryReserved : boost::statechart::state< WaitLocalRecoveryReserved, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< LocalRecoveryReserved, WaitRemoteRecoveryReserved >
	> reactions;
      WaitLocalRecoveryReserved(my_context ctx);
      void exit();
    };

    struct Activating : boost::statechart::state< Activating, Active >, NamedState {
      typedef boost::mpl::list <
	boost::statechart::transition< AllReplicasRecovered, Recovered >,
	boost::statechart::transition< DoRecovery, WaitLocalRecoveryReserved >,
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved >
	> reactions;
      Activating(my_context ctx);
      void exit();
    };

    struct Stray : boost::statechart::state< Stray, Started >, NamedState {
      map<int, pair<pg_query_t, epoch_t> > pending_queries;

      Stray(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< RecoveryDone >
	> reactions;
      boost::statechart::result react(const MQuery& query);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const RecoveryDone&) {
	return discard_event();
      }
    };

    struct GetLog;

    struct GetInfo : boost::statechart::state< GetInfo, Peering >, NamedState {
      set<int> peer_info_requested;

      GetInfo(my_context ctx);
      void exit();
      void get_infos();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::transition< GotInfo, GetLog >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MNotifyRec& infoevt);
    };

    struct GetMissing;
    struct GotLog : boost::statechart::event< GotLog > {
      GotLog() : boost::statechart::event< GotLog >() {}
    };

    struct GetLog : boost::statechart::state< GetLog, Peering >, NamedState {
      int newest_update_osd;
      boost::intrusive_ptr<MOSDPGLog> msg;

      GetLog(my_context ctx);
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
    struct WaitFlushedPeering;

    struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
      set<int> peer_missing_requested;

      GetMissing(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::transition< NeedUpThru, WaitUpThru >,
	boost::statechart::transition< CheckRepops, WaitFlushedPeering>
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const MLogRec& logevt);
    };

    struct WaitFlushedPeering :
      boost::statechart::state< WaitFlushedPeering, Peering>, NamedState {
      WaitFlushedPeering(my_context ctx);
      void exit() {}
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< FlushedEvt >
      > reactions;
      boost::statechart::result react(const FlushedEvt& evt);
      boost::statechart::result react(const QueryState& q);
    };

    struct WaitUpThru : boost::statechart::state< WaitUpThru, Peering >, NamedState {
      WaitUpThru(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< QueryState >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::transition< CheckRepops, WaitFlushedPeering>,
	boost::statechart::custom_reaction< MLogRec >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const ActMap& am);
      boost::statechart::result react(const MLogRec& logrec);
    };

    struct Incomplete : boost::statechart::state< Incomplete, Peering>, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AdvMap >
	> reactions;
      Incomplete(my_context ctx);
      boost::statechart::result react(const AdvMap &advmap);
      void exit();
    };


    RecoveryMachine machine;
    PG *pg;
    RecoveryCtx *rctx;

  public:
    RecoveryState(PG *pg) : machine(this, pg), pg(pg), rctx(0) {
      machine.initiate();
    }

    void handle_event(const boost::statechart::event_base &evt,
		      RecoveryCtx *rctx) {
      start_handle(rctx);
      machine.process_event(evt);
      end_handle();
    }

    void handle_event(CephPeeringEvtRef evt,
		      RecoveryCtx *rctx) {
      start_handle(rctx);
      machine.process_event(evt->get_event());
      end_handle();
    }

  } recovery_state;


 public:
  PG(OSDService *o, OSDMapRef curmap,
     const PGPool &pool, pg_t p, const hobject_t& loid, const hobject_t& ioid);
  virtual ~PG();

 private:
  // Prevent copying
  PG(const PG& rhs);
  PG& operator=(const PG& rhs);

 public:
  pg_t       get_pgid() const { return info.pgid; }
  int        get_nrep() const { return acting.size(); }

  int        get_primary() { return acting.empty() ? -1:acting[0]; }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  bool       is_primary() const { return role == 0; }
  bool       is_replica() const { return role > 0; }

  epoch_t get_last_peering_reset() const { return last_peering_reset; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }
  bool should_send_notify() const { return send_notify; }

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_peering() const { return state_test(PG_STATE_PEERING); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool       is_degraded() const { return state_test(PG_STATE_DEGRADED); }

  bool       is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  void init(
    int role,
    vector<int>& up,
    vector<int>& acting,
    pg_history_t& history,
    pg_interval_map_t& pim,
    bool backfill,
    ObjectStore::Transaction *t);

  // pg on-disk state
  void do_pending_flush();

private:
  void write_info(ObjectStore::Transaction& t);
  void write_log(ObjectStore::Transaction& t);

public:
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll,
    map<epoch_t,pg_interval_t> &past_intervals,
    interval_set<snapid_t> &snap_collections,
    hobject_t &infos_oid,
    __u8 info_struct_v, bool dirty_big_info, bool force_ver = false);
  void write_if_dirty(ObjectStore::Transaction& t);

  void add_log_entry(pg_log_entry_t& e, bufferlist& log_bl);
  void append_log(
    vector<pg_log_entry_t>& logv, eversion_t trim_to, ObjectStore::Transaction &t);
  bool check_log_for_corruption(ObjectStore *store);
  void trim_peers();

  std::string get_corrupt_pg_log_name() const;
  static int read_info(
    ObjectStore *store, const coll_t coll,
    bufferlist &bl, pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
    hobject_t &biginfo_oid, hobject_t &infos_oid,
    interval_set<snapid_t>  &snap_collections, __u8 &);
  void read_state(ObjectStore *store, bufferlist &bl);
  static epoch_t peek_map_epoch(ObjectStore *store, coll_t coll,
                               hobject_t &infos_oid, bufferlist *bl);
  void update_snap_map(
    vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(SnapContext& snapc);

  void log_weirdness();

  void queue_snap_trim();
  bool queue_scrub();

  /// share pg info after a pg is active
  void share_pg_info();
  /// share new pg log entries after a pg is active
  void share_pg_log();

  void start_peering_interval(const OSDMapRef lastmap,
			      const vector<int>& newup,
			      const vector<int>& newacting);
  void start_flush(ObjectStore::Transaction *t,
		   list<Context *> *on_applied,
		   list<Context *> *on_safe);
  void set_last_peering_reset();
  bool pg_has_reset_since(epoch_t e) {
    assert(is_locked());
    return deleting || e < get_last_peering_reset();
  }

  void update_history_from_master(pg_history_t new_history);
  void fulfill_info(int from, const pg_query_t &query, 
		    pair<int, pg_info_t> &notify_info);
  void fulfill_log(int from, const pg_query_t &query, epoch_t query_epoch);
  bool is_split(OSDMapRef lastmap, OSDMapRef nextmap);
  bool acting_up_affected(const vector<int>& newup, const vector<int>& newacting);

  // OpRequest queueing
  bool can_discard_op(OpRequestRef op);
  bool can_discard_scan(OpRequestRef op);
  bool can_discard_subop(OpRequestRef op);
  bool can_discard_backfill(OpRequestRef op);
  bool can_discard_request(OpRequestRef op);

  static bool op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op);

  static bool split_request(OpRequestRef op, unsigned match, unsigned bits);

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);
  bool old_peering_evt(CephPeeringEvtRef evt) {
    return old_peering_msg(evt->get_epoch_sent(), evt->get_epoch_requested());
  }
  static bool have_same_or_newer_map(OSDMapRef osdmap, epoch_t e) {
    return e <= osdmap->get_epoch();
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef op);


  // recovery bits
  void take_waiters();
  void queue_peering_event(CephPeeringEvtRef evt);
  void handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx);
  void queue_notify(epoch_t msg_epoch, epoch_t query_epoch,
		    int from, pg_notify_t& i);
  void queue_info(epoch_t msg_epoch, epoch_t query_epoch,
		  int from, pg_info_t& i);
  void queue_log(epoch_t msg_epoch, epoch_t query_epoch, int from,
		 MOSDPGLog *msg);
  void queue_query(epoch_t msg_epoch, epoch_t query_epoch,
		   int from, const pg_query_t& q);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(OSDMapRef osdmap, OSDMapRef lastmap,
			  vector<int>& newup, vector<int>& newacting,
			  RecoveryCtx *rctx);
  void handle_activate_map(RecoveryCtx *rctx);
  void handle_create(RecoveryCtx *rctx);
  void handle_loaded(RecoveryCtx *rctx);
  void handle_query_state(Formatter *f);

  virtual void on_removal(ObjectStore::Transaction *t) = 0;


  // abstract bits
  void do_request(OpRequestRef op);

  virtual void do_op(OpRequestRef op) = 0;
  virtual void do_sub_op(OpRequestRef op) = 0;
  virtual void do_sub_op_reply(OpRequestRef op) = 0;
  virtual void do_scan(OpRequestRef op) = 0;
  virtual void do_backfill(OpRequestRef op) = 0;
  virtual void snap_trimmer() = 0;

  virtual int do_command(vector<string>& cmd, ostream& ss,
			 bufferlist& idata, bufferlist& odata) = 0;

  virtual bool same_for_read_since(epoch_t e) = 0;
  virtual bool same_for_modify_since(epoch_t e) = 0;
  virtual bool same_for_rep_modify_since(epoch_t e) = 0;

  virtual void on_role_change() = 0;
  virtual void on_change() = 0;
  virtual void on_activate() = 0;
  virtual void on_flushed() = 0;
  virtual void on_shutdown() = 0;
};

ostream& operator<<(ostream& out, const PG& pg);

#endif
