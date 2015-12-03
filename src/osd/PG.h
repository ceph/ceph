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
#include "include/memory.h"

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
#include "common/cmdparse.h"
#include "common/tracked_int_ptr.hpp"
#include "common/WorkQueue.h"
#include "common/ceph_context.h"
#include "include/str_list.h"
#include "PGBackend.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include "include/unordered_map.h"
#include "include/unordered_set.h"


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

    // cppcheck-suppress unreachableCode
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

  void dump_formatted(Formatter *f) {
    Mutex::Locker l(lock);
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

  PGPool(int64_t i, const string& _name, uint64_t au)
    : id(i), name(_name), auid(au) { }

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
  CephContext *cct;
  OSDriver osdriver;
  SnapMapper snap_mapper;

  virtual PGBackend *get_pgbackend() = 0;
public:
  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
  /// get_is_recoverable_predicate: caller owns returned pointer and must delete when done
  IsPGRecoverablePredicate *get_is_recoverable_predicate() {
    return get_pgbackend()->get_is_recoverable_predicate();
  }
protected:
  // Ops waiting for map, should be queued at back
  Mutex map_lock;
  list<OpRequestRef> waiting_for_map;
  OSDMapRef osdmap_ref;
  OSDMapRef last_persisted_osdmap_ref;
  PGPool pool;

  void queue_op(OpRequestRef& op);
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

public:
  OSDMapRef get_osdmap() const {
    assert(is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }
protected:

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  mutable Mutex _lock;
  atomic_t ref;

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock;
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id;
#endif

public:
  bool deleting;  // true while in removing or OSD is shutting down


  void lock_suspend_timeout(ThreadPool::TPHandle &handle);
  void lock(bool no_lockdep = false) const;
  void unlock() const {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    assert(!dirty_info);
    assert(!dirty_big_info);
    _lock.Unlock();
  }

  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() const {
    return _lock.is_locked();
  }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  void get(const string &tag);
  void put(const string &tag);

  bool dirty_info, dirty_big_info;

public:
  bool is_ec_pg() const {
    return pool.info.ec_pool();
  }
  // pg state
  pg_info_t        info;
  __u8 info_struct_v;
  static const __u8 cur_struct_v = 8;
  // v7 was SnapMapper addition in 86658392516d5175b2756659ef7ffaaf95b0f8ad
  // (first appeared in cuttlefish).
  static const __u8 compat_struct_v = 7;
  bool must_upgrade() {
    return info_struct_v < cur_struct_v;
  }
  bool can_upgrade() {
    return info_struct_v >= compat_struct_v;
  }
  void upgrade(ObjectStore *store);

  const coll_t coll;
  PGLog  pg_log;
  static string get_info_key(spg_t pgid) {
    return stringify(pgid) + "_info";
  }
  static string get_biginfo_key(spg_t pgid) {
    return stringify(pgid) + "_biginfo";
  }
  static string get_epoch_key(spg_t pgid) {
    return stringify(pgid) + "_epoch";
  }
  ghobject_t    pgmeta_oid;

  class MissingLoc {
    map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator> needs_recovery_map;
    map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator > missing_loc;
    set<pg_shard_t> missing_loc_sources;
    PG *pg;
    set<pg_shard_t> empty_set;
  public:
    boost::scoped_ptr<IsPGReadablePredicate> is_readable;
    boost::scoped_ptr<IsPGRecoverablePredicate> is_recoverable;
    MissingLoc(PG *pg)
      : pg(pg) {}
    void set_backend_predicates(
      IsPGReadablePredicate *_is_readable,
      IsPGRecoverablePredicate *_is_recoverable) {
      is_readable.reset(_is_readable);
      is_recoverable.reset(_is_recoverable);
    }
    string gen_prefix() const { return pg->gen_prefix(); }
    bool needs_recovery(
      const hobject_t &hoid,
      eversion_t *v = 0) const {
      map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::const_iterator i =
	needs_recovery_map.find(hoid);
      if (i == needs_recovery_map.end())
	return false;
      if (v)
	*v = i->second.need;
      return true;
    }
    bool is_unfound(const hobject_t &hoid) const {
      return needs_recovery(hoid) && (
	!missing_loc.count(hoid) ||
	!(*is_recoverable)(missing_loc.find(hoid)->second));
    }
    bool readable_with_acting(
      const hobject_t &hoid,
      const set<pg_shard_t> &acting) const;
    uint64_t num_unfound() const {
      uint64_t ret = 0;
      for (map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::const_iterator i =
	     needs_recovery_map.begin();
	   i != needs_recovery_map.end();
	   ++i) {
	if (is_unfound(i->first))
	  ++ret;
      }
      return ret;
    }

    void clear() {
      needs_recovery_map.clear();
      missing_loc.clear();
      missing_loc_sources.clear();
    }

    void add_location(const hobject_t &hoid, pg_shard_t location) {
      missing_loc[hoid].insert(location);
    }
    void remove_location(const hobject_t &hoid, pg_shard_t location) {
      missing_loc[hoid].erase(location);
    }
    void add_active_missing(const pg_missing_t &missing) {
      for (map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::const_iterator i =
	     missing.missing.begin();
	   i != missing.missing.end();
	   ++i) {
	map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator>::const_iterator j =
	  needs_recovery_map.find(i->first);
	if (j == needs_recovery_map.end()) {
	  needs_recovery_map.insert(*i);
	} else {
	  assert(i->second.need == j->second.need);
	}
      }
    }

    void add_missing(const hobject_t &hoid, eversion_t need, eversion_t have) {
      needs_recovery_map[hoid] = pg_missing_t::item(need, have);
    }
    void revise_need(const hobject_t &hoid, eversion_t need) {
      assert(needs_recovery(hoid));
      needs_recovery_map[hoid].need = need;
    }

    /// Adds info about a possible recovery source
    bool add_source_info(
      pg_shard_t source,           ///< [in] source
      const pg_info_t &oinfo,      ///< [in] info
      const pg_missing_t &omissing, ///< [in] (optional) missing
      bool sort_bitwise,            ///< [in] local sort bitwise (vs nibblewise)
      ThreadPool::TPHandle* handle  ///< [in] ThreadPool handle
      ); ///< @return whether a new object location was discovered

    /// Adds recovery sources in batch
    void add_batch_sources_info(
      const set<pg_shard_t> &sources  ///< [in] a set of resources which can be used for all objects
      );

    /// Uses osdmap to update structures for now down sources
    void check_recovery_sources(const OSDMapRef osdmap);

    /// Call when hoid is no longer missing in acting set
    void recovered(const hobject_t &hoid) {
      needs_recovery_map.erase(hoid);
      missing_loc.erase(hoid);
    }

    const set<pg_shard_t> &get_locations(const hobject_t &hoid) const {
      return missing_loc.count(hoid) ?
	missing_loc.find(hoid)->second : empty_set;
    }
    const map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &get_missing_locs() const {
      return missing_loc;
    }
    const map<hobject_t, pg_missing_t::item, hobject_t::BitwiseComparator> &get_needs_recovery() const {
      return needs_recovery_map;
    }
  } missing_loc;
  
  map<epoch_t,pg_interval_t> past_intervals;

  interval_set<snapid_t> snap_trimq;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item recovery_item, stat_queue_item;
  bool snap_trim_queued;
  bool scrub_queued;

  int recovery_ops_active;
  set<pg_shard_t> waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  set<hobject_t, hobject_t::BitwiseComparator> recovering_oids;
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


  struct C_UpdateLastRollbackInfoTrimmedToApplied : Context {
    PGRef pg;
    epoch_t e;
    eversion_t v;
    C_UpdateLastRollbackInfoTrimmedToApplied(PG *pg, epoch_t e, eversion_t v)
      : pg(pg), e(e), v(v) {}
    void finish(int) {
      pg->lock();
      if (!pg->pg_has_reset_since(e)) {
	pg->last_rollback_info_trimmed_to_applied = v;
      }
      pg->unlock();
    }
  };
  // entries <= last_rollback_info_trimmed_to_applied have been trimmed,
  // and the transaction has applied
  eversion_t  last_rollback_info_trimmed_to_applied;

  // primary state
 public:
  pg_shard_t primary;
  pg_shard_t pg_whoami;
  pg_shard_t up_primary;
  vector<int> up, acting, want_acting;
  set<pg_shard_t> actingbackfill, actingset;
  map<pg_shard_t,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  set<int> blocked_by; ///< osds we are blocked by (for pg stats)

  // [primary only] content recovery state
 protected:
  struct PriorSet {
    const bool ec_pool;
    set<pg_shard_t> probe; /// current+prior OSDs we need to probe.
    set<int> down;  /// down osds that would normally be in @a probe and might be interesting.
    map<int, epoch_t> blocked_by;  /// current lost_at values for any OSDs in cur set for which (re)marking them lost would affect cur set

    bool pg_down;   /// some down osds are included in @a cur; the DOWN pg state bit should be set.
    boost::scoped_ptr<IsPGRecoverablePredicate> pcontdec;
    PriorSet(bool ec_pool,
	     IsPGRecoverablePredicate *c,
	     const OSDMap &osdmap,
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
  struct BufferedRecoveryMessages {
    map<int, map<spg_t, pg_query_t> > query_map;
    map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > info_map;
    map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > notify_list;
  };

  struct RecoveryCtx {
    utime_t start_time;
    map<int, map<spg_t, pg_query_t> > *query_map;
    map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map;
    map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list;
    C_Contexts *on_applied;
    C_Contexts *on_safe;
    ObjectStore::Transaction *transaction;
    ThreadPool::TPHandle* handle;
    RecoveryCtx(map<int, map<spg_t, pg_query_t> > *query_map,
		map<int,
		    vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map,
		map<int,
		    vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list,
		C_Contexts *on_applied,
		C_Contexts *on_safe,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map), 
	notify_list(notify_list),
	on_applied(on_applied),
	on_safe(on_safe),
	transaction(transaction),
        handle(NULL) {}

    RecoveryCtx(BufferedRecoveryMessages &buf, RecoveryCtx &rctx)
      : query_map(&(buf.query_map)),
	info_map(&(buf.info_map)),
	notify_list(&(buf.notify_list)),
	on_applied(rctx.on_applied),
	on_safe(rctx.on_safe),
	transaction(rctx.transaction),
        handle(rctx.handle) {}

    void accept_buffered_messages(BufferedRecoveryMessages &m) {
      assert(query_map);
      assert(info_map);
      assert(notify_list);
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
      for (map<int, vector<pair<pg_notify_t, pg_interval_map_t> > >::iterator i
	     = m.info_map.begin();
	   i != m.info_map.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > &ovec =
	  (*info_map)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
      for (map<int, vector<pair<pg_notify_t, pg_interval_map_t> > >::iterator i
	     = m.notify_list.begin();
	   i != m.notify_list.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > &ovec =
	  (*notify_list)[i->first];
	ovec.reserve(ovec.size() + i->second.size());
	ovec.insert(ovec.end(), i->second.begin(), i->second.end());
      }
    }
  };

  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    const char *get_state_name() { return state_name; }
    NamedState(CephContext *cct_, const char *state_name_)
      : state_name(state_name_),
        enter_time(ceph_clock_now(cct_)) {}
    virtual ~NamedState() {}
  };



protected:

  /*
   * peer_info    -- projected (updates _before_ replicas ack)
   * peer_missing -- committed (updates _after_ replicas ack)
   */
  
  bool        need_up_thru;
  set<pg_shard_t>    stray_set;   // non-acting osds that have PG data.
  eversion_t  oldest_update; // acting: lowest (valid) last_update in active set
  map<pg_shard_t, pg_info_t>    peer_info;   // info from peers (stray or prior)
  set<pg_shard_t> peer_purged; // peers purged
  map<pg_shard_t, pg_missing_t> peer_missing;
  set<pg_shard_t> peer_log_requested;  // logs i've requested (and start stamps)
  set<pg_shard_t> peer_missing_requested;

  // i deleted these strays; ignore racing PGInfo from them
  set<pg_shard_t> stray_purged;
  set<pg_shard_t> peer_activated;

  // primary-only, recovery-only state
  set<pg_shard_t> might_have_unfound;  // These osds might have objects on them
                                       // which are unfound on the primary
  epoch_t last_peering_reset;


  /* heartbeat peers */
  void set_probe_targets(const set<pg_shard_t> &probe_set);
  void clear_probe_targets();
public:
  Mutex heartbeat_peer_lock;
  set<int> heartbeat_peers;
  set<int> probe_targets;

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
    map<hobject_t,eversion_t,hobject_t::Comparator> objects;
    bool sort_bitwise;
    hobject_t begin;
    hobject_t end;

    BackfillInterval(bool bitwise=true)
      : objects(hobject_t::Comparator(bitwise)),
	sort_bitwise(bitwise)
    {}
    
    /// clear content
    void clear(bool bitwise=true) {
      *this = BackfillInterval(bitwise);
    }

    /// clear objects list only
    void clear_objects() {
      // make sure we preserve the allocator and ordering!
      objects = map<hobject_t,eversion_t,hobject_t::Comparator>(
        hobject_t::Comparator(sort_bitwise));
    }

    /// reinstantiate with a new start+end position and sort order
    void reset(hobject_t start, bool bitwise) {
      clear(bitwise);
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
	     cmp(objects.begin()->first, soid, sort_bitwise) <= 0) {
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
      assert(!objects.empty());
      objects.erase(objects.begin());
      trim();
    }

    /// dump
    void dump(Formatter *f) const {
      f->dump_stream("begin") << begin;
      f->dump_stream("end") << end;
      f->open_array_section("objects");
      for (map<hobject_t, eversion_t, hobject_t::Comparator>::const_iterator i =
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

  friend class OSD;

public:
  set<pg_shard_t> backfill_targets;

  bool is_backfill_targets(pg_shard_t osd) {
    return backfill_targets.count(osd);
  }

protected:


  // pg waiters
  unsigned flushes_in_progress;

  // ops waiting on peered
  list<OpRequestRef>            waiting_for_peered;

  // ops waiting on active (require peered as well)
  list<OpRequestRef>            waiting_for_active;

  list<OpRequestRef>            waiting_for_cache_not_full;
  list<OpRequestRef>            waiting_for_all_missing;
  map<hobject_t, list<OpRequestRef>, hobject_t::BitwiseComparator> waiting_for_unreadable_object,
			     waiting_for_degraded_object,
			     waiting_for_blocked_object;

  set<
    hobject_t,
    hobject_t::BitwiseComparator> objects_blocked_on_cache_full;
  map<
    hobject_t,
    snapid_t,
    hobject_t::BitwiseComparator> objects_blocked_on_degraded_snap;
  map<
    hobject_t,
    ObjectContextRef,
    hobject_t::BitwiseComparator> objects_blocked_on_snap_promotion;

  // Callbacks should assume pg (and nothing else) is locked
  map<hobject_t, list<Context*>, hobject_t::BitwiseComparator> callbacks_for_degraded_object;

  map<eversion_t,
      list<pair<OpRequestRef, version_t> > > waiting_for_ack, waiting_for_ondisk;

  map<eversion_t,OpRequestRef>   replay_queue;
  void split_ops(PG *child, unsigned split_bits);

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef>, hobject_t::BitwiseComparator>& m);
  void requeue_op(OpRequestRef op);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  // for ordering writes
  ceph::shared_ptr<ObjectStore::Sequencer> osr;

  void _update_calc_stats();
  void _update_blocked_by();
  void publish_stats_to_osd();
  void clear_publish_stats();

public:
  void clear_primary_state();

 public:
  bool is_actingbackfill(pg_shard_t osd) const {
    return actingbackfill.count(osd);
  }
  bool is_acting(pg_shard_t osd) const {
    if (pool.info.ec_pool()) {
      return acting.size() > (unsigned)osd.shard && acting[osd.shard] == osd.osd;
    } else {
      return std::find(acting.begin(), acting.end(), osd.osd) != acting.end();
    }
  }
  bool is_up(pg_shard_t osd) const {
    if (pool.info.ec_pool()) {
      return up.size() > (unsigned)osd.shard && up[osd.shard] == osd.osd;
    } else {
      return std::find(up.begin(), up.end(), osd.osd) != up.end();
    }
  }
  
  bool needs_recovery() const;
  bool needs_backfill() const;

  /// get log recovery reservation priority
  unsigned get_recovery_priority();
  /// get backfill reservation priority
  unsigned get_backfill_priority();

  void mark_clean();  ///< mark an active pg clean

  bool _calc_past_interval_range(epoch_t *start, epoch_t *end, epoch_t oldest_map);
  void generate_past_intervals();
  void trim_past_intervals();
  void build_prior(std::unique_ptr<PriorSet> &prior_set);

  void remove_down_peer_info(const OSDMapRef osdmap);

  bool adjust_need_up_thru(const OSDMapRef osdmap);

  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;
  virtual void mark_all_unfound_lost(int how) = 0;
  virtual void dump_recovery_info(Formatter *f) const = 0;

  bool calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      if (peer_last_complete_ondisk.count(*i) == 0)
	return false;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[*i];
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
			pg_missing_t& omissing, pg_shard_t from);
  void proc_master_log(ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog,
		       pg_missing_t& omissing, pg_shard_t from);
  bool proc_replica_info(
    pg_shard_t from, const pg_info_t &info, epoch_t send_epoch);


  struct LogEntryTrimmer : public ObjectModDesc::Visitor {
    const hobject_t &soid;
    PG *pg;
    ObjectStore::Transaction *t;
    LogEntryTrimmer(const hobject_t &soid, PG *pg, ObjectStore::Transaction *t)
      : soid(soid), pg(pg), t(t) {}
    void rmobject(version_t old_version) {
      pg->get_pgbackend()->trim_stashed_object(
	soid,
	old_version,
	t);
    }
  };

  struct SnapRollBacker : public ObjectModDesc::Visitor {
    const hobject_t &soid;
    PG *pg;
    ObjectStore::Transaction *t;
    SnapRollBacker(const hobject_t &soid, PG *pg, ObjectStore::Transaction *t)
      : soid(soid), pg(pg), t(t) {}
    void update_snaps(set<snapid_t> &snaps) {
      pg->update_object_snap_mapping(t, soid, snaps);
    }
    void create() {
      pg->clear_object_snap_mapping(
	t,
	soid);
    }
  };

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    list<pg_log_entry_t> to_rollback;
    set<hobject_t, hobject_t::BitwiseComparator> to_remove;
    list<pg_log_entry_t> to_trim;
    
    // LogEntryHandler
    void remove(const hobject_t &hoid) {
      to_remove.insert(hoid);
    }
    void rollback(const pg_log_entry_t &entry) {
      to_rollback.push_back(entry);
    }
    void trim(const pg_log_entry_t &entry) {
      to_trim.push_back(entry);
    }

    void apply(PG *pg, ObjectStore::Transaction *t) {
      for (list<pg_log_entry_t>::iterator j = to_rollback.begin();
	   j != to_rollback.end();
	   ++j) {
	assert(j->mod_desc.can_rollback());
	pg->get_pgbackend()->rollback(j->soid, j->mod_desc, t);
	SnapRollBacker rollbacker(j->soid, pg, t);
	j->mod_desc.visit(&rollbacker);
      }
      for (set<hobject_t, hobject_t::BitwiseComparator>::iterator i = to_remove.begin();
	   i != to_remove.end();
	   ++i) {
	pg->get_pgbackend()->rollback_create(*i, t);
	pg->remove_snap_mapped_object(*t, *i);
      }
      for (list<pg_log_entry_t>::reverse_iterator i = to_trim.rbegin();
	   i != to_trim.rend();
	   ++i) {
	LogEntryTrimmer trimmer(i->soid, pg, t);
	i->mod_desc.visit(&trimmer);
      }
    }
  };
  
  friend struct SnapRollBacker;
  friend struct PGLogEntryHandler;
  friend struct LogEntryTrimmer;
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

  void check_for_lost_objects();
  void forget_lost_objects();

  void discover_all_missing(std::map<int, map<spg_t,pg_query_t> > &query_map);
  
  void trim_write_ahead();

  map<pg_shard_t, pg_info_t>::const_iterator find_best_info(
    const map<pg_shard_t, pg_info_t> &infos) const;
  static void calc_ec_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    unsigned size,
    const vector<int> &acting,
    pg_shard_t acting_primary,
    const vector<int> &up,
    pg_shard_t up_primary,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool compat_mode,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    pg_shard_t *want_primary,
    ostream &ss);
  static void calc_replicated_acting(
    map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
    unsigned size,
    const vector<int> &acting,
    pg_shard_t acting_primary,
    const vector<int> &up,
    pg_shard_t up_primary,
    const map<pg_shard_t, pg_info_t> &all_info,
    bool compat_mode,
    vector<int> *want,
    set<pg_shard_t> *backfill,
    set<pg_shard_t> *acting_backfill,
    pg_shard_t *want_primary,
    ostream &ss);
  bool choose_acting(pg_shard_t &auth_log_shard);
  void build_might_have_unfound();
  void replay_queued_ops();
  void activate(
    ObjectStore::Transaction& t,
    epoch_t activation_epoch,
    list<Context*>& tfin,
    map<int, map<spg_t,pg_query_t> >& query_map,
    map<int,
      vector<pair<pg_notify_t, pg_interval_map_t> > > *activator_map,
    RecoveryCtx *ctx);
  void _activate_committed(epoch_t epoch, epoch_t activation_epoch);
  void all_activated_and_committed();

  void proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &info);

  bool have_unfound() const { 
    return missing_loc.num_unfound();
  }
  int get_num_unfound() const {
    return missing_loc.num_unfound();
  }

  virtual void check_local() = 0;

  /**
   * @param ops_begun returns how many recovery ops the function started
   * @returns true if any useful work was accomplished; false otherwise
   */
  virtual bool start_recovery_ops(
    int max, ThreadPool::TPHandle &handle,
    int *ops_begun) = 0;

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
      active(false), queue_snap_trim(false),
      waiting_on(0), shallow_errors(0), deep_errors(0), fixed(0),
      must_scrub(false), must_deep_scrub(false), must_repair(false),
      auto_repair(false),
      num_digest_updates_pending(0),
      state(INACTIVE),
      deep(false),
      seed(0)
    {
    }

    // metadata
    set<pg_shard_t> reserved_peers;
    bool reserved, reserve_failed;
    epoch_t epoch_start;

    // common to both scrubs
    bool active;
    bool queue_snap_trim;
    int waiting_on;
    set<pg_shard_t> waiting_on_whom;
    int shallow_errors;
    int deep_errors;
    int fixed;
    ScrubMap primary_scrubmap;
    map<pg_shard_t, ScrubMap> received_maps;
    OpRequestRef active_rep_scrub;
    utime_t scrub_reg_stamp;  // stamp we registered for

    // flags to indicate explicitly requested scrubs (by admin)
    bool must_scrub, must_deep_scrub, must_repair;

    // this flag indicates whether we would like to do auto-repair of the PG or not
    bool auto_repair;

    // Maps from objects with errors to missing/inconsistent peers
    map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> missing;
    map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> inconsistent;

    // Map from object with errors to good peers
    map<hobject_t, list<pair<ScrubMap::object, pg_shard_t> >, hobject_t::BitwiseComparator> authoritative;

    // digest updates which we are waiting on
    int num_digest_updates_pending;

    // chunky scrub
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
      WAIT_DIGEST_UPDATES,
      FINISH,
    } state;

    // deep scrub
    bool deep;
    uint32_t seed;

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
        case WAIT_DIGEST_UPDATES: ret = "WAIT_DIGEST_UPDATES"; break;
        case FINISH: ret = "FINISH"; break;
      }
      return ret;
    }

    bool is_chunky_scrub_active() const { return state != INACTIVE; }

    // classic (non chunk) scrubs block all writes
    // chunky scrubs only block writes to a range
    bool write_blocked_by_scrub(const hobject_t &soid, bool sort_bitwise) {
      if (cmp(soid, start, sort_bitwise) >= 0 &&
	  cmp(soid, end, sort_bitwise) < 0)
	return true;

      return false;
    }

    // clear all state
    void reset() {
      active = false;
      queue_snap_trim = false;
      waiting_on = 0;
      waiting_on_whom.clear();
      if (active_rep_scrub) {
        active_rep_scrub = OpRequestRef();
      }
      received_maps.clear();

      must_scrub = false;
      must_deep_scrub = false;
      must_repair = false;
      auto_repair = false;

      state = PG::Scrubber::INACTIVE;
      start = hobject_t();
      end = hobject_t();
      subset_last_update = eversion_t();
      shallow_errors = 0;
      deep_errors = 0;
      fixed = 0;
      deep = false;
      seed = 0;
      run_callbacks();
      inconsistent.clear();
      missing.clear();
      authoritative.clear();
      num_digest_updates_pending = 0;
    }

  } scrubber;

  bool scrub_after_recovery;

  int active_pushes;

  void repair_object(
    const hobject_t& soid, list<pair<ScrubMap::object, pg_shard_t> > *ok_peers,
    pg_shard_t bad_peer);

  void scrub(epoch_t queued, ThreadPool::TPHandle &handle);
  void chunky_scrub(ThreadPool::TPHandle &handle);
  void scrub_compare_maps();
  /**
   * return true if any inconsistency/missing is repaired, false otherwise
   */
  bool scrub_process_inconsistent();
  void scrub_finish();
  void scrub_clear_state();
  void _scan_snaps(ScrubMap &map);
  void _scan_rollback_obs(
    const vector<ghobject_t> &rollback_obs,
    ThreadPool::TPHandle &handle);
  void _request_scrub_map(pg_shard_t replica, eversion_t version,
                          hobject_t start, hobject_t end, bool deep,
			  uint32_t seed);
  int build_scrub_map_chunk(
    ScrubMap &map,
    hobject_t start, hobject_t end, bool deep, uint32_t seed,
    ThreadPool::TPHandle &handle);
  /**
   * returns true if [begin, end) is good to scrub at this time
   * a false return value obliges the implementer to requeue scrub when the
   * condition preventing scrub clears
   */
  virtual bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end) = 0;
  virtual void _scrub(
    ScrubMap &map,
    const std::map<hobject_t, pair<uint32_t, uint32_t>, hobject_t::BitwiseComparator> &missing_digest) { }
  virtual void _scrub_clear_state() { }
  virtual void _scrub_finish() { }
  virtual void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction *t) = 0;
  virtual bool _report_snap_collection_errors(
    const hobject_t &hoid,
    const map<string, bufferptr> &attrs,
    pg_shard_t osd,
    ostream &out) { return false; }
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;
  bool sched_scrub();
  void reg_next_scrub();
  void unreg_next_scrub();

  void replica_scrub(
    OpRequestRef op,
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
  typedef ceph::shared_ptr<CephPeeringEvt> CephPeeringEvtRef;
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
    pg_shard_t from;
    pg_info_t info;
    epoch_t msg_epoch;
    MInfoRec(pg_shard_t from, pg_info_t &info, epoch_t msg_epoch) :
      from(from), info(info), msg_epoch(msg_epoch) {}
    void print(std::ostream *out) const {
      *out << "MInfoRec from " << from << " info: " << info;
    }
  };

  struct MLogRec : boost::statechart::event< MLogRec > {
    pg_shard_t from;
    boost::intrusive_ptr<MOSDPGLog> msg;
    MLogRec(pg_shard_t from, MOSDPGLog *msg) :
      from(from), msg(msg) {}
    void print(std::ostream *out) const {
      *out << "MLogRec from " << from;
    }
  };

  struct MNotifyRec : boost::statechart::event< MNotifyRec > {
    pg_shard_t from;
    pg_notify_t notify;
    uint64_t features;
    MNotifyRec(pg_shard_t from, pg_notify_t &notify, uint64_t f) :
      from(from), notify(notify), features(f) {}
    void print(std::ostream *out) const {
      *out << "MNotifyRec from " << from << " notify: " << notify
        << " features: 0x" << hex << features << dec;
    }
  };

  struct MQuery : boost::statechart::event< MQuery > {
    pg_shard_t from;
    pg_query_t query;
    epoch_t query_epoch;
    MQuery(pg_shard_t from, const pg_query_t &query, epoch_t query_epoch):
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
    Activate(epoch_t q) : boost::statechart::event< Activate >(),
			  activation_epoch(q) {}
    void print(std::ostream *out) const {
      *out << "Activate from " << activation_epoch;
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
  TrivialEvent(AllBackfillsReserved)
  TrivialEvent(Recovering)
  TrivialEvent(GoClean)

  TrivialEvent(AllReplicasActivated)

  TrivialEvent(IntervalFlush)

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
	assert(state->rctx);
	assert(state->rctx->transaction);
	return state->rctx->transaction;
      }

      void send_query(pg_shard_t to, const pg_query_t &query) {
	assert(state->rctx);
	assert(state->rctx->query_map);
	(*state->rctx->query_map)[to.osd][spg_t(pg->info.pgid.pgid, to.shard)] =
	  query;
      }

      map<int, map<spg_t, pg_query_t> > *get_query_map() {
	assert(state->rctx);
	assert(state->rctx->query_map);
	return state->rctx->query_map;
      }

      map<int, vector<pair<pg_notify_t, pg_interval_map_t> > > *get_info_map() {
	assert(state->rctx);
	assert(state->rctx->info_map);
	return state->rctx->info_map;
      }

      list< Context* > *get_on_safe_context_list() {
	assert(state->rctx);
	assert(state->rctx->on_safe);
	return &(state->rctx->on_safe->contexts);
      }

      list< Context * > *get_on_applied_context_list() {
	assert(state->rctx);
	assert(state->rctx->on_applied);
	return &(state->rctx->on_applied->contexts);
      }

      RecoveryCtx *get_recovery_ctx() { return &*(state->rctx); }

      void send_notify(pg_shard_t to,
		       const pg_notify_t &info, const pg_interval_map_t &pi) {
	assert(state->rctx);
	assert(state->rctx->notify_list);
	(*state->rctx->notify_list)[to.osd].push_back(make_pair(info, pi));
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
	boost::statechart::custom_reaction< IntervalFlush >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const IntervalFlush&);
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
	boost::statechart::custom_reaction< IntervalFlush >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const QueryState& q);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const FlushedEvt&);
      boost::statechart::result react(const IntervalFlush&);
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
	boost::statechart::transition< NeedActingChange, WaitActingChange >
	> reactions;
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
      std::unique_ptr< PriorSet > prior_set;

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
	boost::statechart::custom_reaction< RemoteReservationRejected >,
	boost::statechart::transition< AllBackfillsReserved, Backfilling >
	> reactions;
      set<pg_shard_t>::const_iterator backfill_osd_it;
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
	boost::statechart::transition< RequestBackfill, WaitLocalBackfillReserved>,
	boost::statechart::custom_reaction< RemoteBackfillReserved >,
	boost::statechart::custom_reaction< RemoteReservationRejected >
	> reactions;
      NotBackfilling(my_context ctx);
      void exit();
      boost::statechart::result react(const RemoteBackfillReserved& evt);
      boost::statechart::result react(const RemoteReservationRejected& evt);
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
	boost::statechart::transition< RemoteReservationRejected, RepNotRecovering >,
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
      set<pg_shard_t>::const_iterator remote_recovery_reservation_it;
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
      set<pg_shard_t> peer_info_requested;

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
      pg_shard_t auth_log_shard;
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

    struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
      set<pg_shard_t> peer_missing_requested;

      GetMissing(my_context ctx);
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
      WaitUpThru(my_context ctx);
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

    struct Incomplete : boost::statechart::state< Incomplete, Peering>, NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      Incomplete(my_context ctx);
      boost::statechart::result react(const AdvMap &advmap);
      boost::statechart::result react(const MNotifyRec& infoevt);
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
    RecoveryState(PG *pg)
      : machine(this, pg), pg(pg), orig_ctx(0) {
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
     const PGPool &pool, spg_t p);
  virtual ~PG();

 private:
  // Prevent copying
  explicit PG(const PG& rhs);
  PG& operator=(const PG& rhs);
  const spg_t pg_id;
  uint64_t peer_features;
  uint64_t acting_features;
  uint64_t upacting_features;

  bool do_sort_bitwise;
  epoch_t last_epoch;

 public:
  const spg_t&      get_pgid() const { return pg_id; }
  int        get_nrep() const { return acting.size(); }

  void reset_min_peer_features() {
    peer_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  }
  uint64_t get_min_peer_features() const { return peer_features; }
  void apply_peer_features(uint64_t f) { peer_features &= f; }

  uint64_t get_min_acting_features() const { return acting_features; }
  uint64_t get_min_upacting_features() const { return upacting_features; }

  /// true if we will sort hobjects bitwise for this pg interval
  bool get_sort_bitwise() const {
    return do_sort_bitwise;
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
	    pool.info.ec_pool() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
    up = newup;
    if (!pool.info.ec_pool()) {
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
    assert(up_primary.osd == new_up_primary);
    assert(primary.osd == new_acting_primary);
  }
  pg_shard_t get_primary() const { return primary; }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  bool       is_primary() const { return pg_whoami == primary; }
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
  bool       is_activating() const { return state_test(PG_STATE_ACTIVATING); }
  bool       is_peering() const { return state_test(PG_STATE_PEERING); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool       is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool       is_undersized() const { return state_test(PG_STATE_UNDERSIZED); }

  bool       is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }
  bool       is_peered() const {
    return state_test(PG_STATE_ACTIVE) || state_test(PG_STATE_PEERED);
  }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  void init(
    int role,
    const vector<int>& up,
    int up_primary,
    const vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    pg_interval_map_t& pim,
    bool backfill,
    ObjectStore::Transaction *t);

  // pg on-disk state
  void do_pending_flush();

  static void _create(ObjectStore::Transaction& t, spg_t pgid, int bits);
  static void _init(ObjectStore::Transaction& t,
		    spg_t pgid, const pg_pool_t *pool);

private:
  void prepare_write_info(map<string,bufferlist> *km);

public:
  static int _prepare_write_info(map<string,bufferlist> *km,
    epoch_t epoch,
    pg_info_t &info, coll_t coll,
    map<epoch_t,pg_interval_t> &past_intervals,
    ghobject_t &pgmeta_oid,
    bool dirty_big_info,
    bool dirty_epoch);
  void write_if_dirty(ObjectStore::Transaction& t);

  eversion_t get_next_version() const {
    eversion_t at_version(get_osdmap()->get_epoch(),
			  pg_log.get_head().version+1);
    assert(at_version > info.last_update);
    assert(at_version > pg_log.get_head());
    return at_version;
  }

  void add_log_entry(const pg_log_entry_t& e);
  void append_log(
    const vector<pg_log_entry_t>& logv,
    eversion_t trim_to,
    eversion_t trim_rollback_to,
    ObjectStore::Transaction &t,
    bool transaction_applied = true);
  bool check_log_for_corruption(ObjectStore *store);
  void trim_peers();

  std::string get_corrupt_pg_log_name() const;
  static int read_info(
    ObjectStore *store, spg_t pgid, const coll_t &coll,
    bufferlist &bl, pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
    __u8 &);
  void read_state(ObjectStore *store, bufferlist &bl);
  static bool _has_removal_flag(ObjectStore *store, spg_t pgid);
  static int peek_map_epoch(ObjectStore *store, spg_t pgid,
			    epoch_t *pepoch, bufferlist *bl);
  void update_snap_map(
    const vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(vector<snapid_t> &snaps);

  void log_weirdness();

  void queue_snap_trim();
  bool requeue_scrub();
  bool queue_scrub();

  /// share pg info after a pg is active
  void share_pg_info();
  /// share new pg log entries after a pg is active
  void share_pg_log();

  void reset_interval_flush();
  void start_peering_interval(
    const OSDMapRef lastmap,
    const vector<int>& newup, int up_primary,
    const vector<int>& newacting, int acting_primary,
    ObjectStore::Transaction *t);
  void on_new_interval();
  virtual void _on_new_interval() = 0;
  void start_flush(ObjectStore::Transaction *t,
		   list<Context *> *on_applied,
		   list<Context *> *on_safe);
  void set_last_peering_reset();
  bool pg_has_reset_since(epoch_t e) {
    assert(is_locked());
    return deleting || e < get_last_peering_reset();
  }

  void update_history_from_master(pg_history_t new_history);
  void fulfill_info(pg_shard_t from, const pg_query_t &query,
		    pair<pg_shard_t, pg_info_t> &notify_info);
  void fulfill_log(pg_shard_t from, const pg_query_t &query, epoch_t query_epoch);

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

  static bool op_must_wait_for_map(epoch_t cur_epoch, OpRequestRef& op);

  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);
  bool old_peering_evt(CephPeeringEvtRef evt) {
    return old_peering_msg(evt->get_epoch_sent(), evt->get_epoch_requested());
  }
  static bool have_same_or_newer_map(epoch_t cur_epoch, epoch_t e) {
    return e <= cur_epoch;
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef& op);


  // recovery bits
  void take_waiters();
  void queue_peering_event(CephPeeringEvtRef evt);
  void handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx);
  void queue_query(epoch_t msg_epoch, epoch_t query_epoch,
		   pg_shard_t from, const pg_query_t& q);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(
    OSDMapRef osdmap, OSDMapRef lastmap,
    vector<int>& newup, int up_primary,
    vector<int>& newacting, int acting_primary,
    RecoveryCtx *rctx);
  void handle_activate_map(RecoveryCtx *rctx);
  void handle_create(RecoveryCtx *rctx);
  void handle_loaded(RecoveryCtx *rctx);
  void handle_query_state(Formatter *f);

  virtual void on_removal(ObjectStore::Transaction *t) = 0;


  // abstract bits
  virtual void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle
  ) = 0;

  virtual void do_op(OpRequestRef& op) = 0;
  virtual void do_sub_op(OpRequestRef op) = 0;
  virtual void do_sub_op_reply(OpRequestRef op) = 0;
  virtual void do_scan(
    OpRequestRef op,
    ThreadPool::TPHandle &handle
  ) = 0;
  virtual void do_backfill(OpRequestRef op) = 0;
  virtual void snap_trimmer(epoch_t epoch_queued) = 0;

  virtual int do_command(cmdmap_t cmdmap, ostream& ss,
			 bufferlist& idata, bufferlist& odata) = 0;

  virtual void on_role_change() = 0;
  virtual void on_pool_change() = 0;
  virtual void on_change(ObjectStore::Transaction *t) = 0;
  virtual void on_activate() = 0;
  virtual void on_flushed() = 0;
  virtual void on_shutdown() = 0;
  virtual void check_blacklisted_watchers() = 0;
  virtual void get_watchers(std::list<obj_watch_item_t>&) = 0;

  virtual bool agent_work(int max) = 0;
  virtual bool agent_work(int max, int agent_flush_quota) = 0;
  virtual void agent_stop() = 0;
  virtual void agent_delay() = 0;
  virtual void agent_clear() = 0;
  virtual void agent_choose_mode_restart() = 0;
};

ostream& operator<<(ostream& out, const PG& pg);

ostream& operator<<(ostream& out, const PG::BackfillInterval& bi);

#endif
