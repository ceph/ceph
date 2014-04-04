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

#ifndef CEPH_OSD_H
#define CEPH_OSD_H

#include "boost/tuple/tuple.hpp"

#include "PG.h"

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/LogClient.h"
#include "common/AsyncReserver.h"
#include "common/ceph_context.h"

#include "os/ObjectStore.h"
#include "OSDCap.h"

#include "osd/ClassHandler.h"

#include "include/CompatSet.h"

#include "auth/KeyRing.h"
#include "messages/MOSDRepScrub.h"
#include "OpRequest.h"

#include <map>
#include <memory>
#include "include/memory.h"
using namespace std;

#include "include/unordered_map.h"
#include "include/unordered_set.h"

#include "Watch.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/sharedptr_registry.hpp"
#include "common/PrioritizedQueue.h"

#define CEPH_OSD_PROTOCOL    10 /* cluster internal */


enum {
  l_osd_first = 10000,
  l_osd_opq,
  l_osd_op_wip,
  l_osd_op,
  l_osd_op_inb,
  l_osd_op_outb,
  l_osd_op_lat,
  l_osd_op_process_lat,
  l_osd_op_r,
  l_osd_op_r_outb,
  l_osd_op_r_lat,
  l_osd_op_r_process_lat,
  l_osd_op_w,
  l_osd_op_w_inb,
  l_osd_op_w_rlat,
  l_osd_op_w_lat,
  l_osd_op_w_process_lat,
  l_osd_op_rw,
  l_osd_op_rw_inb,
  l_osd_op_rw_outb,
  l_osd_op_rw_rlat,
  l_osd_op_rw_lat,
  l_osd_op_rw_process_lat,

  l_osd_sop,
  l_osd_sop_inb,
  l_osd_sop_lat,
  l_osd_sop_w,
  l_osd_sop_w_inb,
  l_osd_sop_w_lat,
  l_osd_sop_pull,
  l_osd_sop_pull_lat,
  l_osd_sop_push,
  l_osd_sop_push_inb,
  l_osd_sop_push_lat,

  l_osd_pull,
  l_osd_push,
  l_osd_push_outb,

  l_osd_push_in,
  l_osd_push_inb,

  l_osd_rop,

  l_osd_loadavg,
  l_osd_buf,

  l_osd_pg,
  l_osd_pg_primary,
  l_osd_pg_replica,
  l_osd_pg_stray,
  l_osd_hb_to,
  l_osd_hb_from,
  l_osd_map,
  l_osd_mape,
  l_osd_mape_dup,

  l_osd_waiting_for_map,

  l_osd_stat_bytes,
  l_osd_stat_bytes_used,
  l_osd_stat_bytes_avail,

  l_osd_copyfrom,

  l_osd_tier_promote,
  l_osd_tier_flush,
  l_osd_tier_flush_fail,
  l_osd_tier_try_flush,
  l_osd_tier_try_flush_fail,
  l_osd_tier_evict,
  l_osd_tier_whiteout,
  l_osd_tier_dirty,
  l_osd_tier_clean,

  l_osd_agent_wake,
  l_osd_agent_skip,
  l_osd_agent_flush,
  l_osd_agent_evict,

  l_osd_last,
};

// RecoveryState perf counters
enum {
  rs_first = 20000,
  rs_initial_latency,
  rs_started_latency,
  rs_reset_latency,
  rs_start_latency,
  rs_primary_latency,
  rs_peering_latency,
  rs_backfilling_latency,
  rs_waitremotebackfillreserved_latency,
  rs_waitlocalbackfillreserved_latency,
  rs_notbackfilling_latency,
  rs_repnotrecovering_latency,
  rs_repwaitrecoveryreserved_latency,
  rs_repwaitbackfillreserved_latency,
  rs_RepRecovering_latency,
  rs_activating_latency,
  rs_waitlocalrecoveryreserved_latency,
  rs_waitremoterecoveryreserved_latency,
  rs_recovering_latency,
  rs_recovered_latency,
  rs_clean_latency,
  rs_active_latency,
  rs_replicaactive_latency,
  rs_stray_latency,
  rs_getinfo_latency,
  rs_getlog_latency,
  rs_waitactingchange_latency,
  rs_incomplete_latency,
  rs_getmissing_latency,
  rs_waitupthru_latency,
  rs_last,
};

class Messenger;
class Message;
class MonClient;
class PerfCounters;
class ObjectStore;
class OSDMap;
class MLog;
class MClass;
class MOSDPGMissing;
class Objecter;

class Watch;
class Notification;
class ReplicatedPG;

class AuthAuthorizeHandlerRegistry;

class OpsFlightSocketHook;
class HistoricOpsSocketHook;
class TestOpsSocketHook;
struct C_CompleteSplits;

typedef ceph::shared_ptr<ObjectStore::Sequencer> SequencerRef;

class DeletingState {
  Mutex lock;
  Cond cond;
  enum {
    QUEUED,
    CLEARING_DIR,
    CLEARING_WAITING,
    DELETING_DIR,
    DELETED_DIR,
    CANCELED,
  } status;
  bool stop_deleting;
public:
  const spg_t pgid;
  const PGRef old_pg_state;
  DeletingState(const pair<spg_t, PGRef> &in) :
    lock("DeletingState::lock"), status(QUEUED), stop_deleting(false),
    pgid(in.first), old_pg_state(in.second) {}

  /// transition status to clearing
  bool start_clearing() {
    Mutex::Locker l(lock);
    assert(
      status == QUEUED ||
      status == DELETED_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = CLEARING_DIR;
    return true;
  } ///< @return false if we should cancel deletion

  /// transition status to CLEARING_WAITING
  bool pause_clearing() {
    Mutex::Locker l(lock);
    assert(status == CLEARING_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = CLEARING_WAITING;
    return true;
  } ///< @return false if we should cancel deletion

  /// transition status to CLEARING_DIR
  bool resume_clearing() {
    Mutex::Locker l(lock);
    assert(status == CLEARING_WAITING);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = CLEARING_DIR;
    return true;
  } ///< @return false if we should cancel deletion

  /// transition status to deleting
  bool start_deleting() {
    Mutex::Locker l(lock);
    assert(status == CLEARING_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = DELETING_DIR;
    return true;
  } ///< @return false if we should cancel deletion

  /// signal collection removal queued
  void finish_deleting() {
    Mutex::Locker l(lock);
    assert(status == DELETING_DIR);
    status = DELETED_DIR;
    cond.Signal();
  }

  /// try to halt the deletion
  bool try_stop_deletion() {
    Mutex::Locker l(lock);
    stop_deleting = true;
    /**
     * If we are in DELETING_DIR or CLEARING_DIR, there are in progress
     * operations we have to wait for before continuing on.  States
     * CLEARING_WAITING and QUEUED indicate that the remover will check
     * stop_deleting before queueing any further operations.  CANCELED
     * indicates that the remover has already halted.  DELETED_DIR
     * indicates that the deletion has been fully queueud.
     */
    while (status == DELETING_DIR || status == CLEARING_DIR)
      cond.Wait(lock);
    return status != DELETED_DIR;
  } ///< @return true if we don't need to recreate the collection
};
typedef ceph::shared_ptr<DeletingState> DeletingStateRef;

class OSD;
class OSDService {
public:
  OSD *osd;
  CephContext *cct;
  SharedPtrRegistry<spg_t, ObjectStore::Sequencer> osr_registry;
  SharedPtrRegistry<spg_t, DeletingState> deleting_pgs;
  const int whoami;
  ObjectStore *&store;
  LogClient &clog;
  PGRecoveryStats &pg_recovery_stats;
  hobject_t infos_oid;
private:
  Messenger *&cluster_messenger;
  Messenger *&client_messenger;
public:
  PerfCounters *&logger;
  PerfCounters *&recoverystate_perf;
  MonClient   *&monc;
  ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>, PGRef> &op_wq;
  ThreadPool::BatchWorkQueue<PG> &peering_wq;
  ThreadPool::WorkQueue<PG> &recovery_wq;
  ThreadPool::WorkQueue<PG> &snap_trim_wq;
  ThreadPool::WorkQueue<PG> &scrub_wq;
  ThreadPool::WorkQueue<PG> &scrub_finalize_wq;
  ThreadPool::WorkQueue<MOSDRepScrub> &rep_scrub_wq;
  GenContextWQ push_wq;
  GenContextWQ gen_wq;
  ClassHandler  *&class_handler;

  void dequeue_pg(PG *pg, list<OpRequestRef> *dequeued);

  // -- superblock --
  Mutex publish_lock, pre_publish_lock; // pre-publish orders before publish
  OSDSuperblock superblock;
  OSDSuperblock get_superblock() {
    Mutex::Locker l(publish_lock);
    return superblock;
  }
  void publish_superblock(const OSDSuperblock &block) {
    Mutex::Locker l(publish_lock);
    superblock = block;
  }

  int get_nodeid() const { return whoami; }

  OSDMapRef osdmap;
  OSDMapRef get_osdmap() {
    Mutex::Locker l(publish_lock);
    return osdmap;
  }
  void publish_map(OSDMapRef map) {
    Mutex::Locker l(publish_lock);
    osdmap = map;
  }

  /*
   * osdmap - current published amp
   * next_osdmap - pre_published map that is about to be published.
   *
   * We use the next_osdmap to send messages and initiate connections,
   * but only if the target is the same instance as the one in the map
   * epoch the current user is working from (i.e., the result is
   * equivalent to what is in next_osdmap).
   *
   * This allows the helpers to start ignoring osds that are about to
   * go down, and let OSD::handle_osd_map()/note_down_osd() mark them
   * down, without worrying about reopening connections from threads
   * working from old maps.
   */
  OSDMapRef next_osdmap;
  void pre_publish_map(OSDMapRef map) {
    Mutex::Locker l(pre_publish_lock);
    next_osdmap = map;
  }

  void activate_map();

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch);
  pair<ConnectionRef,ConnectionRef> get_con_osd_hb(int peer, epoch_t from_epoch);  // (back, front)
  void send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch);
  void send_message_osd_cluster(Message *m, Connection *con) {
    cluster_messenger->send_message(m, con);
  }
  void send_message_osd_cluster(Message *m, const ConnectionRef& con) {
    cluster_messenger->send_message(m, con.get());
  }
  void send_message_osd_client(Message *m, Connection *con) {
    client_messenger->send_message(m, con);
  }
  void send_message_osd_client(Message *m, const ConnectionRef& con) {
    client_messenger->send_message(m, con.get());
  }
  entity_name_t get_cluster_msgr_name() {
    return cluster_messenger->get_myname();
  }

  // -- scrub scheduling --
  Mutex sched_scrub_lock;
  int scrubs_pending;
  int scrubs_active;
  set< pair<utime_t,spg_t> > last_scrub_pg;

  void reg_last_pg_scrub(spg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    last_scrub_pg.insert(pair<utime_t,spg_t>(t, pgid));
  }
  void unreg_last_pg_scrub(spg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    pair<utime_t,spg_t> p(t, pgid);
    set<pair<utime_t,spg_t> >::iterator it = last_scrub_pg.find(p);
    assert(it != last_scrub_pg.end());
    last_scrub_pg.erase(it);
  }
  bool first_scrub_stamp(pair<utime_t, spg_t> *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (last_scrub_pg.empty())
      return false;
    set< pair<utime_t, spg_t> >::iterator iter = last_scrub_pg.begin();
    *out = *iter;
    return true;
  }
  bool next_scrub_stamp(pair<utime_t, spg_t> next,
			pair<utime_t, spg_t> *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (last_scrub_pg.empty())
      return false;
    set< pair<utime_t, spg_t> >::iterator iter = last_scrub_pg.lower_bound(next);
    if (iter == last_scrub_pg.end())
      return false;
    ++iter;
    if (iter == last_scrub_pg.end())
      return false;
    *out = *iter;
    return true;
  }

  bool inc_scrubs_pending();
  void inc_scrubs_active(bool reserved);
  void dec_scrubs_pending();
  void dec_scrubs_active();

  void reply_op_error(OpRequestRef op, int err);
  void reply_op_error(OpRequestRef op, int err, eversion_t v, version_t uv);
  void handle_misdirected_op(PG *pg, OpRequestRef op);


  // -- agent shared state --
  Mutex agent_lock;
  Cond agent_cond;
  map<uint64_t, set<PGRef> > agent_queue;
  set<PGRef>::iterator agent_queue_pos;
  bool agent_valid_iterator;
  int agent_ops;
  set<hobject_t> agent_oids;
  bool agent_active;
  struct AgentThread : public Thread {
    OSDService *osd;
    AgentThread(OSDService *o) : osd(o) {}
    void *entry() {
      osd->agent_entry();
      return NULL;
    }
  } agent_thread;
  bool agent_stop_flag;

  void agent_entry();
  void agent_stop();

  void _enqueue(PG *pg, uint64_t priority) {
    if (!agent_queue.empty() &&
	agent_queue.rbegin()->first < priority)
      agent_valid_iterator = false;  // inserting higher-priority queue
    set<PGRef>& nq = agent_queue[priority];
    if (nq.empty())
      agent_cond.Signal();
    nq.insert(pg);
  }

  void _dequeue(PG *pg, uint64_t old_priority) {
    set<PGRef>& oq = agent_queue[old_priority];
    set<PGRef>::iterator p = oq.find(pg);
    assert(p != oq.end());
    if (p == agent_queue_pos)
      ++agent_queue_pos;
    oq.erase(p);
    if (oq.empty()) {
      if (agent_queue.rbegin()->first == old_priority)
	agent_valid_iterator = false;
      agent_queue.erase(old_priority);
    }
  }

  /// enable agent for a pg
  void agent_enable_pg(PG *pg, uint64_t priority) {
    Mutex::Locker l(agent_lock);
    _enqueue(pg, priority);
  }

  /// adjust priority for an enagled pg
  void agent_adjust_pg(PG *pg, uint64_t old_priority, uint64_t new_priority) {
    Mutex::Locker l(agent_lock);
    assert(new_priority != old_priority);
    _enqueue(pg, new_priority);
    _dequeue(pg, old_priority);
  }

  /// disable agent for a pg
  void agent_disable_pg(PG *pg, uint64_t old_priority) {
    Mutex::Locker l(agent_lock);
    _dequeue(pg, old_priority);
  }

  /// note start of an async (flush) op
  void agent_start_op(const hobject_t& oid) {
    Mutex::Locker l(agent_lock);
    ++agent_ops;
    assert(agent_oids.count(oid) == 0);
    agent_oids.insert(oid);
  }

  /// note finish or cancellation of an async (flush) op
  void agent_finish_op(const hobject_t& oid) {
    Mutex::Locker l(agent_lock);
    assert(agent_ops > 0);
    --agent_ops;
    assert(agent_oids.count(oid) == 1);
    agent_oids.erase(oid);
    agent_cond.Signal();
  }

  /// check if we are operating on an object
  bool agent_is_active_oid(const hobject_t& oid) {
    Mutex::Locker l(agent_lock);
    return agent_oids.count(oid);
  }

  /// get count of active agent ops
  int agent_get_num_ops() {
    Mutex::Locker l(agent_lock);
    return agent_ops;
  }


  // -- Objecter, for teiring reads/writes from/to other OSDs --
  Mutex objecter_lock;
  SafeTimer objecter_timer;
  OSDMap objecter_osdmap;
  Objecter *objecter;
  Finisher objecter_finisher;
  struct ObjecterDispatcher : public Dispatcher {
    OSDService *osd;
    bool ms_dispatch(Message *m);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con) {}
    void ms_handle_connect(Connection *con);
    bool ms_get_authorizer(int dest_type,
			   AuthAuthorizer **authorizer,
			   bool force_new);
    ObjecterDispatcher(OSDService *o) : Dispatcher(cct), osd(o) {}
  } objecter_dispatcher;
  friend struct ObjecterDispatcher;


  // -- Watch --
  Mutex watch_lock;
  SafeTimer watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    Mutex::Locker l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }

  // -- Backfill Request Scheduling --
  Mutex backfill_request_lock;
  SafeTimer backfill_request_timer;

  // -- tids --
  // for ops i issue
  ceph_tid_t last_tid;
  Mutex tid_lock;
  ceph_tid_t get_tid() {
    ceph_tid_t t;
    tid_lock.Lock();
    t = ++last_tid;
    tid_lock.Unlock();
    return t;
  }

  // -- backfill_reservation --
  enum {
    BACKFILL_LOW = 0,   // backfill non-degraded PGs
    BACKFILL_HIGH = 1,	// backfill degraded PGs
    RECOVERY = AsyncReserver<spg_t>::MAX_PRIORITY  // log based recovery
  };
  Finisher reserver_finisher;
  AsyncReserver<spg_t> local_reserver;
  AsyncReserver<spg_t> remote_reserver;

  // -- pg_temp --
  Mutex pg_temp_lock;
  map<pg_t, vector<int> > pg_temp_wanted;
  void queue_want_pg_temp(pg_t pgid, vector<int>& want);
  void remove_want_pg_temp(pg_t pgid) {
    Mutex::Locker l(pg_temp_lock);
    pg_temp_wanted.erase(pgid);
  }
  void send_pg_temp();

  void queue_for_peering(PG *pg);
  bool queue_for_recovery(PG *pg);
  bool queue_for_snap_trim(PG *pg) {
    return snap_trim_wq.queue(pg);
  }
  bool queue_for_scrub(PG *pg) {
    return scrub_wq.queue(pg);
  }

  // osd map cache (past osd maps)
  Mutex map_cache_lock;
  SharedLRU<epoch_t, const OSDMap> map_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_inc_cache;

  OSDMapRef try_get_map(epoch_t e);
  OSDMapRef get_map(epoch_t e) {
    OSDMapRef ret(try_get_map(e));
    assert(ret);
    return ret;
  }
  OSDMapRef add_map(OSDMap *o) {
    Mutex::Locker l(map_cache_lock);
    return _add_map(o);
  }
  OSDMapRef _add_map(OSDMap *o);

  void add_map_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl);
  void _add_map_bl(epoch_t e, bufferlist& bl);
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _get_map_bl(e, bl);
  }
  bool _get_map_bl(epoch_t e, bufferlist& bl);

  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl);
  void _add_map_inc_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map_bl(epoch_t e, bufferlist& bl);

  void clear_map_bl_cache_pins(epoch_t e);

  void need_heartbeat_peer_update();

  void pg_stat_queue_enqueue(PG *pg);
  void pg_stat_queue_dequeue(PG *pg);

  void init();
  void shutdown();

  // split
  Mutex in_progress_split_lock;
  map<spg_t, spg_t> pending_splits; // child -> parent
  map<spg_t, set<spg_t> > rev_pending_splits; // parent -> [children]
  set<spg_t> in_progress_splits;       // child

  void _start_split(spg_t parent, const set<spg_t> &children);
  void start_split(spg_t parent, const set<spg_t> &children) {
    Mutex::Locker l(in_progress_split_lock);
    return _start_split(parent, children);
  }
  void mark_split_in_progress(spg_t parent, const set<spg_t> &pgs);
  void complete_split(const set<spg_t> &pgs);
  void cancel_pending_splits_for_parent(spg_t parent);
  void _cancel_pending_splits_for_parent(spg_t parent);
  bool splitting(spg_t pgid);
  void expand_pg_num(OSDMapRef old_map,
		     OSDMapRef new_map);
  void _maybe_split_pgid(OSDMapRef old_map,
			 OSDMapRef new_map,
			 spg_t pgid);
  void init_splits_between(spg_t pgid, OSDMapRef frommap, OSDMapRef tomap);

  // -- OSD Full Status --
  Mutex full_status_lock;
  enum s_names { NONE, NEAR, FULL } cur_state;
  time_t last_msg;
  double cur_ratio;
  float get_full_ratio();
  float get_nearfull_ratio();
  void check_nearfull_warning(const osd_stat_t &stat);
  bool check_failsafe_full();
  bool too_full_for_backfill(double *ratio, double *max_ratio);


  // -- stopping --
  Mutex is_stopping_lock;
  Cond is_stopping_cond;
  enum {
    NOT_STOPPING,
    PREPARING_TO_STOP,
    STOPPING } state;
  bool is_stopping() {
    Mutex::Locker l(is_stopping_lock);
    return state == STOPPING;
  }
  bool is_preparing_to_stop() {
    Mutex::Locker l(is_stopping_lock);
    return state == PREPARING_TO_STOP;
  }
  bool prepare_to_stop();
  void got_stop_ack();


#ifdef PG_DEBUG_REFS
  Mutex pgid_lock;
  map<spg_t, int> pgid_tracker;
  map<spg_t, PG*> live_pgs;
  void add_pgid(spg_t pgid, PG *pg) {
    Mutex::Locker l(pgid_lock);
    if (!pgid_tracker.count(pgid)) {
      pgid_tracker[pgid] = 0;
      live_pgs[pgid] = pg;
    }
    pgid_tracker[pgid]++;
  }
  void remove_pgid(spg_t pgid, PG *pg) {
    Mutex::Locker l(pgid_lock);
    assert(pgid_tracker.count(pgid));
    assert(pgid_tracker[pgid] > 0);
    pgid_tracker[pgid]--;
    if (pgid_tracker[pgid] == 0) {
      pgid_tracker.erase(pgid);
      live_pgs.erase(pgid);
    }
  }
  void dump_live_pgids() {
    Mutex::Locker l(pgid_lock);
    derr << "live pgids:" << dendl;
    for (map<spg_t, int>::iterator i = pgid_tracker.begin();
	 i != pgid_tracker.end();
	 ++i) {
      derr << "\t" << *i << dendl;
      live_pgs[i->first]->dump_live_ids();
    }
  }
#endif

  OSDService(OSD *osd);
  ~OSDService();
};

struct C_OSD_SendMessageOnConn: public Context {
  OSDService *osd;
  Message *reply;
  ConnectionRef conn;
  C_OSD_SendMessageOnConn(
    OSDService *osd,
    Message *reply,
    ConnectionRef conn) : osd(osd), reply(reply), conn(conn) {}
  void finish(int) {
    osd->send_message_osd_cluster(reply, conn.get());
  }
};

class OSD : public Dispatcher,
	    public md_config_obs_t {
  /** OSD **/
public:
  // config observer bits
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

protected:
  Mutex osd_lock;			// global lock
  SafeTimer tick_timer;    // safe timer (osd_lock)

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

  Messenger   *cluster_messenger;
  Messenger   *client_messenger;
  Messenger   *objecter_messenger;
  MonClient   *monc; // check the "monc helpers" list before accessing directly
  PerfCounters      *logger;
  PerfCounters      *recoverystate_perf;
  ObjectStore *store;

  LogClient clog;

  int whoami;
  std::string dev_path, journal_path;

  class C_Tick : public Context {
    OSD *osd;
  public:
    C_Tick(OSD *o) : osd(o) {}
    void finish(int r) {
      osd->tick();
    }
  };

  Cond dispatch_cond;
  int dispatch_running;

  void create_logger();
  void create_recoverystate_perf();
  void tick();
  void _dispatch(Message *m);
  void dispatch_op(OpRequestRef op);

  void check_osdmap_features(ObjectStore *store);

  // asok
  friend class OSDSocketHook;
  class OSDSocketHook *asok_hook;
  bool asok_command(string command, cmdmap_t& cmdmap, string format, ostream& ss);

public:
  ClassHandler  *class_handler;
  int get_nodeid() { return whoami; }
  
  static hobject_t get_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(foo), 0)); 
  }
  static hobject_t get_inc_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "inc_osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(foo), 0)); 
  }

  static hobject_t make_snapmapper_oid() {
    return hobject_t(
      sobject_t(
	object_t("snapmapper"),
	0));
  }

  static hobject_t make_pg_log_oid(spg_t pg) {
    stringstream ss;
    ss << "pglog_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }
  
  static hobject_t make_pg_biginfo_oid(spg_t pg) {
    stringstream ss;
    ss << "pginfo_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }
  static hobject_t make_infos_oid() {
    hobject_t oid(sobject_t("infos", CEPH_NOSNAP));
    return oid;
  }
  static void recursive_remove_collection(ObjectStore *store, coll_t tmp);

  /**
   * get_osd_initial_compat_set()
   *
   * Get the initial feature set for this OSD.  Features
   * here are automatically upgraded.
   *
   * Return value: Initial osd CompatSet
   */
  static CompatSet get_osd_initial_compat_set();

  /**
   * get_osd_compat_set()
   *
   * Get all features supported by this OSD
   *
   * Return value: CompatSet of all supported features
   */
  static CompatSet get_osd_compat_set();
  

private:
  // -- superblock --
  OSDSuperblock superblock;

  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();

  CompatSet osd_compat;

  // -- state --
public:
  static const int STATE_INITIALIZING = 1;
  static const int STATE_BOOTING = 2;
  static const int STATE_ACTIVE = 3;
  static const int STATE_STOPPING = 4;
  static const int STATE_WAITING_FOR_HEALTHY = 5;

  static const char *get_state_name(int s) {
    switch (s) {
    case STATE_INITIALIZING: return "initializing";
    case STATE_BOOTING: return "booting";
    case STATE_ACTIVE: return "active";
    case STATE_STOPPING: return "stopping";
    case STATE_WAITING_FOR_HEALTHY: return "waiting_for_healthy";
    default: return "???";
    }
  }

private:
  int state;
  epoch_t boot_epoch;  // _first_ epoch we were marked up (after this process started)
  epoch_t up_epoch;    // _most_recent_ epoch we were marked up
  epoch_t bind_epoch;  // epoch we last did a bind to new ip:ports

public:
  bool is_initializing() { return state == STATE_INITIALIZING; }
  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }
  bool is_waiting_for_healthy() { return state == STATE_WAITING_FOR_HEALTHY; }

private:

  ThreadPool op_tp;
  ThreadPool recovery_tp;
  ThreadPool disk_tp;
  ThreadPool command_tp;

  bool paused_recovery;

  // -- sessions --
public:
  struct Session : public RefCountedObject {
    EntityName entity_name;
    OSDCap caps;
    int64_t auid;
    epoch_t last_sent_epoch;
    ConnectionRef con;
    WatchConState wstate;

    Session() : auid(-1), last_sent_epoch(0), con(0) {}
  };

private:
  /**
   *  @defgroup monc helpers
   *
   *  Right now we only have the one
   */

  /**
   * Ask the Monitors for a sequence of OSDMaps.
   *
   * @param epoch The epoch to start with when replying
   * @param force_request True if this request forces a new subscription to
   * the monitors; false if an outstanding request that encompasses it is
   * sufficient.
   */
  void osdmap_subscribe(version_t epoch, bool force_request);
  /** @} monc helpers */

  // -- heartbeat --
  /// information about a heartbeat peer
  struct HeartbeatInfo {
    int peer;           ///< peer
    ConnectionRef con_front;   ///< peer connection (front)
    ConnectionRef con_back;    ///< peer connection (back)
    utime_t first_tx;   ///< time we sent our first ping request
    utime_t last_tx;    ///< last time we sent a ping request
    utime_t last_rx_front;  ///< last time we got a ping reply on the front side
    utime_t last_rx_back;   ///< last time we got a ping reply on the back side
    epoch_t epoch;      ///< most recent epoch we wanted this peer

    bool is_unhealthy(utime_t cutoff) {
      return
	! ((last_rx_front > cutoff ||
	    (last_rx_front == utime_t() && (last_tx == utime_t() ||
					    first_tx > cutoff))) &&
	   (last_rx_back > cutoff ||
	    (last_rx_back == utime_t() && (last_tx == utime_t() ||
					   first_tx > cutoff))));
    }
    bool is_healthy(utime_t cutoff) {
      return last_rx_front > cutoff && last_rx_back > cutoff;
    }

  };
  /// state attached to outgoing heartbeat connections
  struct HeartbeatSession : public RefCountedObject {
    int peer;
    HeartbeatSession(int p) : peer(p) {}
  };
  Mutex heartbeat_lock;
  map<int, int> debug_heartbeat_drops_remaining;
  Cond heartbeat_cond;
  bool heartbeat_stop;
  bool heartbeat_need_update;   ///< true if we need to refresh our heartbeat peers
  epoch_t heartbeat_epoch;      ///< last epoch we updated our heartbeat peers
  map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  utime_t last_mon_heartbeat;
  Messenger *hbclient_messenger;
  Messenger *hb_front_server_messenger;
  Messenger *hb_back_server_messenger;
  utime_t last_heartbeat_resample;   ///< last time we chose random peers in waiting-for-healthy state
  
  void _add_heartbeat_peer(int p);
  void _remove_heartbeat_peer(int p);
  bool heartbeat_reset(Connection *con);
  void maybe_update_heartbeat_peers();
  void reset_heartbeat_peers();
  void heartbeat();
  void heartbeat_check();
  void heartbeat_entry();
  void need_heartbeat_peer_update();

  void heartbeat_kick() {
    Mutex::Locker l(heartbeat_lock);
    heartbeat_cond.Signal();
  }

  struct T_Heartbeat : public Thread {
    OSD *osd;
    T_Heartbeat(OSD *o) : osd(o) {}
    void *entry() {
      osd->heartbeat_entry();
      return 0;
    }
  } heartbeat_thread;

public:
  bool heartbeat_dispatch(Message *m);

  struct HeartbeatDispatcher : public Dispatcher {
    OSD *osd;
    HeartbeatDispatcher(OSD *o) : Dispatcher(cct), osd(o) {}
    bool ms_dispatch(Message *m) {
      return osd->heartbeat_dispatch(m);
    };
    bool ms_handle_reset(Connection *con) {
      return osd->heartbeat_reset(con);
    }
    void ms_handle_remote_reset(Connection *con) {}
    bool ms_verify_authorizer(Connection *con, int peer_type,
			      int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			      bool& isvalid, CryptoKey& session_key) {
      isvalid = true;
      return true;
    }
  } heartbeat_dispatcher;

private:
  // -- stats --
  Mutex stat_lock;
  osd_stat_t osd_stat;

  void update_osd_stat();
  
  // -- waiters --
  list<OpRequestRef> finished;
  Mutex finished_lock;
  
  void take_waiters(list<OpRequestRef>& ls) {
    finished_lock.Lock();
    finished.splice(finished.end(), ls);
    finished_lock.Unlock();
  }
  void take_waiters_front(list<OpRequestRef>& ls) {
    finished_lock.Lock();
    finished.splice(finished.begin(), ls);
    finished_lock.Unlock();
  }
  void take_waiter(OpRequestRef op) {
    finished_lock.Lock();
    finished.push_back(op);
    finished_lock.Unlock();
  }
  void do_waiters();
  
  // -- op tracking --
  OpTracker op_tracker;
  void check_ops_in_flight();
  void test_ops(std::string command, std::string args, ostream& ss);
  friend class TestOpsSocketHook;
  TestOpsSocketHook *test_ops_hook;
  friend struct C_CompleteSplits;

  // -- op queue --

  struct OpWQ: public ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>,
					       PGRef > {
    Mutex qlock;
    map<PG*, list<OpRequestRef> > pg_for_processing;
    OSD *osd;
    PrioritizedQueue<pair<PGRef, OpRequestRef>, entity_inst_t > pqueue;
    OpWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>, PGRef >(
	"OSD::OpWQ", ti, ti*10, tp),
	qlock("OpWQ::qlock"),
	osd(o),
	pqueue(o->cct->_conf->osd_op_pq_max_tokens_per_priority,
	       o->cct->_conf->osd_op_pq_min_cost)
    {}

    void dump(Formatter *f) {
      lock();
      pqueue.dump(f);
      unlock();
    }

    void _enqueue_front(pair<PGRef, OpRequestRef> item);
    void _enqueue(pair<PGRef, OpRequestRef> item);
    PGRef _dequeue();

    struct Pred {
      PG *pg;
      Pred(PG *pg) : pg(pg) {}
      bool operator()(const pair<PGRef, OpRequestRef> &op) {
	return op.first == pg;
      }
    };
    void dequeue(PG *pg, list<OpRequestRef> *dequeued = 0) {
      lock();
      if (!dequeued) {
	pqueue.remove_by_filter(Pred(pg));
	pg_for_processing.erase(pg);
      } else {
	list<pair<PGRef, OpRequestRef> > _dequeued;
	pqueue.remove_by_filter(Pred(pg), &_dequeued);
	for (list<pair<PGRef, OpRequestRef> >::iterator i = _dequeued.begin();
	     i != _dequeued.end();
	     ++i) {
	  dequeued->push_back(i->second);
	}
	if (pg_for_processing.count(pg)) {
	  dequeued->splice(
	    dequeued->begin(),
	    pg_for_processing[pg]);
	  pg_for_processing.erase(pg);
	}
      }
      unlock();
    }
    bool _empty() {
      return pqueue.empty();
    }
    void _process(PGRef pg, ThreadPool::TPHandle &handle);
  } op_wq;

  void enqueue_op(PG *pg, OpRequestRef op);
  void dequeue_op(
    PGRef pg, OpRequestRef op,
    ThreadPool::TPHandle &handle);

  // -- peering queue --
  struct PeeringWQ : public ThreadPool::BatchWorkQueue<PG> {
    list<PG*> peering_queue;
    OSD *osd;
    set<PG*> in_use;
    PeeringWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::BatchWorkQueue<PG>(
	"OSD::PeeringWQ", ti, ti*10, tp), osd(o) {}

    void _dequeue(PG *pg) {
      for (list<PG*>::iterator i = peering_queue.begin();
	   i != peering_queue.end();
	   ) {
	if (*i == pg) {
	  peering_queue.erase(i++);
	  pg->put("PeeringWQ");
	} else {
	  ++i;
	}
      }
    }
    bool _enqueue(PG *pg) {
      pg->get("PeeringWQ");
      peering_queue.push_back(pg);
      return true;
    }
    bool _empty() {
      return peering_queue.empty();
    }
    void _dequeue(list<PG*> *out);
    void _process(
      const list<PG *> &pgs,
      ThreadPool::TPHandle &handle) {
      osd->process_peering_events(pgs, handle);
      for (list<PG *>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	(*i)->put("PeeringWQ");
      }
    }
    void _process_finish(const list<PG *> &pgs) {
      for (list<PG*>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	in_use.erase(*i);
      }
    }
    void _clear() {
      assert(peering_queue.empty());
    }
  } peering_wq;

  void process_peering_events(
    const list<PG*> &pg,
    ThreadPool::TPHandle &handle);

  friend class PG;
  friend class ReplicatedPG;


 protected:

  // -- osd map --
  OSDMapRef       osdmap;
  OSDMapRef get_osdmap() {
    return osdmap;
  }
  utime_t         had_map_since;
  RWLock          map_lock;
  list<OpRequestRef>  waiting_for_osdmap;

  Mutex peer_map_epoch_lock;
  map<int, epoch_t> peer_map_epoch;
  
  epoch_t get_peer_epoch(int p);
  epoch_t note_peer_epoch(int p, epoch_t e);
  void forget_peer_epoch(int p, epoch_t e);

  bool _share_map_incoming(entity_name_t name, Connection *con, epoch_t epoch,
			   Session *session = 0);
  void _share_map_outgoing(int peer, Connection *con,
			   OSDMapRef map = OSDMapRef());

  void wait_for_new_map(OpRequestRef op);
  void handle_osd_map(class MOSDMap *m);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  
  void advance_pg(
    epoch_t advance_to, PG *pg,
    ThreadPool::TPHandle &handle,
    PG::RecoveryCtx *rctx,
    set<boost::intrusive_ptr<PG> > *split_pgs
  );
  void advance_map(ObjectStore::Transaction& t, C_Contexts *tfin);
  void consume_map();
  void activate_map();

  // osd map cache (past osd maps)
  OSDMapRef get_map(epoch_t e) {
    return service.get_map(e);
  }
  OSDMapRef add_map(OSDMap *o) {
    return service.add_map(o);
  }
  void add_map_bl(epoch_t e, bufferlist& bl) {
    return service.add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl) {
    return service.pin_map_bl(e, bl);
  }
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    return service.get_map_bl(e, bl);
  }
  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    return service.add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl) {
    return service.pin_map_inc_bl(e, bl);
  }
  bool get_inc_map_bl(epoch_t e, bufferlist& bl) {
    return service.get_inc_map_bl(e, bl);
  }

  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to);
  void send_incremental_map(epoch_t since, Connection *con);
  void send_map(MOSDMap *m, Connection *con);

protected:
  // -- placement groups --
  ceph::unordered_map<spg_t, PG*> pg_map;
  map<spg_t, list<OpRequestRef> > waiting_for_pg;
  map<spg_t, list<PG::CephPeeringEvtRef> > peering_wait_for_split;
  PGRecoveryStats pg_recovery_stats;

  PGPool _get_pool(int id, OSDMapRef createmap);

  bool  _have_pg(spg_t pgid);
  PG   *_lookup_lock_pg_with_map_lock_held(spg_t pgid);
  PG   *_lookup_lock_pg(spg_t pgid);
  PG   *_lookup_pg(spg_t pgid);
  PG   *_open_lock_pg(OSDMapRef createmap,
		      spg_t pg, bool no_lockdep_check=false,
		      bool hold_map_lock=false);
  enum res_result {
    RES_PARENT,    // resurrected a parent
    RES_SELF,      // resurrected self
    RES_NONE       // nothing relevant deleting
  };
  res_result _try_resurrect_pg(
    OSDMapRef curmap, spg_t pgid, spg_t *resurrected, PGRef *old_pg_state);
  PG   *_create_lock_pg(
    OSDMapRef createmap,
    spg_t pgid,
    bool newly_created,
    bool hold_map_lock,
    bool backfill,
    int role,
    vector<int>& up, int up_primary,
    vector<int>& acting, int acting_primary,
    pg_history_t history,
    pg_interval_map_t& pi,
    ObjectStore::Transaction& t);
  PG   *_lookup_qlock_pg(spg_t pgid);

  PG* _make_pg(OSDMapRef createmap, spg_t pgid);
  void add_newly_split_pg(PG *pg,
			  PG::RecoveryCtx *rctx);

  void handle_pg_peering_evt(
    spg_t pgid,
    const pg_info_t& info,
    pg_interval_map_t& pi,
    epoch_t epoch,
    pg_shard_t from,
    bool primary,
    PG::CephPeeringEvtRef evt);
  
  void load_pgs();
  void build_past_intervals_parallel();

  void calc_priors_during(
    spg_t pgid, epoch_t start, epoch_t end, set<pg_shard_t>& pset);

  /// project pg history from from to now
  bool project_pg_history(
    spg_t pgid, pg_history_t& h, epoch_t from,
    const vector<int>& lastup,
    int lastupprimary,
    const vector<int>& lastacting,
    int lastactingprimary
    ); ///< @return false if there was a map gap between from and now

  void wake_pg_waiters(spg_t pgid) {
    if (waiting_for_pg.count(pgid)) {
      take_waiters_front(waiting_for_pg[pgid]);
      waiting_for_pg.erase(pgid);
    }
  }
  void wake_all_pg_waiters() {
    for (map<spg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
	 p != waiting_for_pg.end();
	 ++p)
      take_waiters_front(p->second);
    waiting_for_pg.clear();
  }


  // -- pg creation --
  struct create_pg_info {
    pg_history_t history;
    vector<int> acting;
    set<pg_shard_t> prior;
    pg_t parent;
  };
  ceph::unordered_map<spg_t, create_pg_info> creating_pgs;
  double debug_drop_pg_create_probability;
  int debug_drop_pg_create_duration;
  int debug_drop_pg_create_left;  // 0 if we just dropped the last one, -1 if we can drop more

  bool can_create_pg(spg_t pgid);
  void handle_pg_create(OpRequestRef op);

  void split_pgs(
    PG *parent,
    const set<spg_t> &childpgids, set<boost::intrusive_ptr<PG> > *out_pgs,
    OSDMapRef curmap,
    OSDMapRef nextmap,
    PG::RecoveryCtx *rctx);

  // == monitor interaction ==
  utime_t last_mon_report;
  utime_t last_pg_stats_sent;

  /* if our monitor dies, we want to notice it and reconnect.
   *  So we keep track of when it last acked our stat updates,
   *  and if too much time passes (and we've been sending
   *  more updates) then we can call it dead and reconnect
   *  elsewhere.
   */
  utime_t last_pg_stats_ack;
  bool outstanding_pg_stats; // some stat updates haven't been acked yet
  bool timeout_mon_on_pg_stats;
  void restart_stats_timer() {
    Mutex::Locker l(osd_lock);
    last_pg_stats_ack = ceph_clock_now(cct);
    timeout_mon_on_pg_stats = true;
  }

  class C_MonStatsAckTimer : public Context {
    OSD *osd;
  public:
    C_MonStatsAckTimer(OSD *o) : osd(o) {}
    void finish(int r) {
      osd->restart_stats_timer();
    }
  };
  friend class C_MonStatsAckTimer;

  void do_mon_report();

  // -- boot --
  void start_boot();
  void _maybe_boot(epoch_t oldest, epoch_t newest);
  void _send_boot();
  void _collect_metadata(map<string,string> *pmeta);

  void start_waiting_for_healthy();
  bool _is_healthy();
  
  friend struct C_OSD_GetVersion;

  // -- alive --
  epoch_t up_thru_wanted;
  epoch_t up_thru_pending;

  void queue_want_up_thru(epoch_t want);
  void send_alive();

  // -- failures --
  map<int,utime_t> failure_queue;
  map<int,entity_inst_t> failure_pending;


  void send_failures();
  void send_still_alive(epoch_t epoch, const entity_inst_t &i);

  // -- pg stats --
  Mutex pg_stat_queue_lock;
  Cond pg_stat_queue_cond;
  xlist<PG*> pg_stat_queue;
  bool osd_stat_updated;
  uint64_t pg_stat_tid, pg_stat_tid_flushed;

  void send_pg_stats(const utime_t &now);
  void handle_pg_stats_ack(class MPGStatsAck *ack);
  void flush_pg_stats();

  void pg_stat_queue_enqueue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->is_primary() && !pg->stat_queue_item.is_on_list()) {
      pg->get("pg_stat_queue");
      pg_stat_queue.push_back(&pg->stat_queue_item);
    }
    osd_stat_updated = true;
    pg_stat_queue_lock.Unlock();
  }
  void pg_stat_queue_dequeue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->stat_queue_item.remove_myself())
      pg->put("pg_stat_queue");
    pg_stat_queue_lock.Unlock();
  }
  void clear_pg_stat_queue() {
    pg_stat_queue_lock.Lock();
    while (!pg_stat_queue.empty()) {
      PG *pg = pg_stat_queue.front();
      pg_stat_queue.pop_front();
      pg->put("pg_stat_queue");
    }
    pg_stat_queue_lock.Unlock();
  }

  ceph_tid_t get_tid() {
    return service.get_tid();
  }

  // -- generic pg peering --
  PG::RecoveryCtx create_context();
  bool compat_must_dispatch_immediately(PG *pg);
  void dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap,
                        ThreadPool::TPHandle *handle = NULL);
  void dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg,
                                    ThreadPool::TPHandle *handle = NULL);
  void do_notifies(map<int,
		       vector<pair<pg_notify_t, pg_interval_map_t> > >&
		       notify_list,
		   OSDMapRef map);
  void do_queries(map<int, map<spg_t,pg_query_t> >& query_map,
		  OSDMapRef map);
  void do_infos(map<int,
		    vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		OSDMapRef map);
  void repeer(PG *pg, map< int, map<spg_t,pg_query_t> >& query_map);

  bool require_mon_peer(Message *m);
  bool require_osd_peer(OpRequestRef op);

  bool require_same_or_newer_map(OpRequestRef op, epoch_t e);

  void handle_pg_query(OpRequestRef op);
  void handle_pg_notify(OpRequestRef op);
  void handle_pg_log(OpRequestRef op);
  void handle_pg_info(OpRequestRef op);
  void handle_pg_trim(OpRequestRef op);

  void handle_pg_scan(OpRequestRef op);

  void handle_pg_backfill(OpRequestRef op);
  void handle_pg_backfill_reserve(OpRequestRef op);
  void handle_pg_recovery_reserve(OpRequestRef op);

  void handle_pg_remove(OpRequestRef op);
  void _remove_pg(PG *pg);

  // -- commands --
  struct Command {
    vector<string> cmd;
    ceph_tid_t tid;
    bufferlist indata;
    ConnectionRef con;

    Command(vector<string>& c, ceph_tid_t t, bufferlist& bl, Connection *co)
      : cmd(c), tid(t), indata(bl), con(co) {}
  };
  list<Command*> command_queue;
  struct CommandWQ : public ThreadPool::WorkQueue<Command> {
    OSD *osd;
    CommandWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<Command>("OSD::CommandWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->command_queue.empty();
    }
    bool _enqueue(Command *c) {
      osd->command_queue.push_back(c);
      return true;
    }
    void _dequeue(Command *pg) {
      assert(0);
    }
    Command *_dequeue() {
      if (osd->command_queue.empty())
	return NULL;
      Command *c = osd->command_queue.front();
      osd->command_queue.pop_front();
      return c;
    }
    void _process(Command *c) {
      osd->osd_lock.Lock();
      if (osd->is_stopping()) {
	osd->osd_lock.Unlock();
	delete c;
	return;
      }
      osd->do_command(c->con.get(), c->tid, c->cmd, c->indata);
      osd->osd_lock.Unlock();
      delete c;
    }
    void _clear() {
      while (!osd->command_queue.empty()) {
	Command *c = osd->command_queue.front();
	osd->command_queue.pop_front();
	delete c;
      }
    }
  } command_wq;

  void handle_command(class MMonCommand *m);
  void handle_command(class MCommand *m);
  void do_command(Connection *con, ceph_tid_t tid, vector<string>& cmd, bufferlist& data);

  // -- pg recovery --
  xlist<PG*> recovery_queue;
  utime_t defer_recovery_until;
  int recovery_ops_active;
#ifdef DEBUG_RECOVERY_OIDS
  map<spg_t, set<hobject_t> > recovery_oids;
#endif

  struct RecoveryWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    RecoveryWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::RecoveryWQ", ti, ti*10, tp), osd(o) {}

    bool _empty() {
      return osd->recovery_queue.empty();
    }
    bool _enqueue(PG *pg);
    void _dequeue(PG *pg) {
      if (pg->recovery_item.remove_myself())
	pg->put("RecoveryWQ");
    }
    PG *_dequeue() {
      if (osd->recovery_queue.empty())
	return NULL;
      
      if (!osd->_recover_now())
	return NULL;

      PG *pg = osd->recovery_queue.front();
      osd->recovery_queue.pop_front();
      return pg;
    }
    void _queue_front(PG *pg) {
      if (!pg->recovery_item.is_on_list()) {
	pg->get("RecoveryWQ");
	osd->recovery_queue.push_front(&pg->recovery_item);
      }
    }
    void _process(PG *pg, ThreadPool::TPHandle &handle) {
      osd->do_recovery(pg, handle);
      pg->put("RecoveryWQ");
    }
    void _clear() {
      while (!osd->recovery_queue.empty()) {
	PG *pg = osd->recovery_queue.front();
	osd->recovery_queue.pop_front();
	pg->put("RecoveryWQ");
      }
    }
  } recovery_wq;

  void start_recovery_op(PG *pg, const hobject_t& soid);
  void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue);
  void do_recovery(PG *pg, ThreadPool::TPHandle &handle);
  bool _recover_now();

  // replay / delayed pg activation
  Mutex replay_queue_lock;
  list< pair<spg_t, utime_t > > replay_queue;
  
  void check_replay_queue();


  // -- snap trimming --
  xlist<PG*> snap_trim_queue;
  
  struct SnapTrimWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    SnapTrimWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::SnapTrimWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->snap_trim_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->snap_trim_item.is_on_list())
	return false;
      pg->get("SnapTrimWQ");
      osd->snap_trim_queue.push_back(&pg->snap_trim_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->snap_trim_item.remove_myself())
	pg->put("SnapTrimWQ");
    }
    PG *_dequeue() {
      if (osd->snap_trim_queue.empty())
	return NULL;
      PG *pg = osd->snap_trim_queue.front();
      osd->snap_trim_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->snap_trimmer();
      pg->put("SnapTrimWQ");
    }
    void _clear() {
      osd->snap_trim_queue.clear();
    }
  } snap_trim_wq;


  // -- scrubbing --
  void sched_scrub();
  bool scrub_random_backoff();
  bool scrub_should_schedule();

  xlist<PG*> scrub_queue;

  struct ScrubWQ : public ThreadPool::WorkQueue<PG> {
    OSD *osd;
    ScrubWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::ScrubWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->scrub_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->scrub_item.is_on_list()) {
	return false;
      }
      pg->get("ScrubWQ");
      osd->scrub_queue.push_back(&pg->scrub_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_item.remove_myself()) {
	pg->put("ScrubWQ");
      }
    }
    PG *_dequeue() {
      if (osd->scrub_queue.empty())
	return NULL;
      PG *pg = osd->scrub_queue.front();
      osd->scrub_queue.pop_front();
      return pg;
    }
    void _process(
      PG *pg,
      ThreadPool::TPHandle &handle) {
      pg->scrub(handle);
      pg->put("ScrubWQ");
    }
    void _clear() {
      while (!osd->scrub_queue.empty()) {
	PG *pg = osd->scrub_queue.front();
	osd->scrub_queue.pop_front();
	pg->put("ScrubWQ");
      }
    }
  } scrub_wq;

  struct ScrubFinalizeWQ : public ThreadPool::WorkQueue<PG> {
  private:
    xlist<PG*> scrub_finalize_queue;

  public:
    ScrubFinalizeWQ(time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::ScrubFinalizeWQ", ti, ti*10, tp) {}

    bool _empty() {
      return scrub_finalize_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->scrub_finalize_item.is_on_list()) {
	return false;
      }
      pg->get("ScrubFinalizeWQ");
      scrub_finalize_queue.push_back(&pg->scrub_finalize_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_finalize_item.remove_myself()) {
	pg->put("ScrubFinalizeWQ");
      }
    }
    PG *_dequeue() {
      if (scrub_finalize_queue.empty())
	return NULL;
      PG *pg = scrub_finalize_queue.front();
      scrub_finalize_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->scrub_finalize();
      pg->put("ScrubFinalizeWQ");
    }
    void _clear() {
      while (!scrub_finalize_queue.empty()) {
	PG *pg = scrub_finalize_queue.front();
	scrub_finalize_queue.pop_front();
	pg->put("ScrubFinalizeWQ");
      }
    }
  } scrub_finalize_wq;

  struct RepScrubWQ : public ThreadPool::WorkQueue<MOSDRepScrub> {
  private: 
    OSD *osd;
    list<MOSDRepScrub*> rep_scrub_queue;

  public:
    RepScrubWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<MOSDRepScrub>("OSD::RepScrubWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return rep_scrub_queue.empty();
    }
    bool _enqueue(MOSDRepScrub *msg) {
      rep_scrub_queue.push_back(msg);
      return true;
    }
    void _dequeue(MOSDRepScrub *msg) {
      assert(0); // Not applicable for this wq
      return;
    }
    MOSDRepScrub *_dequeue() {
      if (rep_scrub_queue.empty())
	return NULL;
      MOSDRepScrub *msg = rep_scrub_queue.front();
      rep_scrub_queue.pop_front();
      return msg;
    }
    void _process(
      MOSDRepScrub *msg,
      ThreadPool::TPHandle &handle) {
      osd->osd_lock.Lock();
      if (osd->is_stopping()) {
	osd->osd_lock.Unlock();
	return;
      }
      if (osd->_have_pg(msg->pgid)) {
	PG *pg = osd->_lookup_lock_pg(msg->pgid);
	osd->osd_lock.Unlock();
	pg->replica_scrub(msg, handle);
	msg->put();
	pg->unlock();
      } else {
	msg->put();
	osd->osd_lock.Unlock();
      }
    }
    void _clear() {
      while (!rep_scrub_queue.empty()) {
	MOSDRepScrub *msg = rep_scrub_queue.front();
	rep_scrub_queue.pop_front();
	msg->put();
      }
    }
  } rep_scrub_wq;

  // -- removing --
  struct RemoveWQ :
    public ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> > {
    ObjectStore *&store;
    list<pair<PGRef, DeletingStateRef> > remove_queue;
    RemoveWQ(ObjectStore *&o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> >(
	"OSD::RemoveWQ", ti, 0, tp),
	store(o) {}

    bool _empty() {
      return remove_queue.empty();
    }
    void _enqueue(pair<PGRef, DeletingStateRef> item) {
      remove_queue.push_back(item);
    }
    void _enqueue_front(pair<PGRef, DeletingStateRef> item) {
      remove_queue.push_front(item);
    }
    bool _dequeue(pair<PGRef, DeletingStateRef> item) {
      assert(0);
    }
    pair<PGRef, DeletingStateRef> _dequeue() {
      assert(!remove_queue.empty());
      pair<PGRef, DeletingStateRef> item = remove_queue.front();
      remove_queue.pop_front();
      return item;
    }
    void _process(pair<PGRef, DeletingStateRef>, ThreadPool::TPHandle &);
    void _clear() {
      remove_queue.clear();
    }
  } remove_wq;
  uint64_t next_removal_seq;
  coll_t get_next_removal_coll(spg_t pgid) {
    return coll_t::make_removal_coll(next_removal_seq++, pgid);
  }

 private:
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(CephContext *cct_,
      ObjectStore *store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev);
  ~OSD();

  // static bits
  static int find_osd_dev(char *result, int whoami);
  static int do_convertfs(ObjectStore *store);
  static int convert_collection(ObjectStore *store, coll_t cid);
  static int mkfs(CephContext *cct, ObjectStore *store,
		  const string& dev,
		  uuid_d fsid, int whoami);
  /* remove any non-user xattrs from a map of them */
  void filter_xattrs(map<string, bufferptr>& attrs) {
    for (map<string, bufferptr>::iterator iter = attrs.begin();
	 iter != attrs.end();
	 ) {
      if (('_' != iter->first.at(0)) || (iter->first.size() == 1))
	attrs.erase(iter++);
      else ++iter;
    }
  }

private:
  static int write_meta(ObjectStore *store,
			uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami);
public:
  static int peek_meta(ObjectStore *store, string& magic,
		       uuid_d& cluster_fsid, uuid_d& osd_fsid, int& whoami);
  

  // startup/shutdown
  int pre_init();
  int init();
  void final_init();

  void suicide(int exitcode);
  int shutdown();

  void handle_signal(int signum);

  void handle_rep_scrub(MOSDRepScrub *m);
  void handle_scrub(struct MOSDScrub *m);
  void handle_osd_ping(class MOSDPing *m);
  void handle_op(OpRequestRef op);

  template <typename T, int MSGTYPE>
  void handle_replica_op(OpRequestRef op);

  /// check if we can throw out op from a disconnected client
  static bool op_is_discardable(class MOSDOp *m);
  /// check if op should be (re)queued for processing
public:
  void force_remount();

  int init_op_flags(OpRequestRef op);

  OSDService service;
  friend class OSDService;
};

//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif
