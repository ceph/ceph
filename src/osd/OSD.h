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

#include "PG.h"

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/AsyncReserver.h"
#include "common/ceph_context.h"
#include "common/zipkin_trace.h"

#include "mgr/MgrClient.h"

#include "os/ObjectStore.h"
#include "OSDCap.h" 
 
#include "auth/KeyRing.h"
#include "osd/ClassHandler.h"

#include "include/CompatSet.h"

#include "OpRequest.h"
#include "Session.h"

#include "osd/PGQueueable.h"

#include <atomic>
#include <map>
#include <memory>
#include "include/memory.h"
using namespace std;

#include "include/unordered_map.h"

#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/sharedptr_registry.hpp"
#include "common/WeightedPriorityQueue.h"
#include "common/PrioritizedQueue.h"
#include "osd/mClockOpClassQueue.h"
#include "osd/mClockClientQueue.h"
#include "messages/MOSDOp.h"
#include "include/Spinlock.h"
#include "common/EventTrace.h"

#define CEPH_OSD_PROTOCOL    10 /* cluster internal */


enum {
  l_osd_first = 10000,
  l_osd_op_wip,
  l_osd_op,
  l_osd_op_inb,
  l_osd_op_outb,
  l_osd_op_lat,
  l_osd_op_process_lat,
  l_osd_op_prepare_lat,
  l_osd_op_r,
  l_osd_op_r_outb,
  l_osd_op_r_lat,
  l_osd_op_r_lat_outb_hist,
  l_osd_op_r_process_lat,
  l_osd_op_r_prepare_lat,
  l_osd_op_w,
  l_osd_op_w_inb,
  l_osd_op_w_lat,
  l_osd_op_w_lat_inb_hist,
  l_osd_op_w_process_lat,
  l_osd_op_w_prepare_lat,
  l_osd_op_rw,
  l_osd_op_rw_inb,
  l_osd_op_rw_outb,
  l_osd_op_rw_lat,
  l_osd_op_rw_lat_inb_hist,
  l_osd_op_rw_lat_outb_hist,
  l_osd_op_rw_process_lat,
  l_osd_op_rw_prepare_lat,

  l_osd_op_before_queue_op_lat,
  l_osd_op_before_dequeue_op_lat,

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

  l_osd_rop,

  l_osd_loadavg,
  l_osd_buf,
  l_osd_history_alloc_bytes,
  l_osd_history_alloc_num,
  l_osd_cached_crc,
  l_osd_cached_crc_adjusted,
  l_osd_missed_crc,

  l_osd_pg,
  l_osd_pg_primary,
  l_osd_pg_replica,
  l_osd_pg_stray,
  l_osd_pg_removing,
  l_osd_hb_to,
  l_osd_map,
  l_osd_mape,
  l_osd_mape_dup,

  l_osd_waiting_for_map,

  l_osd_map_cache_hit,
  l_osd_map_cache_miss,
  l_osd_map_cache_miss_low,
  l_osd_map_cache_miss_low_avg,
  l_osd_map_bl_cache_hit,
  l_osd_map_bl_cache_miss,

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
  l_osd_tier_delay,
  l_osd_tier_proxy_read,
  l_osd_tier_proxy_write,

  l_osd_agent_wake,
  l_osd_agent_skip,
  l_osd_agent_flush,
  l_osd_agent_evict,

  l_osd_object_ctx_cache_hit,
  l_osd_object_ctx_cache_total,

  l_osd_op_cache_hit,
  l_osd_tier_flush_lat,
  l_osd_tier_promote_lat,
  l_osd_tier_r_lat,

  l_osd_pg_info,
  l_osd_pg_fastinfo,
  l_osd_pg_biginfo,

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
  rs_reprecovering_latency,
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
  rs_down_latency,
  rs_getmissing_latency,
  rs_waitupthru_latency,
  rs_notrecovering_latency,
  rs_last,
};

class Messenger;
class Message;
class MonClient;
class PerfCounters;
class ObjectStore;
class FuseStore;
class OSDMap;
class MLog;
class Objecter;

class Watch;
class PrimaryLogPG;

class AuthAuthorizeHandlerRegistry;

class TestOpsSocketHook;
struct C_CompleteSplits;
struct C_OpenPGs;
class LogChannel;
class CephContext;
typedef ceph::shared_ptr<ObjectStore::Sequencer> SequencerRef;
class MOSDOp;

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
  explicit DeletingState(const pair<spg_t, PGRef> &in) :
    lock("DeletingState::lock"), status(QUEUED), stop_deleting(false),
    pgid(in.first), old_pg_state(in.second) {
    }

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

  /// start or resume the clearing - transition the status to CLEARING_DIR
  bool start_or_resume_clearing() {
    Mutex::Locker l(lock);
    assert(
      status == QUEUED ||
      status == DELETED_DIR ||
      status == CLEARING_WAITING);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = CLEARING_DIR;
    return true;
  } ///< @return false if we should cancel the deletion

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
     * indicates that the deletion has been fully queued.
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
  ceph::shared_ptr<ObjectStore::Sequencer> meta_osr;
  SharedPtrRegistry<spg_t, DeletingState> deleting_pgs;
  const int whoami;
  ObjectStore *&store;
  LogClient &log_client;
  LogChannelRef clog;
  PGRecoveryStats &pg_recovery_stats;
private:
  Messenger *&cluster_messenger;
  Messenger *&client_messenger;
public:
  PerfCounters *&logger;
  PerfCounters *&recoverystate_perf;
  MonClient   *&monc;
  ThreadPool::BatchWorkQueue<PG> &peering_wq;
  GenContextWQ recovery_gen_wq;
  ClassHandler  *&class_handler;

  void enqueue_back(spg_t pgid, PGQueueable qi);
  void enqueue_front(spg_t pgid, PGQueueable qi);

  void maybe_inject_dispatch_delay() {
    if (g_conf->osd_debug_inject_dispatch_delay_probability > 0) {
      if (rand() % 10000 <
	  g_conf->osd_debug_inject_dispatch_delay_probability * 10000) {
	utime_t t;
	t.set_from_double(g_conf->osd_debug_inject_dispatch_delay_duration);
	t.sleep();
      }
    }
  }

private:
  // -- map epoch lower bound --
  Mutex pg_epoch_lock;
  multiset<epoch_t> pg_epochs;
  map<spg_t,epoch_t> pg_epoch;

public:
  void pg_add_epoch(spg_t pgid, epoch_t epoch) {
    Mutex::Locker l(pg_epoch_lock);
    map<spg_t,epoch_t>::iterator t = pg_epoch.find(pgid);
    assert(t == pg_epoch.end());
    pg_epoch[pgid] = epoch;
    pg_epochs.insert(epoch);
  }
  void pg_update_epoch(spg_t pgid, epoch_t epoch) {
    Mutex::Locker l(pg_epoch_lock);
    map<spg_t,epoch_t>::iterator t = pg_epoch.find(pgid);
    assert(t != pg_epoch.end());
    pg_epochs.erase(pg_epochs.find(t->second));
    t->second = epoch;
    pg_epochs.insert(epoch);
  }
  void pg_remove_epoch(spg_t pgid) {
    Mutex::Locker l(pg_epoch_lock);
    map<spg_t,epoch_t>::iterator t = pg_epoch.find(pgid);
    if (t != pg_epoch.end()) {
      pg_epochs.erase(pg_epochs.find(t->second));
      pg_epoch.erase(t);
    }
  }
  epoch_t get_min_pg_epoch() {
    Mutex::Locker l(pg_epoch_lock);
    if (pg_epochs.empty())
      return 0;
    else
      return *pg_epochs.begin();
  }

private:
  // -- superblock --
  Mutex publish_lock, pre_publish_lock; // pre-publish orders before publish
  OSDSuperblock superblock;

public:
  OSDSuperblock get_superblock() {
    Mutex::Locker l(publish_lock);
    return superblock;
  }
  void publish_superblock(const OSDSuperblock &block) {
    Mutex::Locker l(publish_lock);
    superblock = block;
  }

  int get_nodeid() const { return whoami; }

  std::atomic<epoch_t> max_oldest_map;
private:
  OSDMapRef osdmap;

public:
  OSDMapRef get_osdmap() {
    Mutex::Locker l(publish_lock);
    return osdmap;
  }
  epoch_t get_osdmap_epoch() {
    Mutex::Locker l(publish_lock);
    return osdmap ? osdmap->get_epoch() : 0;
  }
  void publish_map(OSDMapRef map) {
    Mutex::Locker l(publish_lock);
    osdmap = map;
  }

  /*
   * osdmap - current published map
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
private:
  OSDMapRef next_osdmap;
  Cond pre_publish_cond;

public:
  void pre_publish_map(OSDMapRef map) {
    Mutex::Locker l(pre_publish_lock);
    next_osdmap = std::move(map);
  }

  void activate_map();
  /// map epochs reserved below
  map<epoch_t, unsigned> map_reservations;

  /// gets ref to next_osdmap and registers the epoch as reserved
  OSDMapRef get_nextmap_reserved() {
    Mutex::Locker l(pre_publish_lock);
    if (!next_osdmap)
      return OSDMapRef();
    epoch_t e = next_osdmap->get_epoch();
    map<epoch_t, unsigned>::iterator i =
      map_reservations.insert(make_pair(e, 0)).first;
    i->second++;
    return next_osdmap;
  }
  /// releases reservation on map
  void release_map(OSDMapRef osdmap) {
    Mutex::Locker l(pre_publish_lock);
    map<epoch_t, unsigned>::iterator i =
      map_reservations.find(osdmap->get_epoch());
    assert(i != map_reservations.end());
    assert(i->second > 0);
    if (--(i->second) == 0) {
      map_reservations.erase(i);
    }
    pre_publish_cond.Signal();
  }
  /// blocks until there are no reserved maps prior to next_osdmap
  void await_reserved_maps() {
    Mutex::Locker l(pre_publish_lock);
    assert(next_osdmap);
    while (true) {
      map<epoch_t, unsigned>::const_iterator i = map_reservations.cbegin();
      if (i == map_reservations.cend() || i->first >= next_osdmap->get_epoch()) {
	break;
      } else {
	pre_publish_cond.Wait(pre_publish_lock);
      }
    }
  }

private:
  Mutex peer_map_epoch_lock;
  map<int, epoch_t> peer_map_epoch;
public:
  epoch_t get_peer_epoch(int p);
  epoch_t note_peer_epoch(int p, epoch_t e);
  void forget_peer_epoch(int p, epoch_t e);

  void send_map(class MOSDMap *m, Connection *con);
  void send_incremental_map(epoch_t since, Connection *con, OSDMapRef& osdmap);
  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to,
                                       OSDSuperblock& superblock);
  bool should_share_map(entity_name_t name, Connection *con, epoch_t epoch,
                        const OSDMapRef& osdmap, const epoch_t *sent_epoch_p);
  void share_map(entity_name_t name, Connection *con, epoch_t epoch,
                 OSDMapRef& osdmap, epoch_t *sent_epoch_p);
  void share_map_peer(int peer, Connection *con,
                      OSDMapRef map = OSDMapRef());

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch);
  pair<ConnectionRef,ConnectionRef> get_con_osd_hb(int peer, epoch_t from_epoch);  // (back, front)
  void send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch);
  void send_message_osd_cluster(Message *m, Connection *con) {
    con->send_message(m);
  }
  void send_message_osd_cluster(Message *m, const ConnectionRef& con) {
    con->send_message(m);
  }
  void send_message_osd_client(Message *m, Connection *con) {
    con->send_message(m);
  }
  void send_message_osd_client(Message *m, const ConnectionRef& con) {
    con->send_message(m);
  }
  entity_name_t get_cluster_msgr_name() {
    return cluster_messenger->get_myname();
  }

private:
  // -- scrub scheduling --
  Mutex sched_scrub_lock;
  int scrubs_pending;
  int scrubs_active;

public:
  struct ScrubJob {
    CephContext* cct;
    /// pg to be scrubbed
    spg_t pgid;
    /// a time scheduled for scrub. but the scrub could be delayed if system
    /// load is too high or it fails to fall in the scrub hours
    utime_t sched_time;
    /// the hard upper bound of scrub time
    utime_t deadline;
    ScrubJob() : cct(nullptr) {}
    explicit ScrubJob(CephContext* cct, const spg_t& pg,
		      const utime_t& timestamp,
		      double pool_scrub_min_interval = 0,
		      double pool_scrub_max_interval = 0, bool must = true);
    /// order the jobs by sched_time
    bool operator<(const ScrubJob& rhs) const;
  };
  set<ScrubJob> sched_scrub_pg;

  /// @returns the scrub_reg_stamp used for unregister the scrub job
  utime_t reg_pg_scrub(spg_t pgid, utime_t t, double pool_scrub_min_interval,
		       double pool_scrub_max_interval, bool must) {
    ScrubJob scrub(cct, pgid, t, pool_scrub_min_interval, pool_scrub_max_interval,
		   must);
    Mutex::Locker l(sched_scrub_lock);
    sched_scrub_pg.insert(scrub);
    return scrub.sched_time;
  }
  void unreg_pg_scrub(spg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    size_t removed = sched_scrub_pg.erase(ScrubJob(cct, pgid, t));
    assert(removed);
  }
  bool first_scrub_stamp(ScrubJob *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (sched_scrub_pg.empty())
      return false;
    set<ScrubJob>::iterator iter = sched_scrub_pg.begin();
    *out = *iter;
    return true;
  }
  bool next_scrub_stamp(const ScrubJob& next,
			ScrubJob *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (sched_scrub_pg.empty())
      return false;
    set<ScrubJob>::const_iterator iter = sched_scrub_pg.lower_bound(next);
    if (iter == sched_scrub_pg.cend())
      return false;
    ++iter;
    if (iter == sched_scrub_pg.cend())
      return false;
    *out = *iter;
    return true;
  }

  void dumps_scrub(Formatter *f) {
    assert(f != nullptr);
    Mutex::Locker l(sched_scrub_lock);

    f->open_array_section("scrubs");
    for (const auto &i: sched_scrub_pg) {
      f->open_object_section("scrub");
      f->dump_stream("pgid") << i.pgid;
      f->dump_stream("sched_time") << i.sched_time;
      f->dump_stream("deadline") << i.deadline;
      f->dump_bool("forced", i.sched_time == i.deadline);
      f->close_section();
    }
    f->close_section();
  }

  bool can_inc_scrubs_pending();
  bool inc_scrubs_pending();
  void inc_scrubs_active(bool reserved);
  void dec_scrubs_pending();
  void dec_scrubs_active();

  void reply_op_error(OpRequestRef op, int err);
  void reply_op_error(OpRequestRef op, int err, eversion_t v, version_t uv);
  void handle_misdirected_op(PG *pg, OpRequestRef op);


private:
  // -- agent shared state --
  Mutex agent_lock;
  Cond agent_cond;
  map<uint64_t, set<PGRef> > agent_queue;
  set<PGRef>::iterator agent_queue_pos;
  bool agent_valid_iterator;
  int agent_ops;
  int flush_mode_high_count; //once have one pg with FLUSH_MODE_HIGH then flush objects with high speed
  set<hobject_t> agent_oids;
  bool agent_active;
  struct AgentThread : public Thread {
    OSDService *osd;
    explicit AgentThread(OSDService *o) : osd(o) {}
    void *entry() override {
      osd->agent_entry();
      return NULL;
    }
  } agent_thread;
  bool agent_stop_flag;
  Mutex agent_timer_lock;
  SafeTimer agent_timer;

public:
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

  /// note start of an async (evict) op
  void agent_start_evict_op() {
    Mutex::Locker l(agent_lock);
    ++agent_ops;
  }

  /// note finish or cancellation of an async (evict) op
  void agent_finish_evict_op() {
    Mutex::Locker l(agent_lock);
    assert(agent_ops > 0);
    --agent_ops;
    agent_cond.Signal();
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

  void agent_inc_high_count() {
    Mutex::Locker l(agent_lock);
    flush_mode_high_count ++;
  }

  void agent_dec_high_count() {
    Mutex::Locker l(agent_lock);
    flush_mode_high_count --;
  }

private:
  /// throttle promotion attempts
  std::atomic_uint promote_probability_millis{1000}; ///< probability thousands. one word.
  PromoteCounter promote_counter;
  utime_t last_recalibrate;
  unsigned long promote_max_objects, promote_max_bytes;

public:
  bool promote_throttle() {
    // NOTE: lockless!  we rely on the probability being a single word.
    promote_counter.attempt();
    if ((unsigned)rand() % 1000 > promote_probability_millis)
      return true;  // yes throttle (no promote)
    if (promote_max_objects &&
	promote_counter.objects > promote_max_objects)
      return true;  // yes throttle
    if (promote_max_bytes &&
	promote_counter.bytes > promote_max_bytes)
      return true;  // yes throttle
    return false;   //  no throttle (promote)
  }
  void promote_finish(uint64_t bytes) {
    promote_counter.finish(bytes);
  }
  void promote_throttle_recalibrate();

  // -- Objecter, for tiering reads/writes from/to other OSDs --
  Objecter *objecter;
  Finisher objecter_finisher;

  // -- Watch --
  Mutex watch_lock;
  SafeTimer watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    Mutex::Locker l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }

  // -- Recovery/Backfill Request Scheduling --
  Mutex recovery_request_lock;
  SafeTimer recovery_request_timer;

  // For async recovery sleep
  bool recovery_needs_sleep = true;
  utime_t recovery_schedule_time = utime_t();

  Mutex recovery_sleep_lock;
  SafeTimer recovery_sleep_timer;

  // -- tids --
  // for ops i issue
  std::atomic_uint last_tid{0};
  ceph_tid_t get_tid() {
    return (ceph_tid_t)last_tid++;
  }

  // -- backfill_reservation --
  Finisher reserver_finisher;
  AsyncReserver<spg_t> local_reserver;
  AsyncReserver<spg_t> remote_reserver;

  // -- pg_temp --
private:
  Mutex pg_temp_lock;
  struct pg_temp_t {
    pg_temp_t()
    {}
    pg_temp_t(vector<int> v, bool f)
      : acting{v}, forced{f}
    {}
    vector<int> acting;
    bool forced = false;
  };
  map<pg_t, pg_temp_t> pg_temp_wanted;
  map<pg_t, pg_temp_t> pg_temp_pending;
  void _sent_pg_temp();
  friend std::ostream& operator<<(std::ostream&, const pg_temp_t&);
public:
  void queue_want_pg_temp(pg_t pgid, const vector<int>& want,
			  bool forced = false);
  void remove_want_pg_temp(pg_t pgid);
  void requeue_pg_temp();
  void send_pg_temp();

  void send_pg_created(pg_t pgid);

  void queue_for_peering(PG *pg);

  Mutex snap_sleep_lock;
  SafeTimer snap_sleep_timer;

  Mutex scrub_sleep_lock;
  SafeTimer scrub_sleep_timer;

  AsyncReserver<spg_t> snap_reserver;
  void queue_for_snap_trim(PG *pg);

  void queue_for_scrub(PG *pg, bool with_high_priority) {
    unsigned scrub_queue_priority = pg->scrubber.priority;
    if (with_high_priority && scrub_queue_priority < cct->_conf->osd_client_op_priority) {
      scrub_queue_priority = cct->_conf->osd_client_op_priority;
    }
    enqueue_back(
      pg->info.pgid,
      PGQueueable(
	PGScrub(pg->get_osdmap()->get_epoch()),
	cct->_conf->osd_scrub_cost,
	scrub_queue_priority,
	ceph_clock_now(),
	entity_inst_t(),
	pg->get_osdmap()->get_epoch()));
  }

private:
  // -- pg recovery and associated throttling --
  Mutex recovery_lock;
  list<pair<epoch_t, PGRef> > awaiting_throttle;

  utime_t defer_recovery_until;
  uint64_t recovery_ops_active;
  uint64_t recovery_ops_reserved;
  bool recovery_paused;
#ifdef DEBUG_RECOVERY_OIDS
  map<spg_t, set<hobject_t> > recovery_oids;
#endif
  bool _recover_now(uint64_t *available_pushes);
  void _maybe_queue_recovery();
  void _queue_for_recovery(
    pair<epoch_t, PGRef> p, uint64_t reserved_pushes) {
    assert(recovery_lock.is_locked_by_me());
    enqueue_back(
      p.second->info.pgid,
      PGQueueable(
	PGRecovery(p.first, reserved_pushes),
	cct->_conf->osd_recovery_cost,
	cct->_conf->osd_recovery_priority,
	ceph_clock_now(),
	entity_inst_t(),
	p.first));
  }
public:
  void start_recovery_op(PG *pg, const hobject_t& soid);
  void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue);
  bool is_recovery_active();
  void release_reserved_pushes(uint64_t pushes) {
    Mutex::Locker l(recovery_lock);
    assert(recovery_ops_reserved >= pushes);
    recovery_ops_reserved -= pushes;
    _maybe_queue_recovery();
  }
  void defer_recovery(float defer_for) {
    defer_recovery_until = ceph_clock_now();
    defer_recovery_until += defer_for;
  }
  void pause_recovery() {
    Mutex::Locker l(recovery_lock);
    recovery_paused = true;
  }
  bool recovery_is_paused() {
    Mutex::Locker l(recovery_lock);
    return recovery_paused;
  }
  void unpause_recovery() {
    Mutex::Locker l(recovery_lock);
    recovery_paused = false;
    _maybe_queue_recovery();
  }
  void kick_recovery_queue() {
    Mutex::Locker l(recovery_lock);
    _maybe_queue_recovery();
  }
  void clear_queued_recovery(PG *pg) {
    Mutex::Locker l(recovery_lock);
    for (list<pair<epoch_t, PGRef> >::iterator i = awaiting_throttle.begin();
	 i != awaiting_throttle.end();
      ) {
      if (i->second.get() == pg) {
	awaiting_throttle.erase(i);
	return;
      } else {
	++i;
      }
    }
  }
  // delayed pg activation
  void queue_for_recovery(PG *pg) {
    Mutex::Locker l(recovery_lock);

    if (pg->get_state() & (PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL)) {
      awaiting_throttle.push_front(make_pair(pg->get_osdmap()->get_epoch(), pg));
    } else {
      awaiting_throttle.push_back(make_pair(pg->get_osdmap()->get_epoch(), pg));
    }
    _maybe_queue_recovery();
  }
  void queue_recovery_after_sleep(PG *pg, epoch_t queued, uint64_t reserved_pushes) {
    Mutex::Locker l(recovery_lock);
    _queue_for_recovery(make_pair(queued, pg), reserved_pushes);
  }

  void adjust_pg_priorities(const vector<PGRef>& pgs, int newflags);

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
  void final_init();  
  void start_shutdown();
  void shutdown_reserver();
  void shutdown();

private:
  // split
  Mutex in_progress_split_lock;
  map<spg_t, spg_t> pending_splits; // child -> parent
  map<spg_t, set<spg_t> > rev_pending_splits; // parent -> [children]
  set<spg_t> in_progress_splits;       // child

public:
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

  // -- stats --
  Mutex stat_lock;
  osd_stat_t osd_stat;
  uint32_t seq = 0;

  void set_statfs(const struct store_statfs_t &stbuf);
  osd_stat_t set_osd_stat(vector<int>& hb_peers, int num_pgs);
  osd_stat_t get_osd_stat() {
    Mutex::Locker l(stat_lock);
    ++seq;
    osd_stat.up_from = up_epoch;
    osd_stat.seq = ((uint64_t)osd_stat.up_from << 32) + seq;
    return osd_stat;
  }
  uint64_t get_osd_stat_seq() {
    Mutex::Locker l(stat_lock);
    return osd_stat.seq;
  }
  void get_hb_pingtime(map<int, osd_stat_t::Interfaces> *pp)
  {
    Mutex::Locker l(stat_lock);
    *pp = osd_stat.hb_pingtime;
    return;
  }

  // -- OSD Full Status --
private:
  friend TestOpsSocketHook;
  mutable Mutex full_status_lock;
  enum s_names { INVALID = -1, NONE, NEARFULL, BACKFILLFULL, FULL, FAILSAFE } cur_state;  // ascending
  const char *get_full_state_name(s_names s) const {
    switch (s) {
    case NONE: return "none";
    case NEARFULL: return "nearfull";
    case BACKFILLFULL: return "backfillfull";
    case FULL: return "full";
    case FAILSAFE: return "failsafe";
    default: return "???";
    }
  }
  s_names get_full_state(string type) const {
    if (type == "none")
      return NONE;
    else if (type == "failsafe")
      return FAILSAFE;
    else if (type == "full")
      return FULL;
    else if (type == "backfillfull")
      return BACKFILLFULL;
    else if (type == "nearfull")
      return NEARFULL;
    else
      return INVALID;
  }
  double cur_ratio;  ///< current utilization
  mutable int64_t injectfull = 0;
  s_names injectfull_state = NONE;
  float get_failsafe_full_ratio();
  bool _check_full(s_names type, ostream &ss) const;
public:
  void check_full_status(float ratio);
  bool check_failsafe_full(ostream &ss) const;
  bool check_full(ostream &ss) const;
  bool check_backfill_full(ostream &ss) const;
  bool check_nearfull(ostream &ss) const;
  bool is_failsafe_full() const;
  bool is_full() const;
  bool is_backfillfull() const;
  bool is_nearfull() const;
  bool need_fullness_update();  ///< osdmap state needs update
  void set_injectfull(s_names type, int64_t count);
  bool check_osdmap_full(const set<pg_shard_t> &missing_on);


  // -- epochs --
private:
  mutable Mutex epoch_lock; // protects access to boot_epoch, up_epoch, bind_epoch
  epoch_t boot_epoch;  // _first_ epoch we were marked up (after this process started)
  epoch_t up_epoch;    // _most_recent_ epoch we were marked up
  epoch_t bind_epoch;  // epoch we last did a bind to new ip:ports
public:
  /**
   * Retrieve the boot_, up_, and bind_ epochs the OSD has set. The params
   * can be NULL if you don't care about them.
   */
  void retrieve_epochs(epoch_t *_boot_epoch, epoch_t *_up_epoch,
                       epoch_t *_bind_epoch) const;
  /**
   * Set the boot, up, and bind epochs. Any NULL params will not be set.
   */
  void set_epochs(const epoch_t *_boot_epoch, const epoch_t *_up_epoch,
                  const epoch_t *_bind_epoch);
  epoch_t get_boot_epoch() const {
    epoch_t ret;
    retrieve_epochs(&ret, NULL, NULL);
    return ret;
  }
  epoch_t get_up_epoch() const {
    epoch_t ret;
    retrieve_epochs(NULL, &ret, NULL);
    return ret;
  }
  epoch_t get_bind_epoch() const {
    epoch_t ret;
    retrieve_epochs(NULL, NULL, &ret);
    return ret;
  }

  void request_osdmap_update(epoch_t e);

  // -- stopping --
  Mutex is_stopping_lock;
  Cond is_stopping_cond;
  enum {
    NOT_STOPPING,
    PREPARING_TO_STOP,
    STOPPING };
  std::atomic_int state{NOT_STOPPING};
  int get_state() {
    return state;
  }
  void set_state(int s) {
    state = s;
  }
  bool is_stopping() const {
    return state == STOPPING;
  }
  bool is_preparing_to_stop() const {
    return state == PREPARING_TO_STOP;
  }
  bool prepare_to_stop();
  void got_stop_ack();


#ifdef PG_DEBUG_REFS
  Mutex pgid_lock;
  map<spg_t, int> pgid_tracker;
  map<spg_t, PG*> live_pgs;
  void add_pgid(spg_t pgid, PG *pg);
  void remove_pgid(spg_t pgid, PG *pg);
  void dump_live_pgids();
#endif

  explicit OSDService(OSD *osd);
  ~OSDService();
};

class OSD : public Dispatcher,
	    public md_config_obs_t {
  /** OSD **/
  Mutex osd_lock;			// global lock
  SafeTimer tick_timer;    // safe timer (osd_lock)

  // Tick timer for those stuff that do not need osd_lock
  Mutex tick_timer_lock;
  SafeTimer tick_timer_without_osd_lock;
public:
  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const struct md_config_t *conf,
                          const std::set <std::string> &changed) override;
  void update_log_config();
  void check_config();

protected:

  const double OSD_TICK_INTERVAL = { 1.0 };
  double get_tick_interval() const;

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

  Messenger   *cluster_messenger;
  Messenger   *client_messenger;
  Messenger   *objecter_messenger;
  MonClient   *monc; // check the "monc helpers" list before accessing directly
  MgrClient   mgrc;
  PerfCounters      *logger;
  PerfCounters      *recoverystate_perf;
  ObjectStore *store;
#ifdef HAVE_LIBFUSE
  FuseStore *fuse_store = nullptr;
#endif
  LogClient log_client;
  LogChannelRef clog;

  int whoami;
  std::string dev_path, journal_path;

  bool store_is_rotational = true;
  bool journal_is_rotational = true;

  ZTracer::Endpoint trace_endpoint;
  void create_logger();
  void create_recoverystate_perf();
  void tick();
  void tick_without_osd_lock();
  void _dispatch(Message *m);
  void dispatch_op(OpRequestRef op);

  void check_osdmap_features(ObjectStore *store);

  // asok
  friend class OSDSocketHook;
  class OSDSocketHook *asok_hook;
  bool asok_command(string admin_command, cmdmap_t& cmdmap, string format, ostream& ss);

public:
  ClassHandler  *class_handler = nullptr;
  int get_nodeid() { return whoami; }
  
  static ghobject_t get_osdmap_pobject_name(epoch_t epoch) {
    char foo[20];
    snprintf(foo, sizeof(foo), "osdmap.%d", epoch);
    return ghobject_t(hobject_t(sobject_t(object_t(foo), 0)));
  }
  static ghobject_t get_inc_osdmap_pobject_name(epoch_t epoch) {
    char foo[22];
    snprintf(foo, sizeof(foo), "inc_osdmap.%d", epoch);
    return ghobject_t(hobject_t(sobject_t(object_t(foo), 0)));
  }

  static ghobject_t make_snapmapper_oid() {
    return ghobject_t(hobject_t(
      sobject_t(
	object_t("snapmapper"),
	0)));
  }

  static ghobject_t make_pg_log_oid(spg_t pg) {
    stringstream ss;
    ss << "pglog_" << pg;
    string s;
    getline(ss, s);
    return ghobject_t(hobject_t(sobject_t(object_t(s.c_str()), 0)));
  }
  
  static ghobject_t make_pg_biginfo_oid(spg_t pg) {
    stringstream ss;
    ss << "pginfo_" << pg;
    string s;
    getline(ss, s);
    return ghobject_t(hobject_t(sobject_t(object_t(s.c_str()), 0)));
  }
  static ghobject_t make_infos_oid() {
    hobject_t oid(sobject_t("infos", CEPH_NOSNAP));
    return ghobject_t(oid);
  }
  static void recursive_remove_collection(CephContext* cct,
					  ObjectStore *store,
					  spg_t pgid,
					  coll_t tmp);

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
  class C_Tick;
  class C_Tick_WithoutOSDLock;

  // -- superblock --
  OSDSuperblock superblock;

  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();

  void clear_temp_objects();

  CompatSet osd_compat;

  // -- state --
public:
  typedef enum {
    STATE_INITIALIZING = 1,
    STATE_PREBOOT,
    STATE_BOOTING,
    STATE_ACTIVE,
    STATE_STOPPING,
    STATE_WAITING_FOR_HEALTHY
  } osd_state_t;

  static const char *get_state_name(int s) {
    switch (s) {
    case STATE_INITIALIZING: return "initializing";
    case STATE_PREBOOT: return "preboot";
    case STATE_BOOTING: return "booting";
    case STATE_ACTIVE: return "active";
    case STATE_STOPPING: return "stopping";
    case STATE_WAITING_FOR_HEALTHY: return "waiting_for_healthy";
    default: return "???";
    }
  }

private:
  std::atomic_int state{STATE_INITIALIZING};
  bool waiting_for_luminous_mons = false;

public:
  int get_state() const {
    return state;
  }
  void set_state(int s) {
    state = s;
  }
  bool is_initializing() const {
    return state == STATE_INITIALIZING;
  }
  bool is_preboot() const {
    return state == STATE_PREBOOT;
  }
  bool is_booting() const {
    return state == STATE_BOOTING;
  }
  bool is_active() const {
    return state == STATE_ACTIVE;
  }
  bool is_stopping() const {
    return state == STATE_STOPPING;
  }
  bool is_waiting_for_healthy() const {
    return state == STATE_WAITING_FOR_HEALTHY;
  }

private:

  ThreadPool peering_tp;
  ShardedThreadPool osd_op_tp;
  ThreadPool remove_tp;
  ThreadPool recovery_tp;
  ThreadPool command_tp;

  void set_disk_tp_priority();
  void get_latest_osdmap();

  // -- sessions --
private:
  void dispatch_session_waiting(SessionRef session, OSDMapRef osdmap);
  void maybe_share_map(Session *session, OpRequestRef op, OSDMapRef osdmap);

  Mutex session_waiting_lock;
  set<SessionRef> session_waiting_for_map;

  /// Caller assumes refs for included Sessions
  void get_sessions_waiting_for_map(set<SessionRef> *out) {
    Mutex::Locker l(session_waiting_lock);
    out->swap(session_waiting_for_map);
  }
  void register_session_waiting_on_map(SessionRef session) {
    Mutex::Locker l(session_waiting_lock);
    session_waiting_for_map.insert(session);
  }
  void clear_session_waiting_on_map(SessionRef session) {
    Mutex::Locker l(session_waiting_lock);
    session_waiting_for_map.erase(session);
  }
  void dispatch_sessions_waiting_on_map() {
    set<SessionRef> sessions_to_check;
    get_sessions_waiting_for_map(&sessions_to_check);
    for (auto i = sessions_to_check.begin();
	 i != sessions_to_check.end();
	 sessions_to_check.erase(i++)) {
      Mutex::Locker l{(*i)->session_dispatch_lock};
      SessionRef session = *i;
      dispatch_session_waiting(session, osdmap);
    }
  }
  void session_handle_reset(SessionRef session) {
    Mutex::Locker l(session->session_dispatch_lock);
    clear_session_waiting_on_map(session);

    session->clear_backoffs();

    /* Messages have connection refs, we need to clear the
     * connection->session->message->connection
     * cycles which result.
     * Bug #12338
     */
    session->waiting_on_map.clear_and_dispose(TrackedOp::Putter());
  }

private:
  /**
   * @defgroup monc helpers
   * @{
   * Right now we only have the one
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

  Mutex osdmap_subscribe_lock;
  epoch_t latest_subscribed_epoch{0};

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
    /// number of connections we send and receive heartbeat pings/replies
    const static int HEARTBEAT_MAX_CONN = 2;
    /// history of inflight pings, arranging by timestamp we sent
    /// send time -> deadline -> remaining replies
    map<utime_t, pair<utime_t, int>> ping_history;

    utime_t hb_interval_start;
    uint32_t hb_average_count = 0;
    uint32_t hb_index = 0;

    uint32_t hb_total_back = 0;
    uint32_t hb_min_back = UINT_MAX;
    uint32_t hb_max_back = 0;
    vector<uint32_t> hb_back_pingtime;
    vector<uint32_t> hb_back_min;
    vector<uint32_t> hb_back_max;

    uint32_t hb_total_front = 0;
    uint32_t hb_min_front = UINT_MAX;
    uint32_t hb_max_front = 0;
    vector<uint32_t> hb_front_pingtime;
    vector<uint32_t> hb_front_min;
    vector<uint32_t> hb_front_max;

    bool is_unhealthy(utime_t now) {
      if (ping_history.empty()) {
        /// we haven't sent a ping yet or we have got all replies,
        /// in either way we are safe and healthy for now
        return false;
      }

      utime_t oldest_deadline = ping_history.begin()->second.first;
      return now > oldest_deadline;
    }

    bool is_healthy(utime_t now) {
      if (last_rx_front == utime_t() || last_rx_back == utime_t()) {
        // only declare to be healthy until we have received the first
        // replies from both front/back connections
        return false;
      }
      return !is_unhealthy(now);
    }
  };
  /// state attached to outgoing heartbeat connections
  struct HeartbeatSession : public RefCountedObject {
    int peer;
    explicit HeartbeatSession(int p) : peer(p) {}
  };
  Mutex heartbeat_lock;
  map<int, int> debug_heartbeat_drops_remaining;
  Cond heartbeat_cond;
  bool heartbeat_stop;
  std::atomic_bool heartbeat_need_update;   
  map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  utime_t last_mon_heartbeat;
  Messenger *hb_front_client_messenger;
  Messenger *hb_back_client_messenger;
  Messenger *hb_front_server_messenger;
  Messenger *hb_back_server_messenger;
  utime_t last_heartbeat_resample;   ///< last time we chose random peers in waiting-for-healthy state
  double daily_loadavg;

  // Track ping repsonse times using vector as a circular buffer
  // MUST BE A POWER OF 2
  const uint32_t hb_vector_size = 16;

  void _add_heartbeat_peer(int p);
  void _remove_heartbeat_peer(int p);
  bool heartbeat_reset(Connection *con);
  void maybe_update_heartbeat_peers();
  void reset_heartbeat_peers();
  bool heartbeat_peers_need_update() {
    return heartbeat_need_update.load();
  }
  void heartbeat_set_peers_need_update() {
    heartbeat_need_update.store(true);
  }
  void heartbeat_clear_peers_need_update() {
    heartbeat_need_update.store(false);
  }
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
    explicit T_Heartbeat(OSD *o) : osd(o) {}
    void *entry() override {
      osd->heartbeat_entry();
      return 0;
    }
  } heartbeat_thread;

public:
  bool heartbeat_dispatch(Message *m);

  struct HeartbeatDispatcher : public Dispatcher {
    OSD *osd;
    explicit HeartbeatDispatcher(OSD *o) : Dispatcher(o->cct), osd(o) {}

    bool ms_can_fast_dispatch_any() const override { return true; }
    bool ms_can_fast_dispatch(const Message *m) const override {
      switch (m->get_type()) {
	case CEPH_MSG_PING:
	case MSG_OSD_PING:
          return true;
	default:
          return false;
	}
    }
    void ms_fast_dispatch(Message *m) override {
      osd->heartbeat_dispatch(m);
    }
    bool ms_dispatch(Message *m) override {
      return osd->heartbeat_dispatch(m);
    }
    bool ms_handle_reset(Connection *con) override {
      return osd->heartbeat_reset(con);
    }
    void ms_handle_remote_reset(Connection *con) override {}
    bool ms_handle_refused(Connection *con) override {
      return osd->ms_handle_refused(con);
    }
    bool ms_verify_authorizer(Connection *con, int peer_type,
			      int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			      bool& isvalid, CryptoKey& session_key,
			      std::unique_ptr<AuthAuthorizerChallenge> *challenge) override {
      isvalid = true;
      return true;
    }
  } heartbeat_dispatcher;

private:
  // -- waiters --
  list<OpRequestRef> finished;
  
  void take_waiters(list<OpRequestRef>& ls) {
    assert(osd_lock.is_locked());
    finished.splice(finished.end(), ls);
  }
  void do_waiters();
  
  // -- op tracking --
  OpTracker op_tracker;
  void check_ops_in_flight();
  void test_ops(std::string command, std::string args, ostream& ss);
  friend class TestOpsSocketHook;
  TestOpsSocketHook *test_ops_hook;
  friend struct C_CompleteSplits;
  friend struct C_OpenPGs;

  // -- op queue --
  enum class io_queue {
    prioritized,
    weightedpriority,
    mclock_opclass,
    mclock_client,
  };
  friend std::ostream& operator<<(std::ostream& out, const OSD::io_queue& q);

  const io_queue op_queue;
  const unsigned int op_prio_cutoff;

  /*
   * The ordered op delivery chain is:
   *
   *   fast dispatch -> pqueue back
   *                    pqueue front <-> to_process back
   *                                     to_process front  -> RunVis(item)
   *                                                      <- queue_front()
   *
   * The pqueue is per-shard, and to_process is per pg_slot.  Items can be
   * pushed back up into to_process and/or pqueue while order is preserved.
   *
   * Multiple worker threads can operate on each shard.
   *
   * Under normal circumstances, num_running == to_proces.size().  There are
   * two times when that is not true: (1) when waiting_for_pg == true and
   * to_process is accumulating requests that are waiting for the pg to be
   * instantiated; in that case they will all get requeued together by
   * wake_pg_waiters, and (2) when wake_pg_waiters just ran, waiting_for_pg
   * and already requeued the items.
   */
  friend class PGQueueable;

  class ShardedOpWQ
    : public ShardedThreadPool::ShardedWQ<pair<spg_t,PGQueueable>>
  {
    struct ShardData {
      Mutex sdata_lock;
      Cond sdata_cond;

      Mutex sdata_op_ordering_lock;   ///< protects all members below

      OSDMapRef waiting_for_pg_osdmap;
      struct pg_slot {
	PGRef pg;                     ///< cached pg reference [optional]
	list<PGQueueable> to_process; ///< order items for this slot
	int num_running = 0;          ///< _process threads doing pg lookup/lock

	/// true if pg does/did not exist. if so all new items go directly to
	/// to_process.  cleared by prune_pg_waiters.
	bool waiting_for_pg = false;

	/// incremented by wake_pg_waiters; indicates racing _process threads
	/// should bail out (their op has been requeued)
	uint64_t requeue_seq = 0;
      };

      /// map of slots for each spg_t.  maintains ordering of items dequeued
      /// from pqueue while _process thread drops shard lock to acquire the
      /// pg lock.  slots are removed only by prune_pg_waiters.
      unordered_map<spg_t,pg_slot> pg_slots;

      /// priority queue
      std::unique_ptr<OpQueue< pair<spg_t, PGQueueable>, entity_inst_t>> pqueue;

      void _enqueue_front(pair<spg_t, PGQueueable> item, unsigned cutoff) {
	unsigned priority = item.second.get_priority();
	unsigned cost = item.second.get_cost();
	if (priority >= cutoff)
	  pqueue->enqueue_strict_front(
	    item.second.get_owner(),
	    priority, item);
	else
	  pqueue->enqueue_front(
	    item.second.get_owner(),
	    priority, cost, item);
      }

      ShardData(
	string lock_name, string ordering_lock,
	uint64_t max_tok_per_prio, uint64_t min_cost, CephContext *cct,
	io_queue opqueue)
	: sdata_lock(lock_name.c_str(), false, true, false, cct),
	  sdata_op_ordering_lock(ordering_lock.c_str(), false, true,
				 false, cct) {
	if (opqueue == io_queue::weightedpriority) {
	  pqueue = std::unique_ptr
	    <WeightedPriorityQueue<pair<spg_t,PGQueueable>,entity_inst_t>>(
	      new WeightedPriorityQueue<pair<spg_t,PGQueueable>,entity_inst_t>(
		max_tok_per_prio, min_cost));
	} else if (opqueue == io_queue::prioritized) {
	  pqueue = std::unique_ptr
	    <PrioritizedQueue<pair<spg_t,PGQueueable>,entity_inst_t>>(
	      new PrioritizedQueue<pair<spg_t,PGQueueable>,entity_inst_t>(
		max_tok_per_prio, min_cost));
	} else if (opqueue == io_queue::mclock_opclass) {
	  pqueue = std::unique_ptr
	    <ceph::mClockOpClassQueue>(new ceph::mClockOpClassQueue(cct));
	} else if (opqueue == io_queue::mclock_client) {
	  pqueue = std::unique_ptr
	    <ceph::mClockClientQueue>(new ceph::mClockClientQueue(cct));
	}
      }
    }; // struct ShardData

    vector<ShardData*> shard_list;
    OSD *osd;
    uint32_t num_shards;

  public:
    ShardedOpWQ(uint32_t pnum_shards,
		OSD *o,
		time_t ti,
		time_t si,
		ShardedThreadPool* tp)
      : ShardedThreadPool::ShardedWQ<pair<spg_t,PGQueueable>>(ti, si, tp),
        osd(o),
        num_shards(pnum_shards) {
      for (uint32_t i = 0; i < num_shards; i++) {
	char lock_name[32] = {0};
	snprintf(lock_name, sizeof(lock_name), "%s.%d", "OSD:ShardedOpWQ:", i);
	char order_lock[32] = {0};
	snprintf(order_lock, sizeof(order_lock), "%s.%d",
		 "OSD:ShardedOpWQ:order:", i);
	ShardData* one_shard = new ShardData(
	  lock_name, order_lock,
	  osd->cct->_conf->osd_op_pq_max_tokens_per_priority, 
	  osd->cct->_conf->osd_op_pq_min_cost, osd->cct, osd->op_queue);
	shard_list.push_back(one_shard);
      }
    }
    ~ShardedOpWQ() override {
      while (!shard_list.empty()) {
	delete shard_list.back();
	shard_list.pop_back();
      }
    }

    /// wake any pg waiters after a PG is created/instantiated
    void wake_pg_waiters(spg_t pgid);

    /// prune ops (and possiblye pg_slots) for pgs that shouldn't be here
    void prune_pg_waiters(OSDMapRef osdmap, int whoami);

    /// clear cached PGRef on pg deletion
    void clear_pg_pointer(spg_t pgid);

    /// clear pg_slots on shutdown
    void clear_pg_slots();

    /// try to do some work
    void _process(uint32_t thread_index, heartbeat_handle_d *hb) override;

    /// enqueue a new item
    void _enqueue(pair <spg_t, PGQueueable> item) override;

    /// requeue an old item (at the front of the line)
    void _enqueue_front(pair <spg_t, PGQueueable> item) override;
      
    void return_waiting_threads() override {
      for(uint32_t i = 0; i < num_shards; i++) {
	ShardData* sdata = shard_list[i];
	assert (NULL != sdata); 
	sdata->sdata_lock.Lock();
	sdata->sdata_cond.Signal();
	sdata->sdata_lock.Unlock();
      }
    }

    void dump(Formatter *f) {
      for(uint32_t i = 0; i < num_shards; i++) {
	ShardData* sdata = shard_list[i];
	char lock_name[32] = {0};
	snprintf(lock_name, sizeof(lock_name), "%s%d", "OSD:ShardedOpWQ:", i);
	assert (NULL != sdata);
	sdata->sdata_op_ordering_lock.Lock();
	f->open_object_section(lock_name);
	sdata->pqueue->dump(f);
	f->close_section();
	sdata->sdata_op_ordering_lock.Unlock();
      }
    }

    /// Must be called on ops queued back to front
    struct Pred {
      spg_t pgid;
      list<OpRequestRef> *out_ops;
      uint64_t reserved_pushes_to_free;
      Pred(spg_t pg, list<OpRequestRef> *out_ops = 0)
	: pgid(pg), out_ops(out_ops), reserved_pushes_to_free(0) {}
      void accumulate(const PGQueueable &op) {
	reserved_pushes_to_free += op.get_reserved_pushes();
	if (out_ops) {
	  boost::optional<OpRequestRef> mop = op.maybe_get_op();
	  if (mop)
	    out_ops->push_front(*mop);
	}
      }
      bool operator()(const pair<spg_t, PGQueueable> &op) {
	if (op.first == pgid) {
	  accumulate(op.second);
	  return true;
	} else {
	  return false;
	}
      }
      uint64_t get_reserved_pushes_to_free() const {
	return reserved_pushes_to_free;
      }
    };

    bool is_shard_empty(uint32_t thread_index) override {
      uint32_t shard_index = thread_index % num_shards; 
      ShardData* sdata = shard_list[shard_index];
      assert(NULL != sdata);
      Mutex::Locker l(sdata->sdata_op_ordering_lock);
      return sdata->pqueue->empty();
    }
  } op_shardedwq;


  void enqueue_op(spg_t pg, OpRequestRef& op, epoch_t epoch);
  void dequeue_op(
    PGRef pg, OpRequestRef op,
    ThreadPool::TPHandle &handle);

  // -- peering queue --
  struct PeeringWQ : public ThreadPool::BatchWorkQueue<PG> {
    list<PG*> peering_queue;
    OSD *osd;
    set<PG*> in_use;
    PeeringWQ(OSD *o, time_t ti, time_t si, ThreadPool *tp)
      : ThreadPool::BatchWorkQueue<PG>(
	"OSD::PeeringWQ", ti, si, tp), osd(o) {}

    void _dequeue(PG *pg) override {
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
    bool _enqueue(PG *pg) override {
      pg->get("PeeringWQ");
      peering_queue.push_back(pg);
      return true;
    }
    bool _empty() override {
      return peering_queue.empty();
    }
    void _dequeue(list<PG*> *out) override;
    void _process(
      const list<PG *> &pgs,
      ThreadPool::TPHandle &handle) override {
      assert(!pgs.empty());
      osd->process_peering_events(pgs, handle);
      for (list<PG *>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	(*i)->put("PeeringWQ");
      }
    }
    void _process_finish(const list<PG *> &pgs) override {
      for (list<PG*>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	in_use.erase(*i);
      }
    }
    void _clear() override {
      assert(peering_queue.empty());
    }
  } peering_wq;

  void process_peering_events(
    const list<PG*> &pg,
    ThreadPool::TPHandle &handle);

  friend class PG;
  friend class PrimaryLogPG;


 protected:

  // -- osd map --
  OSDMapRef       osdmap;
  OSDMapRef get_osdmap() {
    return osdmap;
  }
  epoch_t get_osdmap_epoch() const {
    return osdmap ? osdmap->get_epoch() : 0;
  }

  utime_t         had_map_since;
  RWLock          map_lock;
  list<OpRequestRef>  waiting_for_osdmap;
  deque<utime_t> osd_markdown_log;

  friend struct send_map_on_destruct;

  void wait_for_new_map(OpRequestRef op);
  void handle_osd_map(class MOSDMap *m);
  void _committed_osd_maps(epoch_t first, epoch_t last, class MOSDMap *m);
  void trim_maps(epoch_t oldest, int nreceived, bool skip_maps);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  friend class C_OnMapCommit;

  bool advance_pg(
    epoch_t advance_to, PG *pg,
    ThreadPool::TPHandle &handle,
    PG::RecoveryCtx *rctx,
    set<PGRef> *split_pgs
  );
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

protected:
  // -- placement groups --
  RWLock pg_map_lock; // this lock orders *above* individual PG _locks
  ceph::unordered_map<spg_t, PG*> pg_map; // protected by pg_map lock

  std::mutex pending_creates_lock;
  using create_from_osd_t = std::pair<pg_t, bool /* is primary*/>;
  std::set<create_from_osd_t> pending_creates_from_osd;
  unsigned pending_creates_from_mon = 0;

  map<spg_t, list<PG::CephPeeringEvtRef> > peering_wait_for_split;
  PGRecoveryStats pg_recovery_stats;

  PGPool _get_pool(int id, OSDMapRef createmap);

  PG   *_lookup_lock_pg_with_map_lock_held(spg_t pgid);
  PG   *_lookup_lock_pg(spg_t pgid);

public:
  PG   *lookup_lock_pg(spg_t pgid);

  int get_num_pgs() {
    RWLock::RLocker l(pg_map_lock);
    return pg_map.size();
  }

protected:
  PG   *_open_lock_pg(OSDMapRef createmap,
		      spg_t pg, bool no_lockdep_check=false);
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
    bool hold_map_lock,
    bool backfill,
    int role,
    vector<int>& up, int up_primary,
    vector<int>& acting, int acting_primary,
    pg_history_t history,
    const PastIntervals& pi,
    ObjectStore::Transaction& t);

  PG* _make_pg(OSDMapRef createmap, spg_t pgid);
  void add_newly_split_pg(PG *pg,
			  PG::RecoveryCtx *rctx);

  int handle_pg_peering_evt(
    spg_t pgid,
    const pg_history_t& orig_history,
    const PastIntervals& pi,
    epoch_t epoch,
    PG::CephPeeringEvtRef evt);
  bool maybe_wait_for_max_pg(spg_t pgid, bool is_mon_create);
  void resume_creating_pg();

  void load_pgs();
  void build_past_intervals_parallel();

  /// build initial pg history and intervals on create
  void build_initial_pg_history(
    spg_t pgid,
    epoch_t created,
    utime_t created_stamp,
    pg_history_t *h,
    PastIntervals *pi);

  /// project pg history from from to now
  bool project_pg_history(
    spg_t pgid, pg_history_t& h, epoch_t from,
    const vector<int>& lastup,
    int lastupprimary,
    const vector<int>& lastacting,
    int lastactingprimary
    ); ///< @return false if there was a map gap between from and now

  // this must be called with pg->lock held on any pg addition to pg_map
  void wake_pg_waiters(PGRef pg) {
    assert(pg->is_locked());
    op_shardedwq.wake_pg_waiters(pg->info.pgid);
  }
  epoch_t last_pg_create_epoch;

  void handle_pg_create(OpRequestRef op);

  void split_pgs(
    PG *parent,
    const set<spg_t> &childpgids, set<PGRef> *out_pgs,
    OSDMapRef curmap,
    OSDMapRef nextmap,
    PG::RecoveryCtx *rctx);

  // == monitor interaction ==
  Mutex mon_report_lock;
  utime_t last_mon_report;
  utime_t last_pg_stats_sent;

  /* if our monitor dies, we want to notice it and reconnect.
   *  So we keep track of when it last acked our stat updates,
   *  and if too much time passes (and we've been sending
   *  more updates) then we can call it dead and reconnect
   *  elsewhere.
   */
  utime_t last_pg_stats_ack;
  float stats_ack_timeout;
  set<uint64_t> outstanding_pg_stats; // how many stat updates haven't been acked yet

  // -- boot --
  void start_boot();
  void _got_mon_epochs(epoch_t oldest, epoch_t newest);
  void _preboot(epoch_t oldest, epoch_t newest);
  void _send_boot();
  void _collect_metadata(map<string,string> *pmeta);

  void start_waiting_for_healthy();
  bool _is_healthy();

  void send_full_update();
  
  friend struct C_OSD_GetVersion;

  // -- alive --
  epoch_t up_thru_wanted;

  void queue_want_up_thru(epoch_t want);
  void send_alive();

  // -- full map requests --
  epoch_t requested_full_first, requested_full_last;

  void request_full_map(epoch_t first, epoch_t last);
  void rerequest_full_maps() {
    epoch_t first = requested_full_first;
    epoch_t last = requested_full_last;
    requested_full_first = 0;
    requested_full_last = 0;
    request_full_map(first, last);
  }
  void got_full_map(epoch_t e);

  // -- failures --
  map<int,utime_t> failure_queue;
  map<int,pair<utime_t,entity_inst_t> > failure_pending;

  void requeue_failures();
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

  ceph::coarse_mono_clock::time_point last_sent_beacon;
  Mutex min_last_epoch_clean_lock{"OSD::min_last_epoch_clean_lock"};
  epoch_t min_last_epoch_clean = 0;
  // which pgs were scanned for min_lec
  std::vector<pg_t> min_last_epoch_clean_pgs;
  void send_beacon(const ceph::coarse_mono_clock::time_point& now);

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
  void clear_outstanding_pg_stats(){
    Mutex::Locker l(pg_stat_queue_lock);
    outstanding_pg_stats.clear();
  }

  ceph_tid_t get_tid() {
    return service.get_tid();
  }

  // -- generic pg peering --
  PG::RecoveryCtx create_context();
  void dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap,
                        ThreadPool::TPHandle *handle = NULL);
  void dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg,
                                    ThreadPool::TPHandle *handle = NULL);
  void do_notifies(map<int,
		       vector<pair<pg_notify_t, PastIntervals> > >&
		       notify_list,
		   OSDMapRef map);
  void do_queries(map<int, map<spg_t,pg_query_t> >& query_map,
		  OSDMapRef map);
  void do_infos(map<int,
		    vector<pair<pg_notify_t, PastIntervals> > >& info_map,
		OSDMapRef map);

  bool require_mon_peer(const Message *m);
  bool require_mon_or_mgr_peer(const Message *m);
  bool require_osd_peer(const Message *m);
  /***
   * Verifies that we were alive in the given epoch, and that
   * still are.
   */
  bool require_self_aliveness(const Message *m, epoch_t alive_since);
  /**
   * Verifies that the OSD who sent the given op has the same
   * address as in the given map.
   * @pre op was sent by an OSD using the cluster messenger
   */
  bool require_same_peer_instance(const Message *m, OSDMapRef& map,
				  bool is_fast_dispatch);

  bool require_same_or_newer_map(OpRequestRef& op, epoch_t e,
				 bool is_fast_dispatch);

  void handle_pg_query(OpRequestRef op);
  void handle_pg_notify(OpRequestRef op);
  void handle_pg_log(OpRequestRef op);
  void handle_pg_info(OpRequestRef op);
  void handle_pg_trim(OpRequestRef op);

  void handle_pg_backfill_reserve(OpRequestRef op);
  void handle_pg_recovery_reserve(OpRequestRef op);

  void handle_force_recovery(Message *m);

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
    CommandWQ(OSD *o, time_t ti, time_t si, ThreadPool *tp)
      : ThreadPool::WorkQueue<Command>("OSD::CommandWQ", ti, si, tp), osd(o) {}

    bool _empty() override {
      return osd->command_queue.empty();
    }
    bool _enqueue(Command *c) override {
      osd->command_queue.push_back(c);
      return true;
    }
    void _dequeue(Command *pg) override {
      ceph_abort();
    }
    Command *_dequeue() override {
      if (osd->command_queue.empty())
	return NULL;
      Command *c = osd->command_queue.front();
      osd->command_queue.pop_front();
      return c;
    }
    void _process(Command *c, ThreadPool::TPHandle &) override {
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
    void _clear() override {
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
  void do_recovery(PG *pg, epoch_t epoch_queued, uint64_t pushes_reserved,
		   ThreadPool::TPHandle &handle);


  // -- scrubbing --
  void sched_scrub();
  bool scrub_random_backoff();
  bool scrub_load_below_threshold();
  bool scrub_time_permit(utime_t now);

  // -- removing --
  struct RemoveWQ :
    public ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> > {
    CephContext* cct;
    ObjectStore *&store;
    list<pair<PGRef, DeletingStateRef> > remove_queue;
    RemoveWQ(CephContext* cct, ObjectStore *&o, time_t ti, time_t si,
	     ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> >(
	"OSD::RemoveWQ", ti, si, tp), cct(cct), store(o) {}

    bool _empty() override {
      return remove_queue.empty();
    }
    void _enqueue(pair<PGRef, DeletingStateRef> item) override {
      remove_queue.push_back(item);
    }
    void _enqueue_front(pair<PGRef, DeletingStateRef> item) override {
      remove_queue.push_front(item);
    }
    bool _dequeue(pair<PGRef, DeletingStateRef> item) {
      ceph_abort();
    }
    pair<PGRef, DeletingStateRef> _dequeue() override {
      assert(!remove_queue.empty());
      pair<PGRef, DeletingStateRef> item = remove_queue.front();
      remove_queue.pop_front();
      return item;
    }
    void _process(pair<PGRef, DeletingStateRef>,
		  ThreadPool::TPHandle &) override;
    void _clear() override {
      remove_queue.clear();
    }
    int get_remove_queue_len() {
      lock();
      int r = remove_queue.size();
      unlock();
      return r;
    }
  } remove_wq;

  // -- status reporting --
  MPGStats *collect_pg_stats();
  std::vector<OSDHealthMetric> get_health_metrics();

private:
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OP:
    case CEPH_MSG_OSD_BACKOFF:
    case MSG_OSD_SUBOP:
    case MSG_OSD_REPOP:
    case MSG_OSD_SUBOPREPLY:
    case MSG_OSD_REPOPREPLY:
    case MSG_OSD_PG_PUSH:
    case MSG_OSD_PG_PULL:
    case MSG_OSD_PG_PUSH_REPLY:
    case MSG_OSD_PG_SCAN:
    case MSG_OSD_PG_BACKFILL:
    case MSG_OSD_PG_BACKFILL_REMOVE:
    case MSG_OSD_EC_WRITE:
    case MSG_OSD_EC_WRITE_REPLY:
    case MSG_OSD_EC_READ:
    case MSG_OSD_EC_READ_REPLY:
    case MSG_OSD_SCRUB_RESERVE:
    case MSG_OSD_REP_SCRUB:
    case MSG_OSD_REP_SCRUBMAP:
    case MSG_OSD_PG_UPDATE_LOG_MISSING:
    case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    case MSG_OSD_PG_RECOVERY_DELETE:
    case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
      return true;
    default:
      return false;
    }
  }
  void ms_fast_dispatch(Message *m) override;
  void ms_fast_preprocess(Message *m) override;
  bool ms_dispatch(Message *m) override;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new) override;
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key,
			    std::unique_ptr<AuthAuthorizerChallenge> *challenge) override;
  void ms_handle_connect(Connection *con) override;
  void ms_handle_fast_connect(Connection *con) override;
  void ms_handle_fast_accept(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  io_queue get_io_queue() const {
    if (cct->_conf->osd_op_queue == "debug_random") {
      static io_queue index_lookup[] = { io_queue::prioritized,
					 io_queue::weightedpriority,
					 io_queue::mclock_opclass,
					 io_queue::mclock_client };
      srand(time(NULL));
      unsigned which = rand() % (sizeof(index_lookup) / sizeof(index_lookup[0]));
      return index_lookup[which];
    } else if (cct->_conf->osd_op_queue == "prioritized") {
      return io_queue::prioritized;
    } else if (cct->_conf->osd_op_queue == "mclock_opclass") {
      return io_queue::mclock_opclass;
    } else if (cct->_conf->osd_op_queue == "mclock_client") {
      return io_queue::mclock_client;
    } else {
      // default / catch-all is 'wpq'
      return io_queue::weightedpriority;
    }
  }

  unsigned int get_io_prio_cut() const {
    if (cct->_conf->osd_op_queue_cut_off == "debug_random") {
      srand(time(NULL));
      return (rand() % 2 < 1) ? CEPH_MSG_PRIO_HIGH : CEPH_MSG_PRIO_LOW;
    } else if (cct->_conf->osd_op_queue_cut_off == "high") {
      return CEPH_MSG_PRIO_HIGH;
    } else {
      // default / catch-all is 'low'
      return CEPH_MSG_PRIO_LOW;
    }
  }

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(CephContext *cct_,
      ObjectStore *store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_front_client,
      Messenger *hb_back_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev);
  ~OSD() override;

  // static bits
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
  int mon_cmd_maybe_osd_create(string &cmd);
  int update_crush_device_class();
  int update_crush_location();

  static int write_meta(CephContext *cct,
			ObjectStore *store,
			uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami);

  void handle_pg_scrub(struct MOSDScrub *m, PG* pg);
  void handle_scrub(struct MOSDScrub *m);
  void handle_osd_ping(class MOSDPing *m);

  int init_op_flags(OpRequestRef& op);

  int get_num_op_shards();
  int get_num_op_threads();

  float get_osd_recovery_sleep();

public:
  static int peek_meta(ObjectStore *store, string& magic,
		       uuid_d& cluster_fsid, uuid_d& osd_fsid, int& whoami);
  

  // startup/shutdown
  int pre_init();
  int init();
  void final_init();

  int enable_disable_fuse(bool stop);

  void suicide(int exitcode);
  int shutdown();

  void handle_signal(int signum);

  /// check if we can throw out op from a disconnected client
  static bool op_is_discardable(const MOSDOp *m);

public:
  OSDService service;
  friend class OSDService;
};


std::ostream& operator<<(std::ostream& out, const OSD::io_queue& q);


//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif // CEPH_OSD_H
