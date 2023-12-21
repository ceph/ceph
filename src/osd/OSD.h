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

#include "common/async/context_pool.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/AsyncReserver.h"
#include "common/ceph_context.h"
#include "common/config_cacher.h"
#include "common/zipkin_trace.h"
#include "common/ceph_timer.h"

#include "mgr/MgrClient.h"

#include "os/ObjectStore.h"

#include "include/CompatSet.h"
#include "include/common_fwd.h"

#include "OpRequest.h"
#include "Session.h"

#include "osd/scheduler/OpScheduler.h"

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "include/unordered_map.h"

#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "messages/MOSDOp.h"
#include "common/EventTrace.h"
#include "osd/osd_perf_counters.h"
#include "common/Finisher.h"
#include "scrubber/osd_scrub.h"

#define CEPH_OSD_PROTOCOL    10 /* cluster internal */

/*

  lock ordering for pg map

    PG::lock
      ShardData::lock
        OSD::pg_map_lock

  */

class Messenger;
class Message;
class MonClient;
class ObjectStore;
class FuseStore;
class OSDMap;
class MLog;
class Objecter;
class KeyStore;

class Watch;
class PrimaryLogPG;

class TestOpsSocketHook;
struct C_FinishSplits;
struct C_OpenPGs;
class LogChannel;

class MOSDPGCreate2;
class MOSDPGNotify;
class MOSDPGInfo;
class MOSDPGRemove;
class MOSDForceRecovery;
class MMonGetPurgedSnapsReply;

class OSD;

class OSDService : public Scrub::ScrubSchedListener {
  using OpSchedulerItem = ceph::osd::scheduler::OpSchedulerItem;
public:
  OSD *osd;
  CephContext *cct;
  ObjectStore::CollectionHandle meta_ch;
  const int whoami;
  ObjectStore * const store;
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

  md_config_cacher_t<Option::size_t> osd_max_object_size;
  md_config_cacher_t<bool> osd_skip_data_digest;

  void enqueue_back(OpSchedulerItem&& qi);
  void enqueue_front(OpSchedulerItem&& qi);

  void maybe_inject_dispatch_delay() {
    if (g_conf()->osd_debug_inject_dispatch_delay_probability > 0) {
      if (rand() % 10000 <
	  g_conf()->osd_debug_inject_dispatch_delay_probability * 10000) {
	utime_t t;
	t.set_from_double(g_conf()->osd_debug_inject_dispatch_delay_duration);
	t.sleep();
      }
    }
  }

  ceph::signedspan get_mnow() const;

private:
  // -- superblock --
  ceph::mutex publish_lock, pre_publish_lock; // pre-publish orders before publish
  OSDSuperblock superblock;

public:
  OSDSuperblock get_superblock() {
    std::lock_guard l(publish_lock);
    return superblock;
  }
  void publish_superblock(const OSDSuperblock &block) {
    std::lock_guard l(publish_lock);
    superblock = block;
  }

  int get_nodeid() const final { return whoami; }
private:
  OSDMapRef osdmap;

public:
  OSDMapRef get_osdmap() {
    std::lock_guard l(publish_lock);
    return osdmap;
  }
  epoch_t get_osdmap_epoch() {
    std::lock_guard l(publish_lock);
    return osdmap ? osdmap->get_epoch() : 0;
  }
  void publish_map(OSDMapRef map) {
    std::lock_guard l(publish_lock);
    osdmap = map;
  }

  /*
   * osdmap - current published std::map
   * next_osdmap - pre_published std::map that is about to be published.
   *
   * We use the next_osdmap to send messages and initiate connections,
   * but only if the target is the same instance as the one in the std::map
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
  ceph::condition_variable pre_publish_cond;
  int pre_publish_waiter = 0;

public:
  void pre_publish_map(OSDMapRef map) {
    std::lock_guard l(pre_publish_lock);
    next_osdmap = std::move(map);
  }

  void activate_map();
  /// map epochs reserved below
  std::map<epoch_t, unsigned> map_reservations;

  /// gets ref to next_osdmap and registers the epoch as reserved
  OSDMapRef get_nextmap_reserved();
  /// releases reservation on map
  void release_map(OSDMapRef osdmap);
  /// blocks until there are no reserved maps prior to next_osdmap
  void await_reserved_maps() ;
  OSDMapRef get_next_osdmap() {
    std::lock_guard l(pre_publish_lock);
    return next_osdmap;
  }

  void maybe_share_map(Connection *con,
		       const OSDMapRef& osdmap,
		       epoch_t peer_epoch_lb=0);

  void send_incremental_map(epoch_t since, Connection *con,
			    const OSDMapRef& osdmap);
  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to,
                                       OSDSuperblock& superblock);

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch);
  std::pair<ConnectionRef,ConnectionRef> get_con_osd_hb(int peer, epoch_t from_epoch);  // (back, front)
  void send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch);
  void send_message_osd_cluster(std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch);
  void send_message_osd_cluster(MessageRef m, Connection *con) {
    con->send_message2(std::move(m));
  }
  void send_message_osd_cluster(Message *m, const ConnectionRef& con) {
    con->send_message(m);
  }
  void send_message_osd_client(Message *m, const ConnectionRef& con) {
    con->send_message(m);
  }
  entity_name_t get_cluster_msgr_name() const;


public:

  void reply_op_error(OpRequestRef op, int err);
  void reply_op_error(OpRequestRef op, int err, eversion_t v, version_t uv,
		      std::vector<pg_log_op_return_item_t> op_returns);
  void handle_misdirected_op(PG *pg, OpRequestRef op);

 private:
  /// the entity that offloads all scrubbing-related operations
  OsdScrub m_osd_scrub;

 public:
  OsdScrub& get_scrub_services() { return m_osd_scrub; }

  /**
   * locks the named PG, returning an RAII wrapper that unlocks upon
   * destruction.
   * returns nullopt if failing to lock.
   */
  std::optional<PGLockWrapper> get_locked_pg(spg_t pgid) final;

  /**
   * the entity that counts the number of active replica scrub
   * operations, and grant scrub reservation requests asynchronously.
   */
  AsyncReserver<spg_t, Finisher>& get_scrub_reserver() {
    return scrub_reserver;
  }

 private:
  // -- agent shared state --
  ceph::mutex agent_lock = ceph::make_mutex("OSDService::agent_lock");
  ceph::condition_variable agent_cond;
  std::map<uint64_t, std::set<PGRef> > agent_queue;
  std::set<PGRef>::iterator agent_queue_pos;
  bool agent_valid_iterator;
  int agent_ops;
  int flush_mode_high_count; //once have one pg with FLUSH_MODE_HIGH then flush objects with high speed
  std::set<hobject_t> agent_oids;
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
  ceph::mutex agent_timer_lock = ceph::make_mutex("OSDService::agent_timer_lock");
  SafeTimer agent_timer;

public:
  void agent_entry();
  void agent_stop();

  void _enqueue(PG *pg, uint64_t priority) {
    if (!agent_queue.empty() &&
	agent_queue.rbegin()->first < priority)
      agent_valid_iterator = false;  // inserting higher-priority queue
    std::set<PGRef>& nq = agent_queue[priority];
    if (nq.empty())
      agent_cond.notify_all();
    nq.insert(pg);
  }

  void _dequeue(PG *pg, uint64_t old_priority) {
    std::set<PGRef>& oq = agent_queue[old_priority];
    std::set<PGRef>::iterator p = oq.find(pg);
    ceph_assert(p != oq.end());
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
    std::lock_guard l(agent_lock);
    _enqueue(pg, priority);
  }

  /// adjust priority for an enagled pg
  void agent_adjust_pg(PG *pg, uint64_t old_priority, uint64_t new_priority) {
    std::lock_guard l(agent_lock);
    ceph_assert(new_priority != old_priority);
    _enqueue(pg, new_priority);
    _dequeue(pg, old_priority);
  }

  /// disable agent for a pg
  void agent_disable_pg(PG *pg, uint64_t old_priority) {
    std::lock_guard l(agent_lock);
    _dequeue(pg, old_priority);
  }

  /// note start of an async (evict) op
  void agent_start_evict_op() {
    std::lock_guard l(agent_lock);
    ++agent_ops;
  }

  /// note finish or cancellation of an async (evict) op
  void agent_finish_evict_op() {
    std::lock_guard l(agent_lock);
    ceph_assert(agent_ops > 0);
    --agent_ops;
    agent_cond.notify_all();
  }

  /// note start of an async (flush) op
  void agent_start_op(const hobject_t& oid) {
    std::lock_guard l(agent_lock);
    ++agent_ops;
    ceph_assert(agent_oids.count(oid) == 0);
    agent_oids.insert(oid);
  }

  /// note finish or cancellation of an async (flush) op
  void agent_finish_op(const hobject_t& oid) {
    std::lock_guard l(agent_lock);
    ceph_assert(agent_ops > 0);
    --agent_ops;
    ceph_assert(agent_oids.count(oid) == 1);
    agent_oids.erase(oid);
    agent_cond.notify_all();
  }

  /// check if we are operating on an object
  bool agent_is_active_oid(const hobject_t& oid) {
    std::lock_guard l(agent_lock);
    return agent_oids.count(oid);
  }

  /// get count of active agent ops
  int agent_get_num_ops() {
    std::lock_guard l(agent_lock);
    return agent_ops;
  }

  void agent_inc_high_count() {
    std::lock_guard l(agent_lock);
    flush_mode_high_count ++;
  }

  void agent_dec_high_count() {
    std::lock_guard l(agent_lock);
    flush_mode_high_count --;
  }

private:
  /// throttle promotion attempts
  std::atomic<unsigned int> promote_probability_millis{1000}; ///< probability thousands. one word.
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
  unsigned get_num_shards() const {
    return m_objecter_finishers;
  }
  Finisher* get_objecter_finisher(int shard) {
    return objecter_finishers[shard].get();
  }

  // -- Objecter, for tiering reads/writes from/to other OSDs --
  ceph::async::io_context_pool& poolctx;
  std::unique_ptr<Objecter> objecter;
  int m_objecter_finishers;
  std::vector<std::unique_ptr<Finisher>> objecter_finishers;

  // -- Watch --
  ceph::mutex watch_lock = ceph::make_mutex("OSDService::watch_lock");
  SafeTimer watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    std::lock_guard l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }

  // -- Recovery/Backfill Request Scheduling --
  ceph::mutex recovery_request_lock = ceph::make_mutex("OSDService::recovery_request_lock");
  SafeTimer recovery_request_timer;

  // For async recovery sleep
  bool recovery_needs_sleep = true;
  ceph::real_clock::time_point recovery_schedule_time;

  // For recovery & scrub & snap
  ceph::mutex sleep_lock = ceph::make_mutex("OSDService::sleep_lock");
  SafeTimer sleep_timer;

  // -- tids --
  // for ops i issue
  std::atomic<unsigned int> last_tid{0};
  ceph_tid_t get_tid() {
    return (ceph_tid_t)last_tid++;
  }

  // -- backfill_reservation --
  Finisher reserver_finisher;
  AsyncReserver<spg_t, Finisher> local_reserver;
  AsyncReserver<spg_t, Finisher> remote_reserver;

  // -- pg merge --
  ceph::mutex merge_lock = ceph::make_mutex("OSD::merge_lock");
  std::map<pg_t,eversion_t> ready_to_merge_source;   // pg -> version
  std::map<pg_t,std::tuple<eversion_t,epoch_t,epoch_t>> ready_to_merge_target;  // pg -> (version,les,lec)
  std::set<pg_t> not_ready_to_merge_source;
  std::map<pg_t,pg_t> not_ready_to_merge_target;
  std::set<pg_t> sent_ready_to_merge_source;

  void set_ready_to_merge_source(PG *pg,
				 eversion_t version);
  void set_ready_to_merge_target(PG *pg,
				 eversion_t version,
				 epoch_t last_epoch_started,
				 epoch_t last_epoch_clean);
  void set_not_ready_to_merge_source(pg_t source);
  void set_not_ready_to_merge_target(pg_t target, pg_t source);
  void clear_ready_to_merge(PG *pg);
  void send_ready_to_merge();
  void _send_ready_to_merge();
  void clear_sent_ready_to_merge();
  void prune_sent_ready_to_merge(const OSDMapRef& osdmap);

  // -- pg_temp --
private:
  ceph::mutex pg_temp_lock = ceph::make_mutex("OSDService::pg_temp_lock");
  struct pg_temp_t {
    std::vector<int> acting;
    bool forced = false;
  };
  std::map<pg_t, pg_temp_t> pg_temp_wanted;
  std::map<pg_t, pg_temp_t> pg_temp_pending;
  void _sent_pg_temp();
  friend std::ostream& operator<<(std::ostream&, const pg_temp_t&);
public:
  void queue_want_pg_temp(pg_t pgid, const std::vector<int>& want,
			  bool forced = false);
  void remove_want_pg_temp(pg_t pgid);
  void requeue_pg_temp();
  void send_pg_temp();

  ceph::mutex pg_created_lock = ceph::make_mutex("OSDService::pg_created_lock");
  std::set<pg_t> pg_created;
  void send_pg_created(pg_t pgid);
  void prune_pg_created();
  void send_pg_created();

  AsyncReserver<spg_t, Finisher> snap_reserver;
  /// keeping track of replicas being reserved for scrubbing
  AsyncReserver<spg_t, Finisher> scrub_reserver;
  void queue_recovery_context(PG *pg,
                              GenContext<ThreadPool::TPHandle&> *c,
                              uint64_t cost,
			      int priority);
  void queue_for_snap_trim(PG *pg, uint64_t cost);
  void queue_for_scrub(PG* pg, Scrub::scrub_prio_t with_priority);

  void queue_scrub_after_repair(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals either (a) the end of a sleep period, or (b) a recheck of the availability
  /// of the primary map being created by the backend.
  void queue_for_scrub_resched(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals a change in the number of in-flight recovery writes
  void queue_scrub_pushes_update(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals that all pending updates were applied
  void queue_scrub_applied_update(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals that the selected chunk (objects range) is available for scrubbing
  void queue_scrub_chunk_free(PG* pg, Scrub::scrub_prio_t with_priority);

  /// The chunk selected is blocked by user operations, and cannot be scrubbed now
  void queue_scrub_chunk_busy(PG* pg, Scrub::scrub_prio_t with_priority);

  /// The block-range that was locked and prevented the scrubbing - is freed
  void queue_scrub_unblocking(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals that all write OPs are done
  void queue_scrub_digest_update(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals that we (the Primary) got all waited-for scrub-maps from our replicas
  void queue_scrub_got_repl_maps(PG* pg, Scrub::scrub_prio_t with_priority);

  /// Signals that all chunks were handled
  /// Note: always with high priority, as must be acted upon before the
  /// next scrub request arrives from the Primary (and the primary is free
  /// to send the request once the replica's map is received).
  void queue_scrub_is_finished(PG* pg);

  /// Signals that there are more chunks to handle
  void queue_scrub_next_chunk(PG* pg, Scrub::scrub_prio_t with_priority);

  void queue_for_rep_scrub(PG* pg,
			   Scrub::scrub_prio_t with_high_priority,
			   unsigned int qu_priority,
			   Scrub::act_token_t act_token);

  /// Signals a change in the number of in-flight recovery writes
  void queue_scrub_replica_pushes(PG *pg, Scrub::scrub_prio_t with_priority);

  /// (not in Crimson) Queue a SchedReplica event to be sent to the replica, to
  /// trigger a re-check of the availability of the scrub map prepared by the
  /// backend.
  void queue_for_rep_scrub_resched(PG* pg,
				   Scrub::scrub_prio_t with_high_priority,
				   unsigned int qu_priority,
				   Scrub::act_token_t act_token);

  void queue_for_pg_delete(spg_t pgid, epoch_t e, int64_t num_objects);
  bool try_finish_pg_delete(PG *pg, unsigned old_pg_num);

private:
  // -- pg recovery and associated throttling --
  ceph::mutex recovery_lock = ceph::make_mutex("OSDService::recovery_lock");

  struct pg_awaiting_throttle_t {
    const epoch_t epoch_queued;
    PGRef pg;
    const uint64_t cost_per_object;
    const int priority;
  };
  std::list<pg_awaiting_throttle_t> awaiting_throttle;

  /// queue a scrub-related message for a PG
  template <class MSG_TYPE>
  void queue_scrub_event_msg(PG* pg,
			     Scrub::scrub_prio_t with_priority,
			     unsigned int qu_priority,
			     Scrub::act_token_t act_token);

  /// An alternative version of queue_scrub_event_msg(), in which the queuing priority is
  /// provided by the executing scrub (i.e. taken from PgScrubber::m_flags)
  template <class MSG_TYPE>
  void queue_scrub_event_msg(PG* pg, Scrub::scrub_prio_t with_priority);
  int64_t get_scrub_cost();

  utime_t defer_recovery_until;
  uint64_t recovery_ops_active;
  uint64_t recovery_ops_reserved;
  bool recovery_paused;
#ifdef DEBUG_RECOVERY_OIDS
  std::map<spg_t, std::set<hobject_t> > recovery_oids;
#endif
  bool _recover_now(uint64_t *available_pushes);
  void _maybe_queue_recovery();
  void _queue_for_recovery(pg_awaiting_throttle_t p, uint64_t reserved_pushes);
public:
  void start_recovery_op(PG *pg, const hobject_t& soid);
  void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue);
  bool is_recovery_active();
  void release_reserved_pushes(uint64_t pushes);
  void defer_recovery(float defer_for) {
    defer_recovery_until = ceph_clock_now();
    defer_recovery_until += defer_for;
  }
  void pause_recovery() {
    std::lock_guard l(recovery_lock);
    recovery_paused = true;
  }
  bool recovery_is_paused() {
    std::lock_guard l(recovery_lock);
    return recovery_paused;
  }
  void unpause_recovery() {
    std::lock_guard l(recovery_lock);
    recovery_paused = false;
    _maybe_queue_recovery();
  }
  void kick_recovery_queue() {
    std::lock_guard l(recovery_lock);
    _maybe_queue_recovery();
  }
  void clear_queued_recovery(PG *pg) {
    std::lock_guard l(recovery_lock);
    awaiting_throttle.remove_if(
      [pg](decltype(awaiting_throttle)::const_reference awaiting ) {
        return awaiting.pg.get() == pg;
      });
  }

  unsigned get_target_pg_log_entries() const;

  // delayed pg activation
  void queue_for_recovery(
    PG *pg, uint64_t cost_per_object,
    int priority) {
    std::lock_guard l(recovery_lock);

    if (pg->is_forced_recovery_or_backfill()) {
      awaiting_throttle.emplace_front(
        pg_awaiting_throttle_t{
          pg->get_osdmap()->get_epoch(), pg, cost_per_object, priority});
    } else {
      awaiting_throttle.emplace_back(
        pg_awaiting_throttle_t{
          pg->get_osdmap()->get_epoch(), pg, cost_per_object, priority});
    }
    _maybe_queue_recovery();
  }
  void queue_recovery_after_sleep(
    PG *pg, epoch_t queued, uint64_t reserved_pushes,
    int priority) {
    std::lock_guard l(recovery_lock);
    // Send cost as 1 in pg_awaiting_throttle_t below. The cost is ignored
    // as this path is only applicable for WeightedPriorityQueue scheduler.
    _queue_for_recovery(
      pg_awaiting_throttle_t{queued, pg, 1, priority},
      reserved_pushes);
  }

  void queue_check_readable(spg_t spgid,
			    epoch_t lpr,
			    ceph::signedspan delay = ceph::signedspan::zero());

  // osd map cache (past osd maps)
  ceph::mutex map_cache_lock = ceph::make_mutex("OSDService::map_cache_lock");
  SharedLRU<epoch_t, const OSDMap> map_cache;
  SimpleLRU<epoch_t, ceph::buffer::list> map_bl_cache;
  SimpleLRU<epoch_t, ceph::buffer::list> map_bl_inc_cache;

  OSDMapRef try_get_map(epoch_t e);
  OSDMapRef get_map(epoch_t e) {
    OSDMapRef ret(try_get_map(e));
    ceph_assert(ret);
    return ret;
  }
  OSDMapRef add_map(OSDMap *o) {
    std::lock_guard l(map_cache_lock);
    return _add_map(o);
  }
  OSDMapRef _add_map(OSDMap *o);

  void _add_map_bl(epoch_t e, ceph::buffer::list& bl);
  bool get_map_bl(epoch_t e, ceph::buffer::list& bl) {
    std::lock_guard l(map_cache_lock);
    return _get_map_bl(e, bl);
  }
  bool _get_map_bl(epoch_t e, ceph::buffer::list& bl);

  void _add_map_inc_bl(epoch_t e, ceph::buffer::list& bl);
  bool get_inc_map_bl(epoch_t e, ceph::buffer::list& bl);

  /// identify split child pgids over a osdmap interval
  void identify_splits_and_merges(
    OSDMapRef old_map,
    OSDMapRef new_map,
    spg_t pgid,
    std::set<std::pair<spg_t,epoch_t>> *new_children,
    std::set<std::pair<spg_t,epoch_t>> *merge_pgs);

  void need_heartbeat_peer_update();

  void init();
  void final_init();
  void start_shutdown();
  void shutdown_reserver();
  void shutdown();

  // -- stats --
  ceph::mutex stat_lock = ceph::make_mutex("OSDService::stat_lock");
  osd_stat_t osd_stat;
  uint32_t seq = 0;

  void set_statfs(const struct store_statfs_t &stbuf,
    osd_alert_list_t& alerts);
  osd_stat_t set_osd_stat(std::vector<int>& hb_peers, int num_pgs);
  void inc_osd_stat_repaired(void);
  float compute_adjusted_ratio(osd_stat_t new_stat, float *pratio, uint64_t adjust_used = 0);
  osd_stat_t get_osd_stat() {
    std::lock_guard l(stat_lock);
    ++seq;
    osd_stat.up_from = up_epoch;
    osd_stat.seq = ((uint64_t)osd_stat.up_from << 32) + seq;
    return osd_stat;
  }
  uint64_t get_osd_stat_seq() {
    std::lock_guard l(stat_lock);
    return osd_stat.seq;
  }
  void get_hb_pingtime(std::map<int, osd_stat_t::Interfaces> *pp)
  {
    std::lock_guard l(stat_lock);
    *pp = osd_stat.hb_pingtime;
    return;
  }

  // -- OSD Full Status --
private:
  friend TestOpsSocketHook;
  mutable ceph::mutex full_status_lock = ceph::make_mutex("OSDService::full_status_lock");
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
  s_names get_full_state(std::string type) const {
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
  double cur_ratio, physical_ratio;  ///< current utilization
  mutable int64_t injectfull = 0;
  s_names injectfull_state = NONE;
  float get_failsafe_full_ratio();
  bool _check_inject_full(DoutPrefixProvider *dpp, s_names type) const;
  bool _check_full(DoutPrefixProvider *dpp, s_names type) const;
public:
  void check_full_status(float ratio, float pratio);
  s_names recalc_full_state(float ratio, float pratio, std::string &inject);
  bool _tentative_full(DoutPrefixProvider *dpp, s_names type, uint64_t adjust_used, osd_stat_t);
  bool check_failsafe_full(DoutPrefixProvider *dpp) const;
  bool check_full(DoutPrefixProvider *dpp) const;
  bool tentative_backfill_full(DoutPrefixProvider *dpp, uint64_t adjust_used, osd_stat_t);
  bool check_backfill_full(DoutPrefixProvider *dpp) const;
  bool check_nearfull(DoutPrefixProvider *dpp) const;
  bool is_failsafe_full() const;
  bool is_full() const;
  bool is_backfillfull() const;
  bool is_nearfull() const;
  bool need_fullness_update();  ///< osdmap state needs update
  void set_injectfull(s_names type, int64_t count);


  // -- epochs --
private:
  // protects access to boot_epoch, up_epoch, bind_epoch
  mutable ceph::mutex epoch_lock = ceph::make_mutex("OSDService::epoch_lock");
  epoch_t boot_epoch;  // _first_ epoch we were marked up (after this process started)
  epoch_t up_epoch;    // _most_recent_ epoch we were marked up
  epoch_t bind_epoch;  // epoch we last did a bind to new ip:ports
public:
  /**
   * Retrieve the boot_, up_, and bind_ epochs the OSD has std::set. The params
   * can be NULL if you don't care about them.
   */
  void retrieve_epochs(epoch_t *_boot_epoch, epoch_t *_up_epoch,
                       epoch_t *_bind_epoch) const;
  /**
   * Std::set the boot, up, and bind epochs. Any NULL params will not be std::set.
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

  // -- heartbeats --
  ceph::mutex hb_stamp_lock = ceph::make_mutex("OSDServce::hb_stamp_lock");

  /// osd -> heartbeat stamps
  std::vector<HeartbeatStampsRef> hb_stamps;

  /// get or create a ref for a peer's HeartbeatStamps
  HeartbeatStampsRef get_hb_stamps(unsigned osd);


  // Timer for readable leases
  ceph::timer<ceph::mono_clock> mono_timer = ceph::timer<ceph::mono_clock>{ceph::construct_suspended};

  void queue_renew_lease(epoch_t epoch, spg_t spgid);

  // -- stopping --
  ceph::mutex is_stopping_lock = ceph::make_mutex("OSDService::is_stopping_lock");
  ceph::condition_variable is_stopping_cond;
  enum {
    NOT_STOPPING,
    PREPARING_TO_STOP,
    STOPPING };
  std::atomic<int> state{NOT_STOPPING};
  int get_state() const {
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
  ceph::mutex pgid_lock = ceph::make_mutex("OSDService::pgid_lock");
  std::map<spg_t, int> pgid_tracker;
  std::map<spg_t, PG*> live_pgs;
  void add_pgid(spg_t pgid, PG *pg);
  void remove_pgid(spg_t pgid, PG *pg);
  void dump_live_pgids();
#endif

  explicit OSDService(OSD *osd, ceph::async::io_context_pool& poolctx);
  ~OSDService() = default;
};

/*

  Each PG slot includes queues for events that are processing and/or waiting
  for a PG to be materialized in the slot.

  These are the constraints:

  - client ops must remained ordered by client, regardless of std::map epoch
  - peering messages/events from peers must remain ordered by peer
  - peering messages and client ops need not be ordered relative to each other

  - some peering events can create a pg (e.g., notify)
  - the query peering event can proceed when a PG doesn't exist

  Implementation notes:

  - everybody waits for split.  If the OSD has the parent PG it will instantiate
    the PGSlot early and mark it waiting_for_split.  Everything will wait until
    the parent is able to commit the split operation and the child PG's are
    materialized in the child slots.

  - every event has an epoch property and will wait for the OSDShard to catch
    up to that epoch.  For example, if we get a peering event from a future
    epoch, the event will wait in the slot until the local OSD has caught up.
    (We should be judicious in specifying the required epoch [by, e.g., setting
    it to the same_interval_since epoch] so that we don't wait for epochs that
    don't affect the given PG.)

  - we maintain two separate wait lists, *waiting* and *waiting_peering*. The
    OpSchedulerItem has an is_peering() bool to determine which we use.  Waiting
    peering events are queued up by epoch required.

  - when we wake a PG slot (e.g., we finished split, or got a newer osdmap, or
    materialized the PG), we wake *all* waiting items.  (This could be optimized,
    probably, but we don't bother.)  We always requeue peering items ahead of
    client ops.

  - some peering events are marked !peering_requires_pg (PGQuery).  if we do
    not have a PG these are processed immediately (under the shard lock).

  - we do not have a PG present, we check if the slot maps to the current host.
    if so, we either queue the item and wait for the PG to materialize, or
    (if the event is a pg creating event like PGNotify), we materialize the PG.

  - when we advance the osdmap on the OSDShard, we scan pg slots and
    discard any slots with no pg (and not waiting_for_split) that no
    longer std::map to the current host.

  */

struct OSDShardPGSlot {
  using OpSchedulerItem = ceph::osd::scheduler::OpSchedulerItem;
  PGRef pg;                      ///< pg reference
  std::deque<OpSchedulerItem> to_process; ///< order items for this slot
  int num_running = 0;          ///< _process threads doing pg lookup/lock

  std::deque<OpSchedulerItem> waiting;   ///< waiting for pg (or map + pg)

  /// waiting for map (peering evt)
  std::map<epoch_t,std::deque<OpSchedulerItem>> waiting_peering;

  /// incremented by wake_pg_waiters; indicates racing _process threads
  /// should bail out (their op has been requeued)
  uint64_t requeue_seq = 0;

  /// waiting for split child to materialize in these epoch(s)
  std::set<epoch_t> waiting_for_split;

  epoch_t epoch = 0;
  boost::intrusive::set_member_hook<> pg_epoch_item;

  /// waiting for a merge (source or target) by this epoch
  epoch_t waiting_for_merge_epoch = 0;
};

struct OSDShard {
  const unsigned shard_id;
  CephContext *cct;
  OSD *osd;

  std::string shard_name;

  std::string sdata_wait_lock_name;
  ceph::mutex sdata_wait_lock;
  ceph::condition_variable sdata_cond;
  int waiting_threads = 0;

  ceph::mutex osdmap_lock;  ///< protect shard_osdmap updates vs users w/o shard_lock
  OSDMapRef shard_osdmap;

  OSDMapRef get_osdmap() {
    std::lock_guard l(osdmap_lock);
    return shard_osdmap;
  }

  std::string shard_lock_name;
  ceph::mutex shard_lock;   ///< protects remaining members below

  /// map of slots for each spg_t.  maintains ordering of items dequeued
  /// from scheduler while _process thread drops shard lock to acquire the
  /// pg lock.  stale slots are removed by consume_map.
  std::unordered_map<spg_t,std::unique_ptr<OSDShardPGSlot>> pg_slots;

  struct pg_slot_compare_by_epoch {
    bool operator()(const OSDShardPGSlot& l, const OSDShardPGSlot& r) const {
      return l.epoch < r.epoch;
    }
  };

  /// maintain an ordering of pg slots by pg epoch
  boost::intrusive::multiset<
    OSDShardPGSlot,
    boost::intrusive::member_hook<
      OSDShardPGSlot,
      boost::intrusive::set_member_hook<>,
      &OSDShardPGSlot::pg_epoch_item>,
    boost::intrusive::compare<pg_slot_compare_by_epoch>> pg_slots_by_epoch;
  int waiting_for_min_pg_epoch = 0;
  ceph::condition_variable min_pg_epoch_cond;

  /// priority queue
  ceph::osd::scheduler::OpSchedulerRef scheduler;

  bool stop_waiting = false;

  ContextQueue context_queue;

  void _attach_pg(OSDShardPGSlot *slot, PG *pg);
  void _detach_pg(OSDShardPGSlot *slot);

  void update_pg_epoch(OSDShardPGSlot *slot, epoch_t epoch);
  epoch_t get_min_pg_epoch();
  void wait_min_pg_epoch(epoch_t need);

  /// return newest epoch we are waiting for
  epoch_t get_max_waiting_epoch();

  /// push osdmap into shard
  void consume_map(
    const OSDMapRef& osdmap,
    unsigned *pushes_to_free);

  int _wake_pg_slot(spg_t pgid, OSDShardPGSlot *slot);

  void identify_splits_and_merges(
    const OSDMapRef& as_of_osdmap,
    std::set<std::pair<spg_t,epoch_t>> *split_children,
    std::set<std::pair<spg_t,epoch_t>> *merge_pgs);
  void _prime_splits(std::set<std::pair<spg_t,epoch_t>> *pgids);
  void prime_splits(const OSDMapRef& as_of_osdmap,
		    std::set<std::pair<spg_t,epoch_t>> *pgids);
  void prime_merges(const OSDMapRef& as_of_osdmap,
		    std::set<std::pair<spg_t,epoch_t>> *merge_pgs);
  void register_and_wake_split_child(PG *pg);
  void unprime_split_children(spg_t parent, unsigned old_pg_num);
  void update_scheduler_config();
  op_queue_type_t get_op_queue_type() const;

  OSDShard(
    int id,
    CephContext *cct,
    OSD *osd,
    op_queue_type_t osd_op_queue,
    unsigned osd_op_queue_cut_off);
};

class OSD : public Dispatcher,
	    public md_config_obs_t {
  using OpSchedulerItem = ceph::osd::scheduler::OpSchedulerItem;

  /** OSD **/
  // global lock
  ceph::mutex osd_lock = ceph::make_mutex("OSD::osd_lock");
  SafeTimer tick_timer;    // safe timer (osd_lock)

  // Tick timer for those stuff that do not need osd_lock
  ceph::mutex tick_timer_lock = ceph::make_mutex("OSD::tick_timer_lock");
  SafeTimer tick_timer_without_osd_lock;
  std::string gss_ktfile_client{};

public:
  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;
  void update_log_config();
  void check_config();

protected:

  const double OSD_TICK_INTERVAL = { 1.0 };
  double get_tick_interval() const;

  Messenger   *cluster_messenger;
  Messenger   *client_messenger;
  Messenger   *objecter_messenger;
  MonClient   *monc; // check the "monc helpers" list before accessing directly
  MgrClient   mgrc;
  PerfCounters      *logger;
  PerfCounters      *recoverystate_perf;
  std::unique_ptr<ObjectStore> store;
#ifdef HAVE_LIBFUSE
  FuseStore *fuse_store = nullptr;
#endif
  LogClient log_client;
  LogChannelRef clog;

  int whoami;
  std::string dev_path, journal_path;

  ceph_release_t last_require_osd_release{ceph_release_t::unknown};

  int numa_node = -1;
  size_t numa_cpu_set_size = 0;
  cpu_set_t numa_cpu_set;

  bool store_is_rotational = true;
  bool journal_is_rotational = true;

  ZTracer::Endpoint trace_endpoint;
  PerfCounters* create_logger();
  PerfCounters* create_recoverystate_perf();
  void tick();
  void tick_without_osd_lock();
  void _dispatch(Message *m);

  void check_osdmap_features();

  // asok
  friend class OSDSocketHook;
  class OSDSocketHook *asok_hook;
  using PGRefOrError = std::tuple<std::optional<PGRef>, int>;
    PGRefOrError locate_asok_target(const cmdmap_t& cmdmap,
				    std::stringstream& ss, bool only_primary);
  int asok_route_to_pg(bool only_primary,
    std::string_view prefix,
    cmdmap_t cmdmap,
    Formatter *f,
    std::stringstream& ss,
    const bufferlist& inbl,
    bufferlist& outbl,
    std::function<void(int, const std::string&, bufferlist&)> on_finish);
  void asok_command(
    std::string_view prefix,
    const cmdmap_t& cmdmap,
    ceph::Formatter *f,
    const ceph::buffer::list& inbl,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_finish);

public:
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
  static ghobject_t make_purged_snaps_oid() {
    return ghobject_t(hobject_t(
      sobject_t(
	object_t("purged_snaps"),
	0)));
  }

  static ghobject_t make_final_pool_info_oid(int64_t pool) {
    return ghobject_t(
      hobject_t(
	sobject_t(
	  object_t(std::string("final_pool_") + stringify(pool)),
	  CEPH_NOSNAP)));
  }

  static ghobject_t make_pg_num_history_oid() {
    return ghobject_t(hobject_t(sobject_t("pg_num_history", CEPH_NOSNAP)));
  }

  static void recursive_remove_collection(CephContext* cct,
					  ObjectStore *store,
					  spg_t pgid,
					  coll_t tmp);

  /**
   * get_osd_initial_compat_set()
   *
   * Get the initial feature std::set for this OSD.  Features
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

  // -- config settings --
  float m_osd_pg_epoch_max_lag_factor;

  // -- superblock --
  OSDSuperblock superblock;

  static void write_superblock(CephContext* cct,
                               OSDSuperblock& sb,
                               ObjectStore::Transaction& t);
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
  std::atomic<int> state{STATE_INITIALIZING};

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

  ShardedThreadPool osd_op_tp;

  void get_latest_osdmap();

  // -- sessions --
private:
  void dispatch_session_waiting(const ceph::ref_t<Session>& session, OSDMapRef osdmap);

  ceph::mutex session_waiting_lock = ceph::make_mutex("OSD::session_waiting_lock");
  std::set<ceph::ref_t<Session>> session_waiting_for_map;

  /// Caller assumes refs for included Sessions
  void get_sessions_waiting_for_map(std::set<ceph::ref_t<Session>> *out) {
    std::lock_guard l(session_waiting_lock);
    out->swap(session_waiting_for_map);
  }
  void register_session_waiting_on_map(const ceph::ref_t<Session>& session) {
    std::lock_guard l(session_waiting_lock);
    session_waiting_for_map.insert(session);
  }
  void clear_session_waiting_on_map(const ceph::ref_t<Session>& session) {
    std::lock_guard l(session_waiting_lock);
    session_waiting_for_map.erase(session);
  }
  void dispatch_sessions_waiting_on_map() {
    std::set<ceph::ref_t<Session>> sessions_to_check;
    get_sessions_waiting_for_map(&sessions_to_check);
    for (auto i = sessions_to_check.begin();
	 i != sessions_to_check.end();
	 sessions_to_check.erase(i++)) {
      std::lock_guard l{(*i)->session_dispatch_lock};
      dispatch_session_waiting(*i, get_osdmap());
    }
  }
  void session_handle_reset(const ceph::ref_t<Session>& session) {
    std::lock_guard l(session->session_dispatch_lock);
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

  ceph::mutex osdmap_subscribe_lock = ceph::make_mutex("OSD::osdmap_subscribe_lock");
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
    static constexpr int HEARTBEAT_MAX_CONN = 2;
    /// history of inflight pings, arranging by timestamp we sent
    /// send time -> deadline -> remaining replies
    std::map<utime_t, std::pair<utime_t, int>> ping_history;

    utime_t hb_interval_start;
    uint32_t hb_average_count = 0;
    uint32_t hb_index = 0;

    uint32_t hb_total_back = 0;
    uint32_t hb_min_back = UINT_MAX;
    uint32_t hb_max_back = 0;
    std::vector<uint32_t> hb_back_pingtime;
    std::vector<uint32_t> hb_back_min;
    std::vector<uint32_t> hb_back_max;

    uint32_t hb_total_front = 0;
    uint32_t hb_min_front = UINT_MAX;
    uint32_t hb_max_front = 0;
    std::vector<uint32_t> hb_front_pingtime;
    std::vector<uint32_t> hb_front_min;
    std::vector<uint32_t> hb_front_max;

    bool is_stale(utime_t stale) const {
      if (ping_history.empty()) {
        return false;
      }
      utime_t oldest_deadline = ping_history.begin()->second.first;
      return oldest_deadline <= stale;
    }

    bool is_unhealthy(utime_t now) const {
      if (ping_history.empty()) {
        /// we haven't sent a ping yet or we have got all replies,
        /// in either way we are safe and healthy for now
        return false;
      }

      utime_t oldest_deadline = ping_history.begin()->second.first;
      return now > oldest_deadline;
    }

    bool is_healthy(utime_t now) const {
      if (last_rx_front == utime_t() || last_rx_back == utime_t()) {
        // only declare to be healthy until we have received the first
        // replies from both front/back connections
        return false;
      }
      return !is_unhealthy(now);
    }

    void clear_mark_down(Connection *except = nullptr) {
      if (con_back && con_back != except) {
	con_back->mark_down();
	con_back->clear_priv();
	con_back.reset(nullptr);
      }
      if (con_front && con_front != except) {
	con_front->mark_down();
	con_front->clear_priv();
	con_front.reset(nullptr);
      }
    }
  };

  ceph::mutex heartbeat_lock = ceph::make_mutex("OSD::heartbeat_lock");
  std::map<int, int> debug_heartbeat_drops_remaining;
  ceph::condition_variable heartbeat_cond;
  bool heartbeat_stop;
  std::atomic<bool> heartbeat_need_update;
  std::map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  utime_t last_mon_heartbeat;
  Messenger *hb_front_client_messenger;
  Messenger *hb_back_client_messenger;
  Messenger *hb_front_server_messenger;
  Messenger *hb_back_server_messenger;
  utime_t last_heartbeat_resample;   ///< last time we chose random peers in waiting-for-healthy state
  double daily_loadavg;
  ceph::mono_time startup_time;

  // Track ping repsonse times using vector as a circular buffer
  // MUST BE A POWER OF 2
  const uint32_t hb_vector_size = 16;

  void _add_heartbeat_peer(int p);
  void _remove_heartbeat_peer(int p);
  bool heartbeat_reset(Connection *con);
  void maybe_update_heartbeat_peers();
  void reset_heartbeat_peers(bool all);
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
    std::lock_guard l(heartbeat_lock);
    heartbeat_cond.notify_all();
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
    int ms_handle_fast_authentication(Connection *con) override {
      return true;
    }
  } heartbeat_dispatcher;

private:
  // -- op tracking --
  OpTracker op_tracker;
  void test_ops(std::string command, std::string args, std::ostream& ss);
  friend class TestOpsSocketHook;
  TestOpsSocketHook *test_ops_hook;
  friend struct C_FinishSplits;
  friend struct C_OpenPGs;

protected:

  /*
   * The ordered op delivery chain is:
   *
   *   fast dispatch -> scheduler back
   *                    scheduler front <-> to_process back
   *                                     to_process front  -> RunVis(item)
   *                                                      <- queue_front()
   *
   * The scheduler is per-shard, and to_process is per pg_slot.  Items can be
   * pushed back up into to_process and/or scheduler while order is preserved.
   *
   * Multiple worker threads can operate on each shard.
   *
   * Under normal circumstances, num_running == to_process.size().  There are
   * two times when that is not true: (1) when waiting_for_pg == true and
   * to_process is accumulating requests that are waiting for the pg to be
   * instantiated; in that case they will all get requeued together by
   * wake_pg_waiters, and (2) when wake_pg_waiters just ran, waiting_for_pg
   * and already requeued the items.
   */
  friend class ceph::osd::scheduler::PGOpItem;
  friend class ceph::osd::scheduler::PGPeeringItem;
  friend class ceph::osd::scheduler::PGRecovery;
  friend class ceph::osd::scheduler::PGRecoveryContext;
  friend class ceph::osd::scheduler::PGRecoveryMsg;
  friend class ceph::osd::scheduler::PGDelete;

  class ShardedOpWQ
    : public ShardedThreadPool::ShardedWQ<OpSchedulerItem>
  {
    OSD *osd;
    bool m_fast_shutdown = false;
  public:
    ShardedOpWQ(OSD *o,
		ceph::timespan ti,
		ceph::timespan si,
		ShardedThreadPool* tp)
      : ShardedThreadPool::ShardedWQ<OpSchedulerItem>(ti, si, tp),
        osd(o) {
    }

    void _add_slot_waiter(
      spg_t token,
      OSDShardPGSlot *slot,
      OpSchedulerItem&& qi);

    /// try to do some work
    void _process(uint32_t thread_index, ceph::heartbeat_handle_d *hb) override;

    void stop_for_fast_shutdown();

    /// enqueue a new item
    void _enqueue(OpSchedulerItem&& item) override;

    /// requeue an old item (at the front of the line)
    void _enqueue_front(OpSchedulerItem&& item) override;

    void return_waiting_threads() override {
      for(uint32_t i = 0; i < osd->num_shards; i++) {
	OSDShard* sdata = osd->shards[i];
	assert (NULL != sdata);
	std::scoped_lock l{sdata->sdata_wait_lock};
	sdata->stop_waiting = true;
	sdata->sdata_cond.notify_all();
      }
    }

    void stop_return_waiting_threads() override {
      for(uint32_t i = 0; i < osd->num_shards; i++) {
	OSDShard* sdata = osd->shards[i];
	assert (NULL != sdata);
	std::scoped_lock l{sdata->sdata_wait_lock};
	sdata->stop_waiting = false;
      }
    }

    void dump(ceph::Formatter *f) {
      for(uint32_t i = 0; i < osd->num_shards; i++) {
	auto &&sdata = osd->shards[i];

	char queue_name[32] = {0};
	snprintf(queue_name, sizeof(queue_name), "%s%" PRIu32, "OSD:ShardedOpWQ:", i);
	ceph_assert(NULL != sdata);

	std::scoped_lock l{sdata->shard_lock};
	f->open_object_section(queue_name);
	sdata->scheduler->dump(*f);
	f->close_section();
      }
    }

    bool is_shard_empty(uint32_t thread_index) override {
      uint32_t shard_index = thread_index % osd->num_shards;
      auto &&sdata = osd->shards[shard_index];
      ceph_assert(sdata);
      std::lock_guard l(sdata->shard_lock);
      if (thread_index < osd->num_shards) {
	return sdata->scheduler->empty() && sdata->context_queue.empty();
      } else {
	return sdata->scheduler->empty();
      }
    }

    void handle_oncommits(std::list<Context*>& oncommits) {
      for (auto p : oncommits) {
	p->complete(0);
      }
    }
  } op_shardedwq;


  void enqueue_op(spg_t pg, OpRequestRef&& op, epoch_t epoch);
  void dequeue_op(
    PGRef pg, OpRequestRef op,
    ThreadPool::TPHandle &handle);

  void enqueue_peering_evt(
    spg_t pgid,
    PGPeeringEventRef ref);
  void dequeue_peering_evt(
    OSDShard *sdata,
    PG *pg,
    PGPeeringEventRef ref,
    ThreadPool::TPHandle& handle);

  void dequeue_delete(
    OSDShard *sdata,
    PG *pg,
    epoch_t epoch,
    ThreadPool::TPHandle& handle);

  friend class PG;
  friend struct OSDShard;
  friend class PrimaryLogPG;
  friend class PgScrubber;


 protected:

  // -- osd map --
  // TODO: switch to std::atomic<OSDMapRef> when C++20 will be available.
  OSDMapRef       _osdmap;
  void set_osdmap(OSDMapRef osdmap) {
    std::atomic_store(&_osdmap, osdmap);
  }
  OSDMapRef get_osdmap() const {
    return std::atomic_load(&_osdmap);
  }
  epoch_t get_osdmap_epoch() const {
    // XXX: performance?
    auto osdmap = get_osdmap();
    return osdmap ? osdmap->get_epoch() : 0;
  }

  pool_pg_num_history_t pg_num_history;

  ceph::shared_mutex map_lock = ceph::make_shared_mutex("OSD::map_lock");
  std::deque<utime_t> osd_markdown_log;

  friend struct send_map_on_destruct;

  void handle_osd_map(class MOSDMap *m);
  void track_pools_and_pg_num_changes(const std::map<epoch_t,OSDMapRef>& added_maps,
                                      ObjectStore::Transaction& t,
                                      epoch_t last);
  void _committed_osd_maps(epoch_t first, epoch_t last, class MOSDMap *m);
  void trim_maps(epoch_t oldest);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  friend struct C_OnMapCommit;

  std::optional<epoch_t> get_epoch_from_osdmap_object(const ghobject_t& osdmap);
  /**
   * trim_stale_maps
   *
   * trim_maps had a possible (rare) leak which resulted in stale osdmaps.
   * This method will cleanup any existing osdmap from the store
   * in the range of 0 up to the superblock's oldest_map.
   * @return number of stale osdmaps which were removed.
   * See: https://tracker.ceph.com/issues/61962
   */
  int trim_stale_maps();

  bool advance_pg(
    epoch_t advance_to,
    PG *pg,
    ThreadPool::TPHandle &handle,
    PeeringCtx &rctx);
  void consume_map();
  void activate_map();

  // osd map cache (past osd maps)
  OSDMapRef get_map(epoch_t e) {
    return service.get_map(e);
  }
  OSDMapRef add_map(OSDMap *o) {
    return service.add_map(o);
  }
  bool get_map_bl(epoch_t e, ceph::buffer::list& bl) {
    return service.get_map_bl(e, bl);
  }

public:
  // -- shards --
  std::vector<OSDShard*> shards;
  uint32_t num_shards = 0;

  void inc_num_pgs() {
    ++num_pgs;
  }
  void dec_num_pgs() {
    --num_pgs;
  }
  int get_num_pgs() const {
    return num_pgs;
  }

protected:
  ceph::mutex merge_lock = ceph::make_mutex("OSD::merge_lock");
  /// merge epoch -> target pgid -> source pgid -> pg
  std::map<epoch_t,std::map<spg_t,std::map<spg_t,PGRef>>> merge_waiters;

  bool add_merge_waiter(OSDMapRef nextmap, spg_t target, PGRef source,
			unsigned need);

  // -- placement groups --
  std::atomic<size_t> num_pgs = {0};

  std::mutex pending_creates_lock;
  using create_from_osd_t = std::pair<spg_t, bool /* is primary*/>;
  std::set<create_from_osd_t> pending_creates_from_osd;
  unsigned pending_creates_from_mon = 0;

  PGRecoveryStats pg_recovery_stats;

  PGRef _lookup_pg(spg_t pgid);
  PGRef _lookup_lock_pg(spg_t pgid);
  void register_pg(PGRef pg);
  bool try_finish_pg_delete(PG *pg, unsigned old_pg_num);

  void _get_pgs(std::vector<PGRef> *v, bool clear_too=false);
  void _get_pgids(std::vector<spg_t> *v);

public:
  PGRef lookup_lock_pg(spg_t pgid);

  std::set<int64_t> get_mapped_pools();

protected:
  PG* _make_pg(OSDMapRef createmap, spg_t pgid);

  bool maybe_wait_for_max_pg(const OSDMapRef& osdmap,
			     spg_t pgid, bool is_mon_create);
  void resume_creating_pg();

  void load_pgs();

  epoch_t last_pg_create_epoch;

  void split_pgs(
    PG *parent,
    const std::set<spg_t> &childpgids, std::set<PGRef> *out_pgs,
    OSDMapRef curmap,
    OSDMapRef nextmap,
    PeeringCtx &rctx);
  void _finish_splits(std::set<PGRef>& pgs);

  // == monitor interaction ==
  ceph::mutex mon_report_lock = ceph::make_mutex("OSD::mon_report_lock");
  utime_t last_mon_report;
  Finisher boot_finisher;

  // -- boot --
  void start_boot();
  void _got_mon_epochs(epoch_t oldest, epoch_t newest);
  void _preboot(epoch_t oldest, epoch_t newest);
  void _send_boot();
  void _collect_metadata(std::map<std::string,std::string> *pmeta);
  void _get_purged_snaps();
  void handle_get_purged_snaps_reply(MMonGetPurgedSnapsReply *r);

  void start_waiting_for_healthy();
  bool _is_healthy();

  void send_full_update();

  friend struct CB_OSD_GetVersion;

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
  std::map<int,utime_t> failure_queue;
  std::map<int,std::pair<utime_t,entity_addrvec_t> > failure_pending;

  void requeue_failures();
  void send_failures();
  void send_still_alive(epoch_t epoch, int osd, const entity_addrvec_t &addrs);
  void cancel_pending_failures();

  ceph::coarse_mono_clock::time_point last_sent_beacon;
  ceph::mutex min_last_epoch_clean_lock = ceph::make_mutex("OSD::min_last_epoch_clean_lock");
  epoch_t min_last_epoch_clean = 0;
  // which pgs were scanned for min_lec
  std::vector<pg_t> min_last_epoch_clean_pgs;
  void send_beacon(const ceph::coarse_mono_clock::time_point& now);

  ceph_tid_t get_tid() {
    return service.get_tid();
  }

  // -- generic pg peering --
  void dispatch_context(PeeringCtx &ctx, PG *pg, OSDMapRef curmap,
                        ThreadPool::TPHandle *handle = NULL);

  bool require_mon_peer(const Message *m);
  bool require_mon_or_mgr_peer(const Message *m);
  bool require_osd_peer(const Message *m);

  void handle_fast_pg_create(MOSDPGCreate2 *m);
  void handle_pg_query_nopg(const MQuery& q);
  void handle_fast_pg_notify(MOSDPGNotify *m);
  void handle_pg_notify_nopg(const MNotifyRec& q);
  void handle_fast_pg_info(MOSDPGInfo *m);
  void handle_fast_pg_remove(MOSDPGRemove *m);

public:
  // used by OSDShard
  PGRef handle_pg_create_info(const OSDMapRef& osdmap, const PGCreateInfo *info);
protected:

  void handle_fast_force_recovery(MOSDForceRecovery *m);

  // -- commands --
  void handle_command(class MCommand *m);


  // -- pg recovery --
  void do_recovery(PG *pg, epoch_t epoch_queued, uint64_t pushes_reserved,
		   int priority,
		   ThreadPool::TPHandle &handle);


  // -- scrubbing --
  void resched_all_scrubs();

  // -- status reporting --
  MPGStats *collect_pg_stats();
  std::vector<DaemonHealthMetric> get_health_metrics();


private:
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
    case CEPH_MSG_OSD_OP:
    case CEPH_MSG_OSD_BACKOFF:
    case MSG_OSD_SCRUB2:
    case MSG_OSD_FORCE_RECOVERY:
    case MSG_MON_COMMAND:
    case MSG_OSD_PG_CREATE2:
    case MSG_OSD_PG_QUERY:
    case MSG_OSD_PG_QUERY2:
    case MSG_OSD_PG_INFO:
    case MSG_OSD_PG_INFO2:
    case MSG_OSD_PG_NOTIFY:
    case MSG_OSD_PG_NOTIFY2:
    case MSG_OSD_PG_LOG:
    case MSG_OSD_PG_TRIM:
    case MSG_OSD_PG_REMOVE:
    case MSG_OSD_BACKFILL_RESERVE:
    case MSG_OSD_RECOVERY_RESERVE:
    case MSG_OSD_REPOP:
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
    case MSG_OSD_PG_LEASE:
    case MSG_OSD_PG_LEASE_ACK:
      return true;
    default:
      return false;
    }
  }
  void ms_fast_dispatch(Message *m) override;
  bool ms_dispatch(Message *m) override;
  void ms_handle_connect(Connection *con) override;
  void ms_handle_fast_connect(Connection *con) override;
  void ms_handle_fast_accept(Connection *con) override;
  int ms_handle_fast_authentication(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(CephContext *cct_,
      std::unique_ptr<ObjectStore> store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_front_client,
      Messenger *hb_back_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev,
      ceph::async::io_context_pool& poolctx);
  ~OSD() override;

  // static bits
  static int mkfs(CephContext *cct,
		  std::unique_ptr<ObjectStore> store,
		  uuid_d fsid,
		  int whoami,
		  std::string osdspec_affinity);

  /* remove any non-user xattrs from a std::map of them */
  void filter_xattrs(std::map<std::string, ceph::buffer::ptr>& attrs) {
    for (std::map<std::string, ceph::buffer::ptr>::iterator iter = attrs.begin();
	 iter != attrs.end();
	 ) {
      if (('_' != iter->first.at(0)) || (iter->first.size() == 1))
	attrs.erase(iter++);
      else ++iter;
    }
  }

private:
  int mon_cmd_maybe_osd_create(std::string &cmd);
  int update_crush_device_class();
  int update_crush_location();

  static int write_meta(CephContext *cct,
			ObjectStore *store,
			uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami, std::string& osdspec_affinity);

  void handle_fast_scrub(class MOSDScrub2 *m);
  void handle_osd_ping(class MOSDPing *m);

  size_t get_num_cache_shards();
  int get_num_op_shards();
  int get_num_op_threads();

  float get_osd_recovery_sleep();
  float get_osd_delete_sleep();
  float get_osd_snap_trim_sleep();

  int get_recovery_max_active();
  void maybe_override_max_osd_capacity_for_qos();
  void maybe_override_sleep_options_for_qos();
  bool maybe_override_options_for_qos(
    const std::set<std::string> *changed = nullptr);
  int run_osd_bench_test(int64_t count,
                         int64_t bsize,
                         int64_t osize,
                         int64_t onum,
                         double *elapsed,
                         std::ostream& ss);
  void mon_cmd_set_config(const std::string &key, const std::string &val);

  void scrub_purged_snaps();
  void probe_smart(const std::string& devid, std::ostream& ss);

public:
  static int peek_meta(ObjectStore *store,
		       std::string *magic,
		       uuid_d *cluster_fsid,
		       uuid_d *osd_fsid,
		       int *whoami,
		       ceph_release_t *min_osd_release);


  // startup/shutdown
  int pre_init();
  int init();
  void final_init();

  int enable_disable_fuse(bool stop);
  int set_numa_affinity();

  void suicide(int exitcode);
  int shutdown();

  void handle_signal(int signum);

  /// check if we can throw out op from a disconnected client
  static bool op_is_discardable(const MOSDOp *m);

public:
  OSDService service;
  friend class OSDService;

  /// op queue type set for the OSD
  op_queue_type_t osd_op_queue_type() const;

private:
  void set_perf_queries(const ConfigPayload &config_payload);
  MetricPayload get_perf_reports();

  ceph::mutex m_perf_queries_lock = ceph::make_mutex("OSD::m_perf_queries_lock");
  std::list<OSDPerfMetricQuery> m_perf_queries;
  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> m_perf_limits;
};


//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif // CEPH_OSD_H
