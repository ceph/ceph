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

#include <boost/scoped_ptr.hpp>
#include <boost/container/flat_set.hpp>
#include "include/mempool.h"

// re-include our assert to clobber boost's
#include "include/ceph_assert.h" 
#include "include/common_fwd.h"

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/xlist.h"
#include "SnapMapper.h"
#include "Session.h"
#include "common/Timer.h"

#include "PGLog.h"
#include "OSDMap.h"
#include "include/str_list.h"
#include "PGBackend.h"
#include "PGPeeringEvent.h"
#include "PeeringState.h"
#include "recovery_types.h"
#include "MissingLoc.h"
#include "scrubber_common.h"

#include "mgr/OSDPerfMetricTypes.h"

#include <atomic>
#include <list>
#include <memory>
#include <string>
#include <tuple>

//#define DEBUG_RECOVERY_OIDS   // track std::set of recovering oids explicitly, to find counting bugs
//#define PG_DEBUG_REFS    // track provenance of pg refs, helpful for finding leaks

class OSD;
class OSDService;
struct OSDShard;
struct OSDShardPGSlot;

class PG;
struct OpRequest;
typedef OpRequest::Ref OpRequestRef;
class DynamicPerfStats;
class PgScrubber;
class ScrubBackend;

namespace Scrub {
  class Store;
  class ReplicaReservations;
  class LocalReservation;
  class ReservedByRemotePrimary;
  enum class schedule_result_t;
}

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
  std::map<const char *,per_state_info> info;
  ceph::mutex lock = ceph::make_mutex("PGRecoverStats::lock");

  public:
  PGRecoveryStats() = default;

  void reset() {
    std::lock_guard l(lock);
    info.clear();
  }
  void dump(ostream& out) {
    std::lock_guard l(lock);
    for (std::map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); ++p) {
      per_state_info& i = p->second;
      out << i.enter << "\t" << i.exit << "\t"
	  << i.events << "\t" << i.event_time << "\t"
	  << i.total_time << "\t"
	  << i.min_time << "\t" << i.max_time << "\t"
	  << p->first << "\n";
    }
  }

  void dump_formatted(ceph::Formatter *f) {
    std::lock_guard l(lock);
    f->open_array_section("pg_recovery_stats");
    for (std::map<const char *,per_state_info>::iterator p = info.begin();
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
      std::vector<std::string> states;
      get_str_vec(p->first, "/", states);
      f->open_array_section("nested_states");
      for (std::vector<std::string>::iterator st = states.begin();
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

/** PG - Replica Placement Group
 *
 */

class PG : public DoutPrefixProvider,
	   public PeeringState::PeeringListener,
	   public Scrub::PgScrubBeListener {
  friend struct NamedState;
  friend class PeeringState;
  friend class PgScrubber;
  friend class ScrubBackend;

public:
  const pg_shard_t pg_whoami;
  const spg_t pg_id;

  /// the 'scrubber'. Will be allocated in the derivative (PrimaryLogPG) ctor,
  /// and be removed only in the PrimaryLogPG destructor.
  std::unique_ptr<ScrubPgIF> m_scrubber;

  /// flags detailing scheduling/operation characteristics of the next scrub 
  requested_scrub_t m_planned_scrub;

  const requested_scrub_t& get_planned_scrub() const {
    return m_planned_scrub;
  }

  /// scrubbing state for both Primary & replicas
  bool is_scrub_active() const { return m_scrubber->is_scrub_active(); }

  /// set when the scrub request is queued, and reset after scrubbing fully
  /// cleaned up.
  bool is_scrub_queued_or_active() const { return m_scrubber->is_queued_or_active(); }

public:
  // -- members --
  const coll_t coll;

  ObjectStore::CollectionHandle ch;

  // -- methods --
  std::ostream& gen_prefix(std::ostream& out) const override;
  CephContext *get_cct() const override {
    return cct;
  }
  unsigned get_subsys() const override {
    return ceph_subsys_osd;
  }

  const char* const get_current_state() const {
    return recovery_state.get_current_state();
  }

  const OSDMapRef& get_osdmap() const {
    ceph_assert(is_locked());
    return recovery_state.get_osdmap();
  }

  epoch_t get_osdmap_epoch() const override final {
    return recovery_state.get_osdmap()->get_epoch();
  }

  PerfCounters &get_peering_perf() override;
  PerfCounters &get_perf_logger() override;
  void log_state_enter(const char *state) override;
  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) override;

  void lock(bool no_lockdep = false) const;
  void unlock() const;
  bool is_locked() const;

  const spg_t& get_pgid() const {
    return pg_id;
  }

  const PGPool& get_pgpool() const final {
    return pool;
  }
  uint64_t get_last_user_version() const {
    return info.last_user_version;
  }
  const pg_history_t& get_history() const {
    return info.history;
  }
  bool get_need_up_thru() const {
    return recovery_state.get_need_up_thru();
  }
  epoch_t get_same_interval_since() const {
    return info.history.same_interval_since;
  }

  bool is_waiting_for_unreadable_object() const final
  {
    return !waiting_for_unreadable_object.empty();
  }

  static void set_last_scrub_stamp(
    utime_t t, pg_history_t &history, pg_stat_t &stats) {
    stats.last_scrub_stamp = t;
    history.last_scrub_stamp = t;
  }

  void set_last_scrub_stamp(utime_t t) {
    recovery_state.update_stats(
      [t](auto &history, auto &stats) {
	set_last_scrub_stamp(t, history, stats);
	return true;
      });
  }

  static void set_last_deep_scrub_stamp(
    utime_t t, pg_history_t &history, pg_stat_t &stats) {
    stats.last_deep_scrub_stamp = t;
    history.last_deep_scrub_stamp = t;
  }

  void set_last_deep_scrub_stamp(utime_t t) {
    recovery_state.update_stats(
      [t](auto &history, auto &stats) {
	set_last_deep_scrub_stamp(t, history, stats);
	return true;
      });
  }

  static void add_objects_scrubbed_count(
    int64_t count, pg_stat_t &stats) {
    stats.objects_scrubbed += count;
  }

  void add_objects_scrubbed_count(int64_t count) {
    recovery_state.update_stats(
      [count](auto &history, auto &stats) {
	add_objects_scrubbed_count(count, stats);
	return true;
      });
  }

  static void reset_objects_scrubbed(pg_stat_t &stats) {
    stats.objects_scrubbed = 0;
  }

  void reset_objects_scrubbed()
  {
    recovery_state.update_stats([](auto& history, auto& stats) {
      reset_objects_scrubbed(stats);
      return true;
    });
  }

  bool is_deleting() const {
    return recovery_state.is_deleting();
  }
  bool is_deleted() const {
    return recovery_state.is_deleted();
  }
  bool is_nonprimary() const {
    return recovery_state.is_nonprimary();
  }
  bool is_primary() const {
    return recovery_state.is_primary();
  }
  bool pg_has_reset_since(epoch_t e) {
    ceph_assert(is_locked());
    return recovery_state.pg_has_reset_since(e);
  }

  bool is_ec_pg() const {
    return recovery_state.is_ec_pg();
  }
  int get_role() const {
    return recovery_state.get_role();
  }
  const std::vector<int> get_acting() const {
    return recovery_state.get_acting();
  }
  const std::set<pg_shard_t> &get_actingset() const {
    return recovery_state.get_actingset();
  }
  int get_acting_primary() const {
    return recovery_state.get_acting_primary();
  }
  pg_shard_t get_primary() const final {
    return recovery_state.get_primary();
  }
  const std::vector<int> get_up() const {
    return recovery_state.get_up();
  }
  int get_up_primary() const {
    return recovery_state.get_up_primary();
  }
  const PastIntervals& get_past_intervals() const {
    return recovery_state.get_past_intervals();
  }
  bool is_acting_recovery_backfill(pg_shard_t osd) const {
    return recovery_state.is_acting_recovery_backfill(osd);
  }
  const std::set<pg_shard_t> &get_acting_recovery_backfill() const {
    return recovery_state.get_acting_recovery_backfill();
  }
  bool is_acting(pg_shard_t osd) const {
    return recovery_state.is_acting(osd);
  }
  bool is_up(pg_shard_t osd) const {
    return recovery_state.is_up(osd);
  }
  static bool has_shard(bool ec, const std::vector<int>& v, pg_shard_t osd) {
    return PeeringState::has_shard(ec, v, osd);
  }

  /// initialize created PG
  void init(
    int role,
    const std::vector<int>& up,
    int up_primary,
    const std::vector<int>& acting,
    int acting_primary,
    const pg_history_t& history,
    const PastIntervals& pim,
    ObjectStore::Transaction &t);

  /// read existing pg state off disk
  void read_state(ObjectStore *store);
  static int peek_map_epoch(ObjectStore *store, spg_t pgid, epoch_t *pepoch);

  static int get_latest_struct_v() {
    return pg_latest_struct_v;
  }
  static int get_compat_struct_v() {
    return pg_compat_struct_v;
  }
  static int read_info(
    ObjectStore *store, spg_t pgid, const coll_t &coll,
    pg_info_t &info, PastIntervals &past_intervals,
    __u8 &);
  static bool _has_removal_flag(ObjectStore *store, spg_t pgid);

  void rm_backoff(const ceph::ref_t<Backoff>& b);

  void update_snap_mapper_bits(uint32_t bits) {
    snap_mapper.update_bits(bits);
  }
  void start_split_stats(const std::set<spg_t>& childpgs, std::vector<object_stat_sum_t> *v);
  virtual void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction &t) = 0;
  void split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void merge_from(std::map<spg_t,PGRef>& sources, PeeringCtx &rctx,
		  unsigned split_bits,
		  const pg_merge_meta_t& last_pg_merge_meta);
  void finish_split_stats(const object_stat_sum_t& stats,
			  ObjectStore::Transaction &t);

  void scrub(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    // a new scrub
    forward_scrub_event(&ScrubPgIF::initiate_regular_scrub, queued, "StartScrub");
  }

  /**
   *  a special version of PG::scrub(), which:
   *  - is initiated after repair, and
   * (not true anymore:)
   *  - is not required to allocate local/remote OSD scrub resources
   */
  void recovery_scrub(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    // a new scrub
    forward_scrub_event(&ScrubPgIF::initiate_scrub_after_repair, queued,
			"AfterRepairScrub");
  }

  void replica_scrub(epoch_t queued,
		     Scrub::act_token_t act_token,
		     ThreadPool::TPHandle& handle);

  void replica_scrub_resched(epoch_t queued,
			     Scrub::act_token_t act_token,
			     ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_sched_replica, queued, act_token,
			"SchedReplica");
  }

  void scrub_send_resources_granted(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_remotes_reserved, queued, "RemotesReserved");
  }

  void scrub_send_resources_denied(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_reservation_failure, queued,
			"ReservationFailure");
  }

  void scrub_send_scrub_resched(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_scrub_resched, queued, "InternalSchedScrub");
  }

  void scrub_send_pushes_update(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::active_pushes_notification, queued,
			"ActivePushesUpd");
  }

  void scrub_send_applied_update(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::update_applied_notification, queued,
			"UpdatesApplied");
  }

  void scrub_send_unblocking(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_scrub_unblock, queued, "Unblocked");
  }

  void scrub_send_digest_update(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::digest_update_notification, queued, "DigestUpdate");
  }

  void scrub_send_local_map_ready(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_local_map_done, queued, "IntLocalMapDone");
  }

  void scrub_send_replmaps_ready(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_replica_maps_ready, queued, "GotReplicas");
  }

  void scrub_send_replica_pushes(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_replica_pushes_upd, queued,
			"ReplicaPushesUpd");
  }

  void scrub_send_get_next_chunk(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_get_next_chunk, queued, "NextChunk");
  }

  void scrub_send_scrub_is_finished(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_scrub_is_finished, queued, "ScrubFinished");
  }

  void scrub_send_chunk_free(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_chunk_free, queued, "SelectedChunkFree");
  }

  void scrub_send_chunk_busy(epoch_t queued, ThreadPool::TPHandle& handle)
  {
    forward_scrub_event(&ScrubPgIF::send_chunk_busy, queued, "ChunkIsBusy");
  }

  void queue_want_pg_temp(const std::vector<int> &wanted) override;
  void clear_want_pg_temp() override;

  void on_new_interval() override;

  void on_role_change() override;
  virtual void plpg_on_role_change() = 0;

  void init_collection_pool_opts();
  void on_pool_change() override;
  virtual void plpg_on_pool_change() = 0;

  void on_info_history_change() override;

  void on_primary_status_change(bool was_primary, bool now_primary) override;

  void reschedule_scrub() override;

  void scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type) override;

  uint64_t get_snap_trimq_size() const override {
    return snap_trimq.size();
  }

  static void add_objects_trimmed_count(
    int64_t count, pg_stat_t &stats) {
    stats.objects_trimmed += count;
  }

  void add_objects_trimmed_count(int64_t count) {
    recovery_state.update_stats_wo_resched(
      [count](auto &history, auto &stats) {
        add_objects_trimmed_count(count, stats);
      });
  }

  static void reset_objects_trimmed(pg_stat_t &stats) {
    stats.objects_trimmed = 0;
  }

  void reset_objects_trimmed() {
    recovery_state.update_stats_wo_resched(
      [](auto &history, auto &stats) {
        reset_objects_trimmed(stats);
      });
  }

  utime_t snaptrim_begin_stamp;

  void set_snaptrim_begin_stamp() {
    snaptrim_begin_stamp = ceph_clock_now();
  }

  void set_snaptrim_duration() {
    utime_t cur_stamp = ceph_clock_now();
    utime_t duration = cur_stamp - snaptrim_begin_stamp;
    recovery_state.update_stats_wo_resched(
      [duration](auto &history, auto &stats) {
        stats.snaptrim_duration = double(duration);
    });
  }

  unsigned get_target_pg_log_entries() const override;

  void clear_publish_stats() override;
  void clear_primary_state() override;

  epoch_t cluster_osdmap_trim_lower_bound() override;
  OstreamTemp get_clog_error() override;
  OstreamTemp get_clog_info() override;
  OstreamTemp get_clog_debug() override;

  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) override;
  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override;
  void update_local_background_io_priority(
    unsigned priority) override;
  void cancel_local_background_io_reservation() override;

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override;
  void cancel_remote_recovery_reservation() override;

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) override;

  void on_active_exit() override;

  Context *on_clean() override {
    if (is_active()) {
      kick_snap_trim();
    }
    requeue_ops(waiting_for_clean_to_primary_repair);
    return finish_recovery();
  }

  void on_activate(interval_set<snapid_t> snaps) override;

  void on_activate_committed() override;

  void on_active_actmap() override;
  void on_active_advmap(const OSDMapRef &osdmap) override;

  void queue_snap_retrim(snapid_t snap);

  void on_backfill_reserved() override;
  void on_backfill_canceled() override;
  void on_recovery_reserved() override;

  bool is_forced_recovery_or_backfill() const {
    return recovery_state.is_forced_recovery_or_backfill();
  }

  PGLog::LogEntryHandlerRef get_log_handler(
    ObjectStore::Transaction &t) override {
    return std::make_unique<PG::PGLogEntryHandler>(this, &t);
  }

  std::pair<ghobject_t, bool> do_delete_work(ObjectStore::Transaction &t,
    ghobject_t _next) override;

  void clear_ready_to_merge() override;
  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) override;
  void set_not_ready_to_merge_source(pg_t pgid) override;
  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) override;
  void set_ready_to_merge_source(eversion_t lu) override;

  void send_pg_created(pg_t pgid) override;

  ceph::signedspan get_mnow() const override;
  HeartbeatStampsRef get_hb_stamps(int peer) override;
  void schedule_renew_lease(epoch_t lpr, ceph::timespan delay) override;
  void queue_check_readable(epoch_t lpr, ceph::timespan delay) override;

  void rebuild_missing_set_with_deletes(PGLog &pglog) override;

  void queue_peering_event(PGPeeringEventRef evt);
  void do_peering_event(PGPeeringEventRef evt, PeeringCtx &rcx);
  void queue_null(epoch_t msg_epoch, epoch_t query_epoch);
  void queue_flushed(epoch_t started_at);
  void handle_advance_map(
    OSDMapRef osdmap, OSDMapRef lastmap,
    std::vector<int>& newup, int up_primary,
    std::vector<int>& newacting, int acting_primary,
    PeeringCtx &rctx);
  void handle_activate_map(PeeringCtx &rctx);
  void handle_initialize(PeeringCtx &rxcx);
  void handle_query_state(ceph::Formatter *f);

  /**
   * @param ops_begun returns how many recovery ops the function started
   * @returns true if any useful work was accomplished; false otherwise
   */
  virtual bool start_recovery_ops(
    uint64_t max,
    ThreadPool::TPHandle &handle,
    uint64_t *ops_begun) = 0;

  // more work after the above, but with a PeeringCtx
  void find_unfound(epoch_t queued, PeeringCtx &rctx);

  virtual void get_watchers(std::list<obj_watch_item_t> *ls) = 0;

  void dump_pgstate_history(ceph::Formatter *f);
  void dump_missing(ceph::Formatter *f);

  void with_pg_stats(ceph::coarse_real_clock::time_point now_is,
		     std::function<void(const pg_stat_t&, epoch_t lec)>&& f);
  void with_heartbeat_peers(std::function<void(int)>&& f);

  void shutdown();
  virtual void on_shutdown() = 0;

  bool get_must_scrub() const;
  Scrub::schedule_result_t sched_scrub();

  unsigned int scrub_requeue_priority(Scrub::scrub_prio_t with_priority, unsigned int suggested_priority) const;
  /// the version that refers to flags_.priority
  unsigned int scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const;
private:
  // auxiliaries used by sched_scrub():
  double next_deepscrub_interval() const;

  /// should we perform deep scrub?
  bool is_time_for_deep(bool allow_deep_scrub,
                        bool allow_shallow_scrub,
                        bool has_deep_errors,
                        const requested_scrub_t& planned) const;

  /**
   * Validate the various 'next scrub' flags in m_planned_scrub against configuration
   * and scrub-related timestamps.
   *
   * @returns an updated copy of the m_planned_flags (or nothing if no scrubbing)
   */
  std::optional<requested_scrub_t> validate_scrub_mode() const;

  std::optional<requested_scrub_t> validate_periodic_mode(
    bool allow_deep_scrub,
    bool try_to_auto_repair,
    bool allow_shallow_scrub,
    bool time_for_deep,
    bool has_deep_errors,
    const requested_scrub_t& planned) const;

  std::optional<requested_scrub_t> validate_initiated_scrub(
    bool allow_deep_scrub,
    bool try_to_auto_repair,
    bool time_for_deep,
    bool has_deep_errors,
    const requested_scrub_t& planned) const;

  using ScrubAPI = void (ScrubPgIF::*)(epoch_t epoch_queued);
  void forward_scrub_event(ScrubAPI fn, epoch_t epoch_queued, std::string_view desc);
  // and for events that carry a meaningful 'activation token'
  using ScrubSafeAPI = void (ScrubPgIF::*)(epoch_t epoch_queued,
					   Scrub::act_token_t act_token);
  void forward_scrub_event(ScrubSafeAPI fn,
			   epoch_t epoch_queued,
			   Scrub::act_token_t act_token,
			   std::string_view desc);

public:
  virtual void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle
  ) = 0;
  virtual void clear_cache() = 0;
  virtual int get_cache_obj_count() = 0;

  virtual void snap_trimmer(epoch_t epoch_queued) = 0;
  virtual void do_command(
    const std::string_view& prefix,
    const cmdmap_t& cmdmap,
    const ceph::buffer::list& idata,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_finish) = 0;

  virtual bool agent_work(int max) = 0;
  virtual bool agent_work(int max, int agent_flush_quota) = 0;
  virtual void agent_stop() = 0;
  virtual void agent_delay() = 0;
  virtual void agent_clear() = 0;
  virtual void agent_choose_mode_restart() = 0;

  struct C_DeleteMore : public Context {
    PGRef pg;
    epoch_t epoch;
    C_DeleteMore(PG *p, epoch_t e) : pg(p), epoch(e) {}
    void finish(int r) override {
      ceph_abort();
    }
    void complete(int r) override;
  };

  virtual void set_dynamic_perf_stats_queries(
    const std::list<OSDPerfMetricQuery> &queries) {
  }
  virtual void get_dynamic_perf_stats(DynamicPerfStats *stats) {
  }

  uint64_t get_min_alloc_size() const;

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

  // locking and reference counting.
  // I destroy myself when the reference count hits zero.
  // lock() should be called before doing anything.
  // get() should be called on pointer copy (to another thread, etc.).
  // put() should be called on destruction of some previously copied pointer.
  // unlock() when done with the current pointer (_most common_).
  mutable ceph::mutex _lock = ceph::make_mutex("PG::_lock");
#ifndef CEPH_DEBUG_MUTEX
  mutable std::thread::id locked_by;
#endif
  std::atomic<unsigned int> ref{0};

#ifdef PG_DEBUG_REFS
  ceph::mutex _ref_id_lock = ceph::make_mutex("PG::_ref_id_lock");
  std::map<uint64_t, std::string> _live_ids;
  std::map<std::string, uint64_t> _tag_counts;
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

  virtual PGBackend *get_pgbackend() = 0;
  virtual const PGBackend* get_pgbackend() const = 0;

protected:
  void requeue_map_waiters();

protected:

  ZTracer::Endpoint trace_endpoint;


protected:
  __u8 info_struct_v = 0;
  void upgrade(ObjectStore *store);

protected:
  ghobject_t    pgmeta_oid;

  // ------------------
  interval_set<snapid_t> snap_trimq;
  std::set<snapid_t> snap_trimq_repeat;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item stat_queue_item;
  bool recovery_queued;

  int recovery_ops_active;
  std::set<pg_shard_t> waiting_on_backfill;
#ifdef DEBUG_RECOVERY_OIDS
  multiset<hobject_t> recovering_oids;
#endif

public:
  bool dne() { return info.dne(); }

  void send_cluster_message(
    int osd, MessageRef m, epoch_t epoch, bool share_map_update) override;

protected:
  epoch_t get_last_peering_reset() const {
    return recovery_state.get_last_peering_reset();
  }

  /* heartbeat peers */
  void set_probe_targets(const std::set<pg_shard_t> &probe_set) override;
  void clear_probe_targets() override;

  ceph::mutex heartbeat_peer_lock =
    ceph::make_mutex("PG::heartbeat_peer_lock");
  std::set<int> heartbeat_peers;
  std::set<int> probe_targets;

protected:
  BackfillInterval backfill_info;
  std::map<pg_shard_t, BackfillInterval> peer_backfill_info;
  bool backfill_reserving;

  // The primary's num_bytes and local num_bytes for this pg, only valid
  // during backfill for non-primary shards.
  // Both of these are adjusted for EC to reflect the on-disk bytes
  std::atomic<int64_t> primary_num_bytes = 0;
  std::atomic<int64_t> local_num_bytes = 0;

public:
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

  bool try_reserve_recovery_space(int64_t primary, int64_t local) override;
  void unreserve_recovery_space() override;

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
    ceph_assert(ceph_mutex_is_locked_by_me(_lock));
    if (num_bytes) {
      recovery_state.update_stats(
	[num_bytes](auto &history, auto &stats) {
	  stats.stats.sum.num_bytes += num_bytes;
	  if (stats.stats.sum.num_bytes < 0) {
	    stats.stats.sum.num_bytes = 0;
	  }
	  return false;
	});
    }
  }
  void sub_num_bytes(int64_t num_bytes) {
    ceph_assert(ceph_mutex_is_locked_by_me(_lock));
    ceph_assert(num_bytes >= 0);
    if (num_bytes) {
      recovery_state.update_stats(
	[num_bytes](auto &history, auto &stats) {
	  stats.stats.sum.num_bytes -= num_bytes;
	  if (stats.stats.sum.num_bytes < 0) {
	    stats.stats.sum.num_bytes = 0;
	  }
	  return false;
	});
    }
  }

  // Only used in testing so not worried about needing the PG lock here
  int64_t get_stats_num_bytes() {
    std::lock_guard l{_lock};
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

  uint64_t get_average_object_size() {
    ceph_assert(ceph_mutex_is_locked_by_me(_lock));
    auto num_bytes = static_cast<uint64_t>(
      std::max<int64_t>(
        0, // ensure bytes is non-negative
        info.stats.stats.sum.num_bytes));
    auto num_objects = static_cast<uint64_t>(
      std::max<int64_t>(
        1, // ensure objects is non-negative and non-zero
        info.stats.stats.sum.num_objects));
    return std::max<uint64_t>(num_bytes / num_objects, 1);
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
   *  - waiting_for_readable
   *    - now > readable_until
   *    - unblocks when we get fresh(er) osd_pings
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
   *  3. The requeue_ops helper will push ops onto the waiting_for_map std::list if
   *     it is non-empty.
   *
   * These three behaviors are generally sufficient to maintain ordering, with
   * the possible exception of cases where we make an object degraded or
   * unreadable that was previously okay, e.g. when scrub or op processing
   * encounter an unexpected error.  FIXME.
   */

  // ops with newer maps than our (or blocked behind them)
  // track these by client, since inter-request ordering doesn't otherwise
  // matter.
  std::unordered_map<entity_name_t,std::list<OpRequestRef>> waiting_for_map;

  // ops waiting on peered
  std::list<OpRequestRef>            waiting_for_peered;

  /// ops waiting on readble
  std::list<OpRequestRef>            waiting_for_readable;

  // ops waiting on active (require peered as well)
  std::list<OpRequestRef>            waiting_for_active;
  std::list<OpRequestRef>            waiting_for_flush;
  std::list<OpRequestRef>            waiting_for_scrub;

  std::list<OpRequestRef>            waiting_for_cache_not_full;
  std::list<OpRequestRef>            waiting_for_clean_to_primary_repair;
  std::map<hobject_t, std::list<OpRequestRef>> waiting_for_unreadable_object,
			     waiting_for_degraded_object,
			     waiting_for_blocked_object;

  std::set<hobject_t> objects_blocked_on_cache_full;
  std::map<hobject_t,snapid_t> objects_blocked_on_degraded_snap;
  std::map<hobject_t,ObjectContextRef> objects_blocked_on_snap_promotion;

  // Callbacks should assume pg (and nothing else) is locked
  std::map<hobject_t, std::list<Context*>> callbacks_for_degraded_object;

  std::map<eversion_t,
      std::list<
	std::tuple<OpRequestRef, version_t, int,
		   std::vector<pg_log_op_return_item_t>>>> waiting_for_ondisk;

  void requeue_object_waiters(std::map<hobject_t, std::list<OpRequestRef>>& m);
  void requeue_op(OpRequestRef op);
  void requeue_ops(std::list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  ceph::mutex pg_stats_publish_lock =
    ceph::make_mutex("PG::pg_stats_publish_lock");
  std::optional<pg_stat_t> pg_stats_publish;

  friend class TestOpsSocketHook;
  void publish_stats_to_osd() override;

  bool needs_recovery() const {
    return recovery_state.needs_recovery();
  }
  bool needs_backfill() const {
    return recovery_state.needs_backfill();
  }

  bool all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const;

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
    const std::set<snapid_t> &snaps);
  void clear_object_snap_mapping(
    ObjectStore::Transaction *t, const hobject_t &soid);
  void remove_snap_mapped_object(
    ObjectStore::Transaction& t, const hobject_t& soid);

  bool have_unfound() const { 
    return recovery_state.have_unfound();
  }
  uint64_t get_num_unfound() const {
    return recovery_state.get_num_unfound();
  }

  virtual void check_local() = 0;

  void purge_strays();

  void update_heartbeat_peers(std::set<int> peers) override;

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
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits) = 0;

  friend class C_OSD_RepModify_Commit;
  friend struct C_DeleteMore;

  // -- backoff --
  ceph::mutex backoff_lock = // orders inside Backoff::lock
    ceph::make_mutex("PG::backoff_lock");
  std::map<hobject_t,std::set<ceph::ref_t<Backoff>>> backoffs;

  void add_backoff(const ceph::ref_t<Session>& s, const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& begin, const hobject_t& end);
  void release_backoffs(const hobject_t& o) {
    release_backoffs(o, o);
  }
  void clear_backoffs();

  void add_pg_backoff(const ceph::ref_t<Session>& s) {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    add_backoff(s, begin, end);
  }
public:
  void release_pg_backoffs() {
    hobject_t begin = info.pgid.pgid.get_hobj_start();
    hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
    release_backoffs(begin, end);
  }

  // -- scrub --
protected:
  bool scrub_after_recovery;

  int active_pushes;

  [[nodiscard]] bool ops_blocked_by_scrub() const;
  [[nodiscard]] Scrub::scrub_prio_t is_scrub_blocking_ops() const;

  void _scan_rollback_obs(const std::vector<ghobject_t> &rollback_obs);
  /**
   * returns true if [begin, end) is good to scrub at this time
   * a false return value obliges the implementer to requeue scrub when the
   * condition preventing scrub clears
   */
  virtual bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end) = 0;

  /**
   * Initiate the process that will create our scrub map for the Primary.
   * (triggered by MSG_OSD_REP_SCRUB)
   */
  void replica_scrub(OpRequestRef op, ThreadPool::TPHandle &handle);

  // -- recovery state --

  struct QueuePeeringEvt : Context {
    PGRef pg;
    PGPeeringEventRef evt;

    template <class EVT>
    QueuePeeringEvt(PG *pg, epoch_t epoch, EVT evt) :
      pg(pg), evt(std::make_shared<PGPeeringEvent>(epoch, epoch, evt)) {}

    QueuePeeringEvt(PG *pg, PGPeeringEventRef evt) :
      pg(pg), evt(std::move(evt)) {}

    void finish(int r) override {
      pg->lock();
      pg->queue_peering_event(std::move(evt));
      pg->unlock();
    }
  };


public:
  int pg_stat_adjust(osd_stat_t *new_stat);
protected:
  bool delete_needs_sleep = false;

protected:
  bool state_test(uint64_t m) const { return recovery_state.state_test(m); }
  void state_set(uint64_t m) { recovery_state.state_set(m); }
  void state_clear(uint64_t m) { recovery_state.state_clear(m); }

  bool is_complete() const {
    return recovery_state.is_complete();
  }
  bool should_send_notify() const {
    return recovery_state.should_send_notify();
  }

  bool is_active() const { return recovery_state.is_active(); }
  bool is_activating() const { return recovery_state.is_activating(); }
  bool is_peering() const { return recovery_state.is_peering(); }
  bool is_down() const { return recovery_state.is_down(); }
  bool is_recovery_unfound() const { return recovery_state.is_recovery_unfound(); }
  bool is_backfill_unfound() const { return recovery_state.is_backfill_unfound(); }
  bool is_incomplete() const { return recovery_state.is_incomplete(); }
  bool is_clean() const { return recovery_state.is_clean(); }
  bool is_degraded() const { return recovery_state.is_degraded(); }
  bool is_undersized() const { return recovery_state.is_undersized(); }
  bool is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); } // Primary only
  bool is_remapped() const { return recovery_state.is_remapped(); }
  bool is_peered() const { return recovery_state.is_peered(); }
  bool is_recovering() const { return recovery_state.is_recovering(); }
  bool is_premerge() const { return recovery_state.is_premerge(); }
  bool is_repair() const { return recovery_state.is_repair(); }
  bool is_laggy() const { return state_test(PG_STATE_LAGGY); }
  bool is_wait() const { return state_test(PG_STATE_WAIT); }

  bool is_empty() const { return recovery_state.is_empty(); }

  // pg on-disk state
  void do_pending_flush();

public:
  void prepare_write(
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ObjectStore::Transaction &t) override;

  void write_if_dirty(PeeringCtx &rctx) {
    write_if_dirty(rctx.transaction);
  }
protected:
  void write_if_dirty(ObjectStore::Transaction& t) {
    recovery_state.write_if_dirty(t);
  }

  PGLog::IndexedLog projected_log;
  bool check_in_progress_op(
    const osd_reqid_t &r,
    eversion_t *version,
    version_t *user_version,
    int *return_code,
    std::vector<pg_log_op_return_item_t> *op_returns) const;
  eversion_t projected_last_update;
  eversion_t get_next_version() const {
    eversion_t at_version(
      get_osdmap_epoch(),
      projected_last_update.version+1);
    ceph_assert(at_version > info.last_update);
    ceph_assert(at_version > recovery_state.get_pg_log().get_head());
    ceph_assert(at_version > projected_last_update);
    return at_version;
  }

  bool check_log_for_corruption(ObjectStore *store);

  std::string get_corrupt_pg_log_name() const;

  void update_snap_map(
    const std::vector<pg_log_entry_t> &log_entries,
    ObjectStore::Transaction& t);

  void filter_snapc(std::vector<snapid_t> &snaps);

  virtual void kick_snap_trim() = 0;
  virtual void snap_trimmer_scrub_complete() = 0;

  void queue_recovery();
  void queue_scrub_after_repair();
  unsigned int get_scrub_priority();

  bool try_flush_or_schedule_async() override;
  void start_flush_on_transaction(
    ObjectStore::Transaction &t) override;

  void update_history(const pg_history_t& history) {
    recovery_state.update_history(history);
  }

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
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef& op);

  // abstract bits
  friend struct FlushState;

  friend ostream& operator<<(ostream& out, const PG& pg);

protected:
  PeeringState recovery_state;

  // ref to recovery_state.pool
  const PGPool &pool;

  // ref to recovery_state.info
  const pg_info_t &info;


// ScrubberPasskey getters/misc:
public:
 const pg_info_t& get_pg_info(ScrubberPasskey) const final { return info; }

 OSDService* get_pg_osd(ScrubberPasskey) const { return osd; }

 requested_scrub_t& get_planned_scrub(ScrubberPasskey)
 {
   return m_planned_scrub;
 }

 void force_object_missing(ScrubberPasskey,
                           const std::set<pg_shard_t>& peer,
                           const hobject_t& oid,
                           eversion_t version) final
 {
   recovery_state.force_object_missing(peer, oid, version);
 }

 uint64_t logical_to_ondisk_size(uint64_t logical_size) const final
 {
   return get_pgbackend()->be_get_ondisk_size(logical_size);
 }
};

#endif
