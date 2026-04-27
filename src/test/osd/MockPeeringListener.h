// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <memory>
#include <vector>
#include <list>
#include <map>
#include "osd/PeeringState.h"
#include "osd/osd_perf_counters.h"
#include "common/HeartbeatMap.h"
#include "os/ObjectStore.h"
#include "MockPGBackendListener.h"
#include "MockPGBackend.h"
#include "MockPGLogEntryHandler.h"
#include "MockMessenger.h"
#include "global/global_context.h"

// Forward declarations
class EventLoop;
class ECPeeringTestFixture;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd

// Mock implementation of PeeringState::PeeringListener for testing.
// inject_* variables can be used to create race hazards or test failure paths.
class MockPeeringListener : public PeeringState::PeeringListener {
 public:
  pg_shard_t pg_whoami;
  PeeringState *ps;
  std::unique_ptr<MockPGBackendListener> backend_listener;
  coll_t coll;
  ObjectStore::CollectionHandle ch;
  std::unique_ptr<MockPGBackend> backend;
  PerfCounters* recoverystate_perf;
  PerfCounters* logger_perf;
  std::vector<int> next_acting;

  // MockMessenger for routing cluster messages (optional - used by ECPeeringTestFixture)
  MockMessenger* messenger = nullptr;
  
  // EventLoop for routing events (optional - used by ECPeeringTestFixture)
  class EventLoop* event_loop = nullptr;
  
  // Fixture pointer for accessing peering state and context (optional - used by ECPeeringTestFixture)
  class ECPeeringTestFixture* fixture = nullptr;

#ifdef WITH_CRIMSON
  // Per OSD state - kept for backward compatibility with TestPeeringState
  // When messenger is set, messages are routed through it instead
  std::map<int,std::list<MessageURef>> messages;
#else
  // Per OSD state - kept for backward compatibility with TestPeeringState
  // When messenger is set, messages are routed through it instead
  std::map<int,std::list<MessageRef>> messages;
#endif
  std::vector<HeartbeatStampsRef> hb_stamps;
  std::list<PGPeeringEventRef> events;
  std::list<PGPeeringEventRef> stalled_events;

  // By default MockPeeringListener will add events to the event queue immediately
  // simulating the responses that PrimaryLogPG normally generates. These inject
  // booleans can change the behavior to test other code paths

  // If inject_event_stall is true then events are added to the stalled_events list
  // and the test case must manually dispatch the event
  bool inject_event_stall = false;

  // If inject_keep_preempt is true then the preempt event for a local/remote
  // reservation is added to the stalled_events list so the test case can later
  // dispatch this event to test a preempted reservation
  bool inject_keep_preempt = false;

  // If inject_fail_reserve_recovery_space is true then reject backfill/pool
  // migration requests with too full
  bool inject_fail_reserve_recovery_space = false;

  std::function<int(ObjectStore::Transaction&&)> queue_transaction_callback;

  MockPeeringListener(OSDMapRef osdmap,
                      int64_t pool_id,
                      DoutPrefixProvider *dpp,
                      pg_shard_t pg_whoami) : pg_whoami(pg_whoami) {
    backend_listener = std::make_unique<MockPGBackendListener>(osdmap, pool_id, dpp, pg_whoami);
    backend = std::make_unique<MockPGBackend>(g_ceph_context, backend_listener.get(), nullptr, coll, ch);
    recoverystate_perf = build_recoverystate_perf(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(recoverystate_perf);
    logger_perf = build_osd_logger(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(logger_perf);
  }
  /// Constructor for ECPeeringTestFixture: accepts a pre-created backend listener
  /// instead of creating one internally. This avoids the throw-away construction
  /// pattern where the internally-created listener would be immediately replaced.
  MockPeeringListener(OSDMapRef osdmap,
                      int64_t pool_id,
                      DoutPrefixProvider *dpp,
                      pg_shard_t pg_whoami,
                      std::unique_ptr<MockPGBackendListener> bl,
                      ObjectStore *object_store,
                      coll_t coll_arg,
                      ObjectStore::CollectionHandle ch_arg)
    : pg_whoami(pg_whoami),
      backend_listener(std::move(bl)),
      coll(coll_arg),
      ch(ch_arg)
  {
    backend = std::make_unique<MockPGBackend>(
      g_ceph_context, backend_listener.get(), object_store, coll, ch);
    recoverystate_perf = build_recoverystate_perf(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(recoverystate_perf);
    logger_perf = build_osd_logger(g_ceph_context);
    g_ceph_context->get_perfcounters_collection()->add(logger_perf);
  }


  ~MockPeeringListener() {
    if (recoverystate_perf) {
      g_ceph_context->get_perfcounters_collection()->remove(recoverystate_perf);
      delete recoverystate_perf;
      recoverystate_perf = nullptr;
    }
    if (logger_perf) {
      g_ceph_context->get_perfcounters_collection()->remove(logger_perf);
      delete logger_perf;
      logger_perf = nullptr;
    }
  }

  epoch_t get_osdmap_epoch() const override {
    return current_epoch;
  }

  // PeeringListener interface
  void prepare_write(
    pg_info_t &info,
    pg_info_t &last_written_info,
    PastIntervals &past_intervals,
    PGLog &pglog,
    bool dirty_info,
    bool dirty_big_info,
    bool need_write_epoch,
    ObjectStore::Transaction &t) override {
    prepare_write_called = true;
    
    // If a callback is set, queue the transaction
    if (queue_transaction_callback && !t.empty()) {
      ObjectStore::Transaction copy;
      copy.append(t);
      queue_transaction_callback(std::move(copy));
    }
  }

  void scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type) override {
    scrub_requested_called = true;
  }

  uint64_t get_snap_trimq_size() const override {
    return snap_trimq_size;
  }

#ifdef WITH_CRIMSON
  void send_cluster_message(
    int osd, MessageURef m, epoch_t epoch, bool share_map_update=false) override {
    dout(0) << "send_cluster_message to " << osd << " " << m << " epoch " << epoch << dendl;
    if (messenger) {
      // Use MockMessenger for EventLoop-based routing with epoch tracking
      messenger->send_message(pg_whoami.osd, osd, m.detach());
    } else {
      // Fall back to direct message queue for TestPeeringState compatibility
      messages[osd].push_back(m);
    }
    messages_sent++;
  }
#else
  void send_cluster_message(
    int osd, MessageRef m, epoch_t epoch, bool share_map_update=false) override {
    dout(0) << "send_cluster_message to " << osd << " " << m << " epoch " << epoch << dendl;
    if (messenger) {
      // Use MockMessenger for EventLoop-based routing with epoch tracking
      messenger->send_message(pg_whoami.osd, osd, m.detach());
    } else {
      // Fall back to direct message queue for TestPeeringState compatibility
      messages[osd].push_back(m);
    }
    messages_sent++;
  }
#endif

  void set_messenger(MockMessenger* m) {
    messenger = m;
  }
  
  void set_event_loop(EventLoop* el) {
    event_loop = el;
  }
  
  void set_fixture(ECPeeringTestFixture* f) {
    fixture = f;
  }

  void send_pg_created(pg_t pgid) override {
    pg_created_sent = true;
  }
  ceph::signedspan get_mnow() const override {
    return ceph::signedspan::zero();
  }

  HeartbeatStampsRef get_hb_stamps(int peer) override {
    if (peer >= (int)hb_stamps.size()) {
      hb_stamps.resize(peer + 1);
    }
    if (!hb_stamps[peer]) {
      hb_stamps[peer] = ceph::make_ref<HeartbeatStamps>(peer);
    }
    return hb_stamps[peer];
  }

  void schedule_renew_lease(epoch_t plr, ceph::timespan delay) override {
    renew_lease_scheduled = true;
  }

  void queue_check_readable(epoch_t lpr, ceph::timespan delay) override {
    check_readable_queued = true;
  }

  void recheck_readable() override {
    readable_rechecked = true;
  }

  unsigned get_target_pg_log_entries() const override {
    return target_pg_log_entries;
  }


  bool try_flush_or_schedule_async() override {
    return true;
  }

  void start_flush_on_transaction(ObjectStore::Transaction &t) override {
    flush_started = true;
  }

  void on_flushed() override {
    flushed = true;
  }

  void schedule_event_after(
    PGPeeringEventRef event,
    float delay) override {
    stalled_events.push_back(std::move(event));
    events_scheduled++;
  }

  void request_local_background_io_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override;

  void update_local_background_io_priority(
    unsigned priority) override {
    io_priority_updated = true;
  }

  void cancel_local_background_io_reservation() override {
    io_reservation_cancelled = true;
  }

  void request_remote_recovery_reservation(
    unsigned priority,
    PGPeeringEventURef on_grant,
    PGPeeringEventURef on_preempt) override;

  void cancel_remote_recovery_reservation() override {
    remote_recovery_reservation_cancelled = true;
  }

  void schedule_event_on_commit(
    ObjectStore::Transaction &t,
    PGPeeringEventRef on_commit) override;

  void update_heartbeat_peers(std::set<int> peers) override {
    heartbeat_peers_updated = true;
  }

  void set_probe_targets(const std::set<pg_shard_t> &probe_set) override {
    probe_targets_set = true;
  }

  void clear_probe_targets() override {
    probe_targets_cleared = true;
  }

  void queue_want_pg_temp(const std::vector<int> &wanted) override {
    pg_temp_wanted = true;
    next_acting = wanted;
  }

  void clear_want_pg_temp() override {
    pg_temp_cleared = true;
  }

#if POOL_MIGRATION
  void send_pg_migrated_pool() override {
    pg_migrated_pool_sent = true;
  }
#endif

  void publish_stats_to_osd() override {
    stats_published = true;
  }

  void clear_publish_stats() override {
    stats_cleared = true;
  }

  void check_recovery_sources(const OSDMapRef& newmap) override {
    recovery_sources_checked = true;
  }

  void check_blocklisted_watchers() override {
    blocklisted_watchers_checked = true;
  }

  void clear_primary_state() override {
    primary_state_cleared = true;
  }

  void on_active_exit() override {
    active_exited = true;
  }

  void on_active_actmap() override {
    active_actmap_called = true;
  }

  void on_active_advmap(const OSDMapRef &osdmap) override {
    active_advmap_called = true;
  }

  void on_backfill_reserved() override {
    backfill_reserved = true;
  }

  void on_recovery_reserved() override {
    recovery_reserved = true;
  }

  Context *on_clean() override {
    clean_called = true;
    return nullptr;
  }

  void on_activate(interval_set<snapid_t> snaps) override {
    activate_called = true;
  }

  void on_change(ObjectStore::Transaction &t) override {
    first_write_in_interval = true;
    change_called = true;
  }

  std::pair<ghobject_t, bool> do_delete_work(
    ObjectStore::Transaction &t, ghobject_t _next) override {
    delete_work_done = true;
    return std::make_pair(ghobject_t(), true);
  }

  void clear_ready_to_merge() override {
    ready_to_merge_cleared = true;
  }

  void set_not_ready_to_merge_target(pg_t pgid, pg_t src) override {
    not_ready_to_merge_target_set = true;
  }

  void set_not_ready_to_merge_source(pg_t pgid) override {
    not_ready_to_merge_source_set = true;
  }

  void set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec) override {
    ready_to_merge_target_set = true;
  }

  void set_ready_to_merge_source(eversion_t lu) override {
    ready_to_merge_source_set = true;
  }

  epoch_t cluster_osdmap_trim_lower_bound() override {
    return 1;
  }

  void on_backfill_suspended() override {
    backfill_suspended = true;
  }

  void on_recovery_cancelled() override {
    recovery_cancelled = true;
  }

#if POOL_MIGRATION
  void on_pool_migration_reserved() override {
    pool_migration_reserved = true;
  }
#endif

#if POOL_MIGRATION
  void on_pool_migration_suspended() override {
    pool_migration_suspended = true;
  }
#endif

  bool try_reserve_recovery_space(
    int64_t primary_num_bytes,
    int64_t local_num_bytes) override {
    recovery_space_reserved = true;
    if (inject_fail_reserve_recovery_space) {
      return false;
    }
    return true;
  }

  void unreserve_recovery_space() override {
    recovery_space_unreserved = true;
  }

  PGLog::LogEntryHandlerRef get_log_handler(
    ObjectStore::Transaction &t) override {
    return std::make_unique<MockPGLogEntryHandler>(backend.get(), &t);
  }

  void rebuild_missing_set_with_deletes(PGLog &pglog) override {
    missing_set_rebuilt = true;
  }

  PerfCounters &get_peering_perf() override {
    return *recoverystate_perf;
  }

  PerfCounters &get_perf_logger() override {
    return *logger_perf;
  }

  void log_state_enter(const char *state) override {
    last_state_entered = std::string(state);
    state_entered = true;
  }

  void log_state_exit(
    const char *state_name, utime_t enter_time,
    uint64_t events, utime_t event_dur) override {
    last_state_exited = std::string(state_name);
    state_exited = true;
  }

  void dump_recovery_info(ceph::Formatter *f) const override {
    recovery_info_dumped = true;
  }

  OstreamTemp get_clog_info() override {
    return OstreamTemp(CLOG_INFO, nullptr);
  }

  OstreamTemp get_clog_error() override {
    return OstreamTemp(CLOG_ERROR, nullptr);
  }

  OstreamTemp get_clog_debug() override {
    return OstreamTemp(CLOG_DEBUG, nullptr);
  }

  void on_activate_complete() override;

  void on_activate_committed() override {
    activate_committed_called = true;
  }

  void on_new_interval() override {
    new_interval_called = true;
  }

  void on_pool_change() override {
    pool_changed = true;
  }

  void on_role_change() override {
    role_changed = true;
  }

  void on_removal(ObjectStore::Transaction &t) override {
    removal_called = true;
  }

  unsigned target_pg_log_entries = 100;
  bool renew_lease_scheduled = false;
  bool check_readable_queued = false;
  bool readable_rechecked = false;
  bool heartbeat_peers_updated = false;
  bool probe_targets_set = false;
  bool probe_targets_cleared = false;
  bool pg_temp_wanted = false;
  bool pg_temp_cleared = false;
  bool pg_migrated_pool_sent = false;
  bool stats_published = false;
  bool stats_cleared = false;
  bool recovery_sources_checked = false;
  bool blocklisted_watchers_checked = false;
  bool primary_state_cleared = false;
  bool delete_work_done = false;
  bool ready_to_merge_cleared = false;
  bool not_ready_to_merge_target_set = false;
  bool not_ready_to_merge_source_set = false;
  bool ready_to_merge_target_set = false;
  bool ready_to_merge_source_set = false;
  bool backfill_suspended = false;
  bool recovery_cancelled = false;
  bool pool_migration_reserved = false;
  bool pool_migration_suspended = false;
  bool recovery_space_reserved = false;
  bool recovery_space_unreserved = false;
  bool missing_set_rebuilt = false;
  std::string last_state_entered;
  bool state_entered = false;
  std::string last_state_exited;
  bool state_exited = false;
  mutable bool recovery_info_dumped = false;
  epoch_t current_epoch = 1;
  uint64_t snap_trimq_size = 0;
  bool prepare_write_called = false;
  bool scrub_requested_called = false;
  bool pg_created_sent = false;
  bool flush_started = false;
  bool flushed = false;
  bool io_priority_updated = false;
  bool io_reservation_cancelled = false;
  bool remote_recovery_reservation_cancelled = false;
  bool active_exited = false;
  bool active_actmap_called = false;
  bool active_advmap_called = false;
  bool backfill_reserved = false;
  bool backfill_cancelled = false;
  bool recovery_reserved = false;
  bool clean_called = false;
  bool activate_called = false;
  bool activate_complete_called = false;
  bool change_called = false;
  bool activate_committed_called = false;
  bool new_interval_called = false;
  bool primary_status_changed = false;
  bool pool_changed = false;
  bool role_changed = false;
  bool removal_called = false;
  bool shutdown_called = false;
  int messages_sent = 0;
  int events_scheduled = 0;
  int io_reservations_requested = 0;
  int remote_recovery_reservations_requested = 0;
  int events_on_commit_scheduled = 0;
  bool first_write_in_interval = false;
};

