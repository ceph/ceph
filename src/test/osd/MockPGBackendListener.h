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

#include <functional>
#include <vector>
#include <map>
#include "osd/PGBackend.h"
#include "osd/ECBackend.h"
#include "osd/PGLog.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "common/ceph_context.h"
#include "common/TrackedOp.h"
#include "common/perf_counters.h"
#include "messages/MOSDPGPush.h"
#include "os/ObjectStore.h"
#include "global/global_context.h"
#include "test/osd/MockConnection.h"
#include "test/osd/EventLoop.h"
#include "test/osd/MockMessenger.h"
#include "osd/OpRequest.h"

// MockPGBackendListener - mock PGBackend::Listener and ECListener for multi-instance testing.
class MockPGBackendListener : public PGBackend::Listener, public ECListener {
public:
  pg_info_t info;
  OSDMapRef osdmap;
  int64_t pool_id;
  PGLog log;
  DoutPrefixProvider *dpp;
  pg_shard_t pg_whoami;
  std::set<pg_shard_t> shardset;
  
  // Pointer to PeeringState for tests that use full peering
  PeeringState *peering_state = nullptr;
  
  shard_id_set acting_recovery_backfill_shard_id_set;
  std::map<pg_shard_t, pg_info_t> shard_info;
  std::map<pg_shard_t, pg_missing_t> shard_missing;
  std::map<hobject_t, std::set<pg_shard_t>> missing_loc_shards;
  pg_missing_tracker_t local_missing;
  
  std::vector<MessageRef> sent_messages;
  std::vector<std::pair<int, MessageRef>> sent_messages_with_dest;
  
  ObjectStore *store = nullptr;
  ObjectStore::CollectionHandle ch;
  EventLoop *event_loop = nullptr;
  MockMessenger *messenger = nullptr;
  OpTracker *op_tracker = nullptr;
  PerfCounters *perf_logger = nullptr;

  // Recovery callback tracking
  struct RecoveryCallbackTracker {
    int on_local_recover_calls = 0;
    std::vector<hobject_t> on_local_recover_objects;

    std::map<pg_shard_t, int> on_peer_recover_calls;
    std::vector<std::pair<pg_shard_t, hobject_t>> on_peer_recover_objects;

    int on_global_recover_calls = 0;
    std::vector<hobject_t> on_global_recover_objects;

    void reset() {
      on_local_recover_calls = 0;
      on_local_recover_objects.clear();
      on_peer_recover_calls.clear();
      on_peer_recover_objects.clear();
      on_global_recover_calls = 0;
      on_global_recover_objects.clear();
    }
  };
  RecoveryCallbackTracker recovery_tracker;

  MockPGBackendListener(OSDMapRef osdmap, int64_t pool_id, DoutPrefixProvider *dpp, pg_shard_t pg_whoami, PeeringState *ps = nullptr) :
    osdmap(osdmap), pool_id(pool_id), log(g_ceph_context), dpp(dpp), pg_whoami(pg_whoami), peering_state(ps) {
    // Create a full OSD PerfCounters using the standard build_osd_logger function.
    // This prevents null pointer dereferences when ReplicatedBackend calls get_logger()->inc().
    perf_logger = build_osd_logger(g_ceph_context);
  }
  
  ~MockPGBackendListener() {
    if (perf_logger) {
      delete perf_logger;
      perf_logger = nullptr;
    }
  }
  
  void set_store(ObjectStore *s, ObjectStore::CollectionHandle c) {
    store = s;
    ch = c;
  }
  
  void set_event_loop(EventLoop *loop) {
    event_loop = loop;
  }
  
  void set_op_tracker(OpTracker *tracker) {
    op_tracker = tracker;
  }
  
  void set_peering_state(PeeringState *ps) {
    peering_state = ps;
  }
  
  void set_messenger(MockMessenger *m) {
    messenger = m;
  }

  // Debugging
  DoutPrefixProvider *get_dpp() override {
    return dpp;
  }

  // Recovery callbacks
  void on_local_recover(
    const hobject_t &oid,
    const ObjectRecoveryInfo &_recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ObjectStore::Transaction *t) override {
    recovery_tracker.on_local_recover_calls++;
    recovery_tracker.on_local_recover_objects.push_back(oid);

    // Make a copy of recovery_info as we may need to modify it
    ObjectRecoveryInfo recovery_info(_recovery_info);
    if (!is_delete && peering_state &&
        peering_state->get_pg_log().get_missing().is_missing(recovery_info.soid) &&
        peering_state->get_pg_log().get_missing().get_items().find(recovery_info.soid)->second.need > recovery_info.version) {
      ceph_assert(pgb_is_primary());
    }

    // Call into PeeringState to update recovery state
    if (peering_state) {
      peering_state->recover_got(recovery_info.soid, recovery_info.version, is_delete, *t);
    }

    // Register transaction callbacks (similar to PrimaryLogPG::on_local_recover)
    // Note: In the mock, we don't track active_pushes or handle all the same callbacks,
    // but we should register basic callbacks if needed by tests
    if (peering_state && pgb_is_primary()) {
      if (!is_delete && obc) {
        obc->obs.exists = true;
        obc->obs.oi = recovery_info.oi;
      }
    }
  }

  void on_global_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    bool is_delete) override {
    recovery_tracker.on_global_recover_calls++;
    recovery_tracker.on_global_recover_objects.push_back(oid);

    // Call into PeeringState to mark object as fully recovered
    if (peering_state) {
      peering_state->object_recovered(oid, stat_diff);
    }
  }

  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info) override {
    recovery_tracker.on_peer_recover_calls[peer]++;
    recovery_tracker.on_peer_recover_objects.push_back({peer, oid});

    // Call into PeeringState to update peer missing state
    if (peering_state) {
      peering_state->on_peer_recover(peer, oid, recovery_info.version);
    }
  }

  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) override {
    if (peering_state) {
      peering_state->begin_peer_recover(peer, oid);
    }
  }

  void apply_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats) override {
    // Mimic PrimaryLogPG::apply_stats() - apply stats to PeeringState
    if (peering_state) {
      peering_state->apply_op_stats(soid, delta_stats);
    }
  }

  void on_failed_pull(
    const std::set<pg_shard_t> &from,
    const hobject_t &soid,
    const eversion_t &v) override {
  }

  void cancel_pull(const hobject_t &soid) override {
  }

  void remove_missing_object(
    const hobject_t &oid,
    eversion_t v,
    Context *on_complete) override {
  }

  // Locking
  void pg_lock() override {}
  void pg_unlock() override {}
  void pg_add_ref() override {}
  void pg_dec_ref() override {}

  // Context wrapping
  Context *bless_context(Context *c) override {
    return c;
  }

  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override {
    return c;
  }

  GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override {
    return c;
  }

  // Routes messages through MockMessenger for asynchronous message processing.
  void send_message(int to_osd, Message *m) override {
    // Callers that pass `new T(...)` hand off a +1 refcount; consume it here
    // with add_ref=false so the original +1 is owned by `mref`/the storage
    // vectors and is correctly released when those go out of scope.
    MessageRef mref(m, /*add_ref=*/false);
    sent_messages.push_back(mref);
    sent_messages_with_dest.push_back({to_osd, mref});
    
    if (messenger) {
      // Capture the sender's OSD ID
      int from_osd = pg_whoami.osd;
      
      // Use MockMessenger to route the message with epoch tracking
      // MockMessenger handles encoding, MockConnection setup, and epoch capture
      messenger->send_message(from_osd, to_osd, m);
    }
  }

  void queue_transaction(
    ObjectStore::Transaction&& t,
    OpRequestRef op = OpRequestRef()) override {
    std::vector<ObjectStore::Transaction> tls;
    tls.push_back(std::move(t));
    queue_transactions(tls, op);
  }

  void queue_transactions(
    std::vector<ObjectStore::Transaction>& tls,
    OpRequestRef op = OpRequestRef()) override {
    if (event_loop && store && ch) {
      // Steal the Context callbacks from the transactions before calling MemStore.
      // This allows the test harness to manage the context callbacks itself instead of using
      // a Finisher thread. This keeps the test harness single threaded and gives more
      // control for ordering async replies.
      Context *on_apply = nullptr;
      Context *on_apply_sync = nullptr;
      Context *on_commit = nullptr;
      ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit, &on_apply_sync);

      // Execute transactions through the store (without contexts - we stole them)
      store->queue_transactions(ch, tls, TrackedOpRef(), nullptr);

      // Apply the on_apply_sync synchronously. This is what queue_transactions
      // would do anyway.
      // NOTE: Memstore will panic rather than fail
      if (on_apply_sync) {
        on_apply_sync->complete(0);
      }

      if (on_apply) {
        event_loop->schedule_transaction(pg_whoami.osd, [on_apply]() mutable {
          on_apply->complete(0);
        });
      }
      if (on_commit) {
        event_loop->schedule_transaction(pg_whoami.osd, [on_commit]() mutable {
          on_commit->complete(0);
        });
      }
    }
  }

  epoch_t get_interval_start_epoch() const override {
    if (peering_state) {
      return peering_state->get_info().history.same_interval_since;
    }
    return 1;
  }

  epoch_t get_last_peering_reset_epoch() const override {
    if (peering_state) {
      return peering_state->get_last_peering_reset();
    }
    return 1;
  }

  // Shard information
  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    if (peering_state) {
      return peering_state->get_acting_recovery_backfill();
    }
    return shardset;
  }

  const shard_id_set &get_acting_recovery_backfill_shard_id_set() const override {
    if (peering_state) {
      return peering_state->get_acting_recovery_backfill_shard_id_set();
    }
    return acting_recovery_backfill_shard_id_set;
  }

  const std::set<pg_shard_t> &get_acting_shards() const override {
    if (peering_state) {
      return peering_state->get_actingset();
    }
    return shardset;
  }

  const std::set<pg_shard_t> &get_backfill_shards() const override {
    if (peering_state) {
      return peering_state->get_backfill_targets();
    }
    return shardset;
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return out << "MockPGBackend ";
  }

  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    if (peering_state) {
      return peering_state->get_missing_loc().get_missing_locs();
    }
    return missing_loc_shards;
  }

  const pg_missing_tracker_t &get_local_missing() const override {
    if (peering_state) {
      return peering_state->get_pg_log().get_missing();
    }
    return local_missing;
  }

  void add_local_next_event(const pg_log_entry_t& e) override {
    if (peering_state) {
      peering_state->add_local_next_event(e);
    }
  }

  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    if (peering_state) {
      return peering_state->get_peer_missing();
    }
    return shard_missing;
  }

  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    if (peering_state) {
      auto m = maybe_get_shard_missing(peer);
      ceph_assert(m);
      return *m;
    }
    return local_missing;
  }

  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    if (peering_state) {
      return peering_state->get_peer_info();
    }
    return shard_info;
  }

  const PGLog &get_log() const override {
    if (peering_state) {
      return peering_state->get_pg_log();
    }
    return log;
  }

  bool pgb_is_primary() const override {
    // For peering tests, use the PeeringState's view of primary
    if (peering_state) {
      return peering_state->is_primary();
    }
    
    // For basic tests without peering, query the OSDMap to determine primary
    // This uses pg_temp if set, otherwise uses the CRUSH mapping
    std::vector<int> acting;
    int acting_primary = -1;
    osdmap->pg_to_acting_osds(info.pgid.pgid, &acting, &acting_primary);
    
    return pg_whoami.osd == acting_primary;
  }

  const OSDMapRef& pgb_get_osdmap() const override {
    return osdmap;
  }

  epoch_t pgb_get_osdmap_epoch() const override {
    return osdmap->get_epoch();
  }

  const pg_info_t &get_info() const override {
    // When PeeringState is available, use its pg_info_t as the single source of truth
    if (peering_state) {
      return peering_state->get_info();
    }
    return info;
  }

  const pg_pool_t &get_pool() const override {
    const pg_pool_t *p = osdmap->get_pg_pool(pool_id);
    ceph_assert(p != nullptr);
    return *p;
  }

  eversion_t get_pg_committed_to() const override {
    if (peering_state) {
      return peering_state->get_pg_committed_to();
    }
    return eversion_t();
  }

  ObjectContextRef get_obc(
    const hobject_t &hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return ObjectContextRef();
  }

  bool try_lock_for_read(
    const hobject_t &hoid,
    ObcLockManager &manager) override {
    return true;
  }

  void release_locks(ObcLockManager &manager) override {
  }

  void op_applied(const eversion_t &applied_version) override {
  }

  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override {
    // If we're sending to ourselves (primary), always send
    if (peer == pg_whoami)
      return true;

    // If we have a peering_state, use it to check async_recovery_targets
    if (peering_state) {
      // Check if peer is an async_recovery_target with this object missing
      if (peering_state->is_async_recovery_target(peer)) {
        const pg_missing_t &peer_missing = peering_state->get_peer_missing(peer);
        if (peer_missing.is_missing(hoid)) {
          // Object is missing on async_recovery_target, send empty transaction
          return false;
        }
      }

      // Check backfill logic
      if (peering_state->is_backfill_target(peer)) {
        const pg_info_t &peer_info = peering_state->get_peer_info(peer);
        // If object is beyond peer's last_backfill, don't send full transaction
        if (hoid > peer_info.last_backfill) {
          return false;
        }
      }
    }

    return true;
  }

  bool pg_is_undersized() const override {
    return false;
  }

  bool pg_is_repair() const override {
    return false;
  }

#if POOL_MIGRATION
  void update_migration_watermark(const hobject_t &watermark) override {
  }
#endif

#if POOL_MIGRATION
  std::optional<hobject_t> consider_updating_migration_watermark(
    std::set<hobject_t> &deleted) override {
    return std::nullopt;
  }
#endif

  void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const std::optional<pg_hit_set_history_t> &hset_history,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const eversion_t &pg_committed_to,
    bool transaction_applied,
    ObjectStore::Transaction &t,
    bool async = false) override {
    // If we have a PeeringState, append the log entries to it
    // This creates proper integration between backend operations and peering state
    if (peering_state && !logv.empty()) {
      peering_state->append_log(
        std::move(logv),
        trim_to,
        roll_forward_to,
        pg_committed_to,
        t,
        transaction_applied,
        async);
    }
  }

  void pgb_set_object_snap_mapping(
    const hobject_t &soid,
    const std::set<snapid_t> &snaps,
    ObjectStore::Transaction *t) override {
  }

  void pgb_clear_object_snap_mapping(
    const hobject_t &soid,
    ObjectStore::Transaction *t) override {
  }

  void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) override {
    if (peering_state) {
      peering_state->update_peer_last_complete_ondisk(fromosd, lcod);
    }
  }

  void update_last_complete_ondisk(eversion_t lcod) override {
    if (peering_state) {
      peering_state->update_last_complete_ondisk(lcod);
    }
  }

  void update_pct(eversion_t pct) override {
    if (peering_state) {
      peering_state->update_pct(pct);
    }
  }

  void update_stats(const pg_stat_t &stat) override {
    if (peering_state) {
      peering_state->update_stats(
        [&stat](auto &history, auto &stats) {
          stats = stat;
          return false;
        });
    }
  }

  void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&> *c,
    uint64_t cost) override {
  }

  common::intrusive_timer &get_pg_timer() override {
    ceph_abort("Not supported");
  }

  pg_shard_t whoami_shard() const override {
    return pg_whoami;
  }

  spg_t primary_spg_t() const override {
    return spg_t();
  }

  pg_shard_t primary_shard() const override {
    if (peering_state) {
      return peering_state->get_primary();
    }
    
    // Query the OSDMap to get the current primary
    pg_t pgid = info.pgid.pgid;
    std::vector<int> acting;
    int acting_primary = -1;
    osdmap->pg_to_acting_osds(pgid, &acting, &acting_primary);
    
    // For EC pools, the primary shard ID matches the OSD ID in the acting set
    // For replicated pools, use NO_SHARD
    if (pg_whoami.shard != shard_id_t::NO_SHARD) {
      // EC pool: find the shard ID of the acting primary in the acting set
      shard_id_t primary_shard_id = shard_id_t::NO_SHARD;
      for (size_t i = 0; i < acting.size(); i++) {
        if (acting[i] == acting_primary) {
          primary_shard_id = shard_id_t(i);
          break;
        }
      }
      return pg_shard_t(acting_primary, primary_shard_id);
    } else {
      // Replicated pool: use NO_SHARD
      return pg_shard_t(acting_primary, shard_id_t::NO_SHARD);
    }
  }

  uint64_t min_peer_features() const override {
    if (peering_state) {
      return peering_state->get_min_peer_features();
    }
    return CEPH_FEATURES_ALL;
  }

  uint64_t min_upacting_features() const override {
    if (peering_state) {
      return peering_state->get_min_upacting_features();
    }
    return CEPH_FEATURES_ALL;
  }

  pg_feature_vec_t get_pg_acting_features() const override {
    if (peering_state) {
      return peering_state->get_pg_acting_features();
    }
    return pg_feature_vec_t();
  }

  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version) override {
    return hobject_t();
  }

  void send_message_osd_cluster(
    int peer, Message *m, epoch_t from_epoch) override {
    send_message(peer, m);
  }

  void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) override {
    for (auto& [osd, m] : messages) {
      send_message(osd, m);
    }
  }

  void send_message_osd_cluster(MessageRef m, Connection *con) override {
    MockConnection* mock_con = dynamic_cast<MockConnection*>(con);
    send_message(mock_con->get_peer_osd(), m.get());
  }

  void send_message_osd_cluster(Message *m, const ConnectionRef& con) override {
    MockConnection* mock_con = dynamic_cast<MockConnection*>(con.get());
    send_message(mock_con->get_peer_osd(), m);
  }

  void start_mon_command(
    std::vector<std::string>&& cmd, bufferlist&& inbl,
    bufferlist *outbl, std::string *outs,
    Context *onfinish) override {
  }

  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) override {
    return nullptr;
  }

  entity_name_t get_cluster_msgr_name() override {
    return entity_name_t();
  }

  PerfCounters *get_logger() override {
    return perf_logger;
  }

  ceph_tid_t get_tid() override {
    return 0;
  }

  OstreamTemp clog_error() override {
    return OstreamTemp(CLOG_ERROR, nullptr);
  }

  OstreamTemp clog_warn() override {
    return OstreamTemp(CLOG_WARN, nullptr);
  }

  bool check_failsafe_full() override {
    return false;
  }

  void inc_osd_stat_repaired() override {
  }

  bool pg_is_remote_backfilling() override {
    return false;
  }

  void pg_add_local_num_bytes(int64_t num_bytes) override {
  }

  void pg_sub_local_num_bytes(int64_t num_bytes) override {
  }

  void pg_add_num_bytes(int64_t num_bytes) override {
  }

  void pg_sub_num_bytes(int64_t num_bytes) override {
  }

  bool maybe_preempt_replica_scrub(const hobject_t& oid) override {
    return false;
  }
  void add_temp_obj(const hobject_t &oid) override {
  }

  void clear_temp_obj(const hobject_t &oid) override {
  }

  const pg_missing_const_i * maybe_get_shard_missing(
    pg_shard_t peer) const override {
    if (peering_state) {
      if (peer == primary_shard()) {
        return &peering_state->get_pg_log().get_missing();
      } else {
        auto i = peering_state->get_peer_missing().find(peer);
        if (i == peering_state->get_peer_missing().end()) {
          return nullptr;
        } else {
          return &(i->second);
        }
      }
    }
    return &local_missing;
  }

  const pg_info_t &get_shard_info(pg_shard_t peer) const override {
    if (peering_state) {
      if (peer == peering_state->get_primary()) {
        return peering_state->get_info();
      } else {
        auto i = peering_state->get_peer_info().find(peer);
        ceph_assert(i != peering_state->get_peer_info().end());
        return i->second;
      }
    }
    
    auto it = shard_info.find(peer);
    if (it != shard_info.end()) {
      return it->second;
    }
    return info;
  }

  bool is_missing_object(const hobject_t& oid) const override {
    // Check if object is in the missing set (same as PrimaryLogPG::is_missing_object)
    if (peering_state) {
      return peering_state->get_pg_log().get_missing().get_items().count(oid);
    }
    // Fallback for tests without peering_state
    return log.get_missing().get_items().count(oid);
  }
  void send_message_osd_cluster(
    int osd, MOSDPGPush* msg, epoch_t from_epoch) override {
    send_message(osd, msg);
  }

  struct ECListener *get_eclistener() override {
    return static_cast<ECListener *>(this);
  }
};

