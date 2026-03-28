// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
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
#include "common/ceph_context.h"
#include "common/TrackedOp.h"
#include "common/perf_counters.h"
#include "messages/MOSDPGPush.h"
#include "os/ObjectStore.h"
#include "global/global_context.h"
#include "test/osd/MockConnection.h"
#include "test/osd/EventLoop.h"
#include "osd/OpRequest.h"

// MockPGBackendListener - Comprehensive mock implementation of PGBackend::Listener and ECListener
// Provides full message routing, transaction scheduling, and state tracking for multi-instance testing
class MockPGBackendListener : public PGBackend::Listener, public ECListener {
public:
  pg_info_t info;
  OSDMapRef osdmap;
  int64_t pool_id;
  PGLog log;
  DoutPrefixProvider *dpp;
  pg_shard_t pg_whoami;
  pg_shard_t primary;  // The primary shard (typically shard 0)
  std::set<pg_shard_t> shardset;
  shard_id_set acting_recovery_backfill_shard_id_set;
  std::map<pg_shard_t, pg_info_t> shard_info;
  std::map<pg_shard_t, pg_missing_t> shard_missing;
  std::map<hobject_t, std::set<pg_shard_t>> missing_loc_shards;
  pg_missing_tracker_t local_missing;
  
  // Message tracking for EC operations
  std::vector<MessageRef> sent_messages;
  std::vector<std::pair<int, MessageRef>> sent_messages_with_dest;  // Track destination OSD
  
  // ObjectStore for actually executing transactions
  ObjectStore *store = nullptr;
  ObjectStore::CollectionHandle ch;
  
  // EventLoop for scheduling work
  EventLoop *event_loop = nullptr;
  
  // Backend for handling messages - this shard's own backend
  std::function<bool(OpRequestRef)> handle_message_callback;
  
  // Message routing table - maps OSD ID to message handler
  std::map<int, std::function<bool(OpRequestRef)>> *message_router = nullptr;
  
  // OpTracker for creating OpRequests
  OpTracker *op_tracker = nullptr;
  
  // Mock PerfCounters for performance tracking
  PerfCounters *perf_logger = nullptr;

  MockPGBackendListener(OSDMapRef osdmap, int64_t pool_id, DoutPrefixProvider *dpp, pg_shard_t pg_whoami, pg_shard_t primary = pg_shard_t(0, shard_id_t::NO_SHARD)) :
    osdmap(osdmap), pool_id(pool_id), log(g_ceph_context), dpp(dpp), pg_whoami(pg_whoami), primary(primary) {
    // Create a full OSD PerfCounters using the standard build_osd_logger function
    // This prevents null pointer dereferences when ReplicatedBackend calls get_logger()->inc()
    // Using the real builder ensures all counters are properly initialized
    perf_logger = build_osd_logger(g_ceph_context);
  }
  
  ~MockPGBackendListener() {
    // Clean up the PerfCounters
    if (perf_logger) {
      delete perf_logger;
      perf_logger = nullptr;
    }
  }
  
  // Set the ObjectStore to use for executing transactions
  void set_store(ObjectStore *s, ObjectStore::CollectionHandle c) {
    store = s;
    ch = c;
  }
  
  // Set the EventLoop to use for scheduling work
  void set_event_loop(EventLoop *loop) {
    event_loop = loop;
  }
  
  // Set the OpTracker for creating OpRequests
  void set_op_tracker(OpTracker *tracker) {
    op_tracker = tracker;
  }
  
  // Set callback for handling messages
  void set_handle_message_callback(std::function<bool(OpRequestRef)> cb) {
    handle_message_callback = cb;
  }
  
  // Set message router for routing messages to correct shards
  void set_message_router(std::map<int, std::function<bool(OpRequestRef)>> *router) {
    message_router = router;
  }

  // Debugging
  DoutPrefixProvider *get_dpp() override {
    return dpp;
  }

  // Recovery callbacks
  void on_local_recover(
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ObjectStore::Transaction *t) override {
  }

  void on_global_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    bool is_delete) override {
  }

  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info) override {
  }

  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) override {
  }

  void apply_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats) override {
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

  // Messaging - Routes messages through EventLoop for asynchronous EC message processing
  void send_message(int to_osd, Message *m) override {
    // Store message for inspection/debugging - MessageRef takes ownership
    MessageRef mref(m);
    sent_messages.push_back(mref);
    sent_messages_with_dest.push_back({to_osd, mref});
    
    if (event_loop && op_tracker && message_router) {
      // Capture the sender's OSD ID
      int from_osd = pg_whoami.osd;
      
      // IMPORTANT: Encode the message payload to simulate network transmission
      // This ensures that txn_payload is moved to the middle section for MOSDRepOp messages
      // Without this, Transaction::decode will fail because the message structure is incomplete
      mref->encode_payload(CEPH_FEATURES_ALL);
      
      // Schedule a lambda to process this message through the destination shard's EC backend
      event_loop->schedule_ec_message(to_osd, [this, mref, to_osd, from_osd]() {
        // Create OpRequest for the message
        if (!mref->get_connection()) {
          // Set connection peer to the SENDER, not the destination
          ConnectionRef conn = new MockConnection(from_osd);
          mref->set_connection(conn);
        }
        OpRequestRef op = op_tracker->create_request<OpRequest>(mref.get());
        
        // Route to the correct shard's backend using the message router
        auto it = message_router->find(to_osd);
        if (it != message_router->end()) {
          it->second(op);
        }
      });
    }
  }

  void queue_transaction(
    ObjectStore::Transaction&& t,
    OpRequestRef op = OpRequestRef()) override {
    if (event_loop && store && ch) {
      // Steal the Context callbacks from the transaction before calling MemStore.
      // This allows the test harness to manage the context callbacks itself instead of using
      // a Finisher thread. This keeps the test harness single threaded and gives more
      // control for ordering async replies.
      Context *on_apply = nullptr;
      Context *on_apply_sync = nullptr;
      Context *on_commit = nullptr;
      std::vector<ObjectStore::Transaction> tls;
      tls.push_back(std::move(t));
      ObjectStore::Transaction::collect_contexts(tls, &on_apply, &on_commit, &on_apply_sync);
      
      // Schedule a lambda to execute the transaction
      event_loop->schedule_transaction(pg_whoami.osd, [this, tls = std::move(tls), on_apply, on_apply_sync, on_commit]() mutable {
        // Execute transaction through the store (without contexts - we stole them)
        store->queue_transaction(ch, std::move(tls[0]), TrackedOpRef(), nullptr);
        
        // Execute the stolen contexts immediately in the EventLoop
        if (on_apply_sync) {
          on_apply_sync->complete(0);
        }
        if (on_apply) {
          on_apply->complete(0);
        }
        if (on_commit) {
          on_commit->complete(0);
        }
      });
    }
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

      // Apply the on_apply_sync synchonously. This is what queue_transactions
      // would do anyway.
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
    return 1;
  }

  epoch_t get_last_peering_reset_epoch() const override {
    return 1;
  }

  // Shard information
  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return shardset;
  }

  const shard_id_set &get_acting_recovery_backfill_shard_id_set() const {
    return acting_recovery_backfill_shard_id_set;
  }

  const std::set<pg_shard_t> &get_acting_shards() const override {
    return shardset;
  }

  const std::set<pg_shard_t> &get_backfill_shards() const override {
    return shardset;
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return out << "MockPGBackend ";
  }

  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    return missing_loc_shards;
  }

  const pg_missing_tracker_t &get_local_missing() const override {
    return local_missing;
  }

  void add_local_next_event(const pg_log_entry_t& e) override {
  }

  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    return shard_missing;
  }

  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    return local_missing;
  }

  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    return shard_info;
  }

  const PGLog &get_log() const override {
    return log;
  }

  bool pgb_is_primary() const override {
    // Returns true if this shard is the primary shard
    // Matches PeeringState::is_primary() behavior (pg_whoami == primary)
    return pg_whoami == primary;
  }

  const OSDMapRef& pgb_get_osdmap() const override {
    return osdmap;
  }

  epoch_t pgb_get_osdmap_epoch() const override {
    return osdmap->get_epoch();
  }

  const pg_info_t &get_info() const override {
    return info;
  }

  const pg_pool_t &get_pool() const override {
    const pg_pool_t *p = osdmap->get_pg_pool(pool_id);
    ceph_assert(p != nullptr);
    return *p;
  }

  eversion_t get_pg_committed_to() const override {
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
  }

  void update_last_complete_ondisk(eversion_t lcod) override {
  }

  void update_pct(eversion_t pct) override {
  }

  void update_stats(const pg_stat_t &stat) override {
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
    return primary;
  }

  uint64_t min_peer_features() const override {
    return CEPH_FEATURES_ALL;
  }

  uint64_t min_upacting_features() const override {
    return CEPH_FEATURES_ALL;
  }

  pg_feature_vec_t get_pg_acting_features() const override {
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
    // Loop through the messages vector and track each message
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
    return &local_missing;
  }

  const pg_info_t &get_shard_info(pg_shard_t peer) const override {
    auto it = shard_info.find(peer);
    if (it != shard_info.end()) {
      return it->second;
    }
    return info;
  }

  bool is_missing_object(const hobject_t& oid) const override {
    return false;
  }
  void send_message_osd_cluster(
    int osd, MOSDPGPush* msg, epoch_t from_epoch) override {
    send_message(osd, msg);
  }

  struct ECListener *get_eclistener() override {
    return this;
  }
};

