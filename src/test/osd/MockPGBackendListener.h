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

#include <map>
#include <set>
#include <optional>
#include "osd/PGBackend.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osd/PGLog.h"
#include "common/intrusive_timer.h"
#include "common/ostream_temp.h"
#include "global/global_context.h"
#include "os/ObjectStore.h"

// MockPGBackendListener - simple stub for PGBackend::Listener
class MockPGBackendListener : public PGBackend::Listener {
public:
  pg_info_t info;
  OSDMapRef osdmap;
  const pg_pool_t pool;
  PGLog log;
  DoutPrefixProvider *dpp;
  pg_shard_t pg_whoami;
  std::set<pg_shard_t> shardset;
  std::map<pg_shard_t, pg_info_t> shard_info;
  std::map<pg_shard_t, pg_missing_t> shard_missing;
  std::map<hobject_t, std::set<pg_shard_t>> missing_loc_shards;
  pg_missing_tracker_t local_missing;

  MockPGBackendListener(OSDMapRef osdmap, const pg_pool_t pi, DoutPrefixProvider *dpp, pg_shard_t pg_whoami) :
    osdmap(osdmap), pool(pi), log(g_ceph_context), dpp(dpp), pg_whoami(pg_whoami) {}

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

  // Messaging
  void send_message(int to_osd, Message *m) override {
  }

  void queue_transaction(
    ObjectStore::Transaction&& t,
    OpRequestRef op = OpRequestRef()) override {
  }

  void queue_transactions(
    std::vector<ObjectStore::Transaction>& tls,
    OpRequestRef op = OpRequestRef()) override {
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
    return true;
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
    return pool;
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
    return pg_shard_t();
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
  }

  void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) override {
  }

  void send_message_osd_cluster(MessageRef, Connection *con) override {
  }

  void send_message_osd_cluster(Message *m, const ConnectionRef& con) override {
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
    return nullptr;
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

  struct ECListener *get_eclistener() override {
    return nullptr;
  }
};

