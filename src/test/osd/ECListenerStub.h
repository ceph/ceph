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

#include "osd/ECCommon.h"
#include "osd/ECBackend.h"

class ECListenerStub : public ECListener {

private:
  OSDMapRef osd_map_ref;
  pg_info_t pg_info;
  std::set<pg_shard_t> backfill_shards;
  shard_id_set backfill_shard_id_set;
  std::map<hobject_t, std::set<pg_shard_t>> missing_loc_shards;
  std::map<pg_shard_t, pg_missing_t> shard_missing;
  pg_missing_set<false> shard_not_missing_const;
  pg_pool_t pg_pool;
  std::set<pg_shard_t> acting_recovery_backfill_shards;
  shard_id_set acting_recovery_backfill_shard_id_set;
  std::map<pg_shard_t, pg_info_t> shard_info;
  PGLog pg_log;
  pg_info_t shard_pg_info;
  std::string dbg_prefix = "stub";

public:
  std::set<pg_shard_t> acting_shards;

  ECListenerStub()
    : pg_log(NULL) {}

  const OSDMapRef &pgb_get_osdmap() const override { return osd_map_ref; }
  epoch_t pgb_get_osdmap_epoch() const override { return 0; }
  const pg_info_t &get_info() const override { return pg_info; }
  void cancel_pull(const hobject_t &soid) override {}
  pg_shard_t primary_shard() const override { return pg_shard_t(); }
  bool pgb_is_primary() const override { return false; }
  void on_failed_pull(const std::set<pg_shard_t> &from, const hobject_t &soid, const eversion_t &v) override {}
  void on_local_recover(const hobject_t &oid, const ObjectRecoveryInfo &recovery_info,
                        ObjectContextRef obc, bool is_delete,
                        ceph::os::Transaction *t) override {}
  void on_global_recover(const hobject_t &oid, const object_stat_sum_t &stat_diff, bool is_delete) override {}
  void on_peer_recover(pg_shard_t peer, const hobject_t &oid, const ObjectRecoveryInfo &recovery_info) override {}
  void begin_peer_recover(pg_shard_t peer, const hobject_t oid) override {}
  bool pg_is_repair() const override { return false; }
  ObjectContextRef get_obc(const hobject_t &hoid,
                           const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return ObjectContextRef();
  }
  bool check_failsafe_full() override { return false; }
  hobject_t get_temp_recovery_object(const hobject_t &target, eversion_t version) override { return hobject_t(); }
  bool pg_is_remote_backfilling() override { return false; }
  void pg_add_local_num_bytes(int64_t num_bytes) override {}
  void pg_add_num_bytes(int64_t num_bytes) override {}
  void inc_osd_stat_repaired() override {}
  void add_temp_obj(const hobject_t &oid) override {}
  void clear_temp_obj(const hobject_t &oid) override {}
  epoch_t get_last_peering_reset_epoch() const override { return 0; }
  GenContext<ThreadPool::TPHandle &> *bless_unlocked_gencontext(
      GenContext<ThreadPool::TPHandle &> *c) override { return nullptr; }
  void schedule_recovery_work(GenContext<ThreadPool::TPHandle &> *c, uint64_t cost) override {}
  epoch_t get_interval_start_epoch() const override { return 0; }
  const std::set<pg_shard_t> &get_acting_shards() const override { return acting_shards; }
  const std::set<pg_shard_t> &get_backfill_shards() const override { return backfill_shards; }
  const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    return missing_loc_shards;
  }
  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override { return shard_missing; }
  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    return shard_not_missing_const;
  }
  const pg_missing_const_i *maybe_get_shard_missing(pg_shard_t peer) const override { return nullptr; }
  const pg_info_t &get_shard_info(pg_shard_t peer) const override { return shard_pg_info; }
  ceph_tid_t get_tid() override { return 0; }
  pg_shard_t whoami_shard() const override { return pg_shard_t(); }
  void send_message_osd_cluster(std::vector<std::pair<int, Message *>> &messages, epoch_t from_epoch) override {}
  void send_message_osd_cluster(int osd, MOSDPGPush* msg, epoch_t from_epoch) override {}
  ostream &gen_dbg_prefix(ostream &out) const override { out << dbg_prefix; return out; }
  const pg_pool_t &get_pool() const override { return pg_pool; }
  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return acting_recovery_backfill_shards;
  }
  const shard_id_set &get_acting_recovery_backfill_shard_id_set() const override {
    return acting_recovery_backfill_shard_id_set;
  }
  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override { return false; }
  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override { return shard_info; }
  spg_t primary_spg_t() const override { return spg_t(); }
  const PGLog &get_log() const override { return pg_log; }
  DoutPrefixProvider *get_dpp() override { return nullptr; }
  void apply_stats(const hobject_t &soid, const object_stat_sum_t &delta_stats) override {}
  bool is_missing_object(const hobject_t &oid) const override { return false; }
  void add_local_next_event(const pg_log_entry_t &e) override {}
  void log_operation(std::vector<pg_log_entry_t> &&logv,
                     const std::optional<pg_hit_set_history_t> &hset_history,
                     const eversion_t &trim_to, const eversion_t &roll_forward_to,
                     const eversion_t &min_last_complete_ondisk, bool transaction_applied,
                     os::Transaction &t, bool async) override {}
  void op_applied(const eversion_t &applied_version) override {}
  uint64_t min_peer_features() const { return 0; }
};
