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

#include "osd/PGBackend.h"
#include "os/ObjectStore.h"

// MockPGBackend - simple stub for PGBackend
class MockPGBackend : public PGBackend {
public:
  MockPGBackend(CephContext* cct, Listener *l, ObjectStore *store,
                const coll_t &coll, ObjectStore::CollectionHandle &ch)
    : PGBackend(cct, l, store, coll, ch) {}

  // Recovery operations
  RecoveryHandle *open_recovery_op() override {
    return nullptr;
  }

  void run_recovery_op(RecoveryHandle *h, int priority) override {
  }

  int recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h) override {
    return 0;
  }

  // Message handling
  bool can_handle_while_inactive(OpRequestRef op) override {
    return false;
  }

  bool _handle_message(OpRequestRef op) override {
    return false;
  }

  void check_recovery_sources(const OSDMapRef& osdmap) override {
  }

  // State management
  void on_change() override {
  }

  void clear_recovery_state() override {
  }

  // Predicates
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const override {
    return nullptr;
  }

  IsPGReadablePredicate *get_is_readable_predicate() const override {
    return nullptr;
  }

  bool get_ec_supports_crc_encode_decode() const override {
    return false;
  }

  void dump_recovery_info(ceph::Formatter *f) const override {
  }

  bool ec_can_decode(const shard_id_set &available_shards) const override {
    return false;
  }

  shard_id_map<bufferlist> ec_encode_acting_set(
    const bufferlist &in_bl) const override {
    return {0};
  }

  shard_id_map<bufferlist> ec_decode_acting_set(
    const shard_id_map<bufferlist> &shard_map, int chunk_size) const override {
    return {0};
  }

  ECUtil::stripe_info_t ec_get_sinfo() const override {
    return {0, 0, 0};
  }

  // Transaction submission
  void submit_transaction(
    const hobject_t &hoid,
    const object_stat_sum_t &delta_stats,
    const eversion_t &at_version,
    PGTransactionUPtr &&t,
    const eversion_t &trim_to,
    const eversion_t &pg_committed_to,
    std::vector<pg_log_entry_t>&& log_entries,
    std::optional<pg_hit_set_history_t> &hset_history,
    Context *on_all_commit,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op) override {
  }

  void call_write_ordered(std::function<void(void)> &&cb) override {
    cb();
  }

  // Object operations
  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl) override {
    return 0;
  }

  int objects_read_local(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl) override {
    return 0;
  }

  void objects_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const std::list<std::pair<ec_align_t,
      std::pair<ceph::buffer::list*, Context*>>> &to_read,
    Context *on_complete, bool fast_read = false) override {
  }

  bool auto_repair_supported() const override {
    return false;
  }

  uint64_t be_get_ondisk_size(uint64_t logical_size,
                              shard_id_t shard_id,
                              bool object_is_legacy_ec) const override {
    return logical_size;
  }

  int be_deep_scrub(
    const Scrub::ScrubCounterSet& io_counters,
    const hobject_t &oid,
    ScrubMap &map,
    ScrubMapBuilder &pos,
    ScrubMap::object &o) override {
    return 0;
  }
};

