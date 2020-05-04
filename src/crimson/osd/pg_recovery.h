// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg_recovery_listener.h"
#include "crimson/osd/scheduler/scheduler.h"
#include "crimson/osd/shard_services.h"

#include "osd/object_state.h"

class PGBackend;

class PGRecovery {
public:
  PGRecovery(PGRecoveryListener* pg) : pg(pg) {}
  virtual ~PGRecovery() {}
  void start_pglogbased_recovery();

  crimson::osd::blocking_future<bool> start_recovery_ops(size_t max_to_start);
  seastar::future<> stop() { return seastar::now(); }
private:
  PGRecoveryListener* pg;
  size_t start_primary_recovery_ops(
    size_t max_to_start,
    std::vector<crimson::osd::blocking_future<>> *out);
  size_t start_replica_recovery_ops(
    size_t max_to_start,
    std::vector<crimson::osd::blocking_future<>> *out);

  std::vector<pg_shard_t> get_replica_recovery_order() const {
    return pg->get_replica_recovery_order();
  }
  std::optional<crimson::osd::blocking_future<>> recover_missing(
    const hobject_t &soid, eversion_t need);
  size_t prep_object_replica_deletes(
    const hobject_t& soid,
    eversion_t need,
    std::vector<crimson::osd::blocking_future<>> *in_progress);
  size_t prep_object_replica_pushes(
    const hobject_t& soid,
    eversion_t need,
    std::vector<crimson::osd::blocking_future<>> *in_progress);

  void on_local_recover(
    const hobject_t& soid,
    const ObjectRecoveryInfo& recovery_info,
    bool is_delete,
    ceph::os::Transaction& t);
  void on_global_recover (
    const hobject_t& soid,
    const object_stat_sum_t& stat_diff,
    bool is_delete);
  void on_failed_recover(
    const set<pg_shard_t>& from,
    const hobject_t& soid,
    const eversion_t& v);
  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info);
  void _committed_pushed_object(epoch_t epoch,
				eversion_t last_complete);
  friend class ReplicatedRecoveryBackend;
  friend class crimson::osd::UrgentRecovery;
  seastar::future<> handle_pull(Ref<MOSDPGPull> m);
  seastar::future<> handle_push(Ref<MOSDPGPush> m);
  seastar::future<> handle_push_reply(Ref<MOSDPGPushReply> m);
  seastar::future<> handle_recovery_delete(Ref<MOSDPGRecoveryDelete> m);
  seastar::future<> handle_recovery_delete_reply(
      Ref<MOSDPGRecoveryDeleteReply> m);
  seastar::future<> handle_pull_response(Ref<MOSDPGPush> m);
  seastar::future<> handle_scan(MOSDPGScan& m);
};
