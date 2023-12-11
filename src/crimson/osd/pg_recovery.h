// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/osd/backfill_state.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg_recovery_listener.h"
#include "crimson/osd/scheduler/scheduler.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/recovery_backend.h"

#include "osd/object_state.h"

namespace crimson::osd {
class UrgentRecovery;
}

class MOSDPGBackfillRemove;
class PGBackend;

class PGRecovery : public crimson::osd::BackfillState::BackfillListener {
public:
  template <typename T = void>
  using interruptible_future = RecoveryBackend::interruptible_future<T>;
  PGRecovery(PGRecoveryListener* pg) : pg(pg) {}
  virtual ~PGRecovery() {}
  void start_pglogbased_recovery();

  interruptible_future<bool> start_recovery_ops(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI&,
    size_t max_to_start);
  void on_backfill_reserved();
  void dispatch_backfill_event(
    boost::intrusive_ptr<const boost::statechart::event_base> evt);

  seastar::future<> stop() { return seastar::now(); }
private:
  PGRecoveryListener* pg;
  size_t start_primary_recovery_ops(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI&,
    size_t max_to_start,
    std::vector<interruptible_future<>> *out);
  size_t start_replica_recovery_ops(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI&,
    size_t max_to_start,
    std::vector<interruptible_future<>> *out);

  std::vector<pg_shard_t> get_replica_recovery_order() const {
    return pg->get_replica_recovery_order();
  }
  RecoveryBackend::interruptible_future<> recover_missing(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI&,
    const hobject_t &soid, eversion_t need);
  RecoveryBackend::interruptible_future<> prep_object_replica_deletes(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
    const hobject_t& soid,
    eversion_t need);
  RecoveryBackend::interruptible_future<> prep_object_replica_pushes(
    RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
    const hobject_t& soid,
    eversion_t need);

  RecoveryBackend::interruptible_future<> on_local_recover(
    const hobject_t& soid,
    const ObjectRecoveryInfo& recovery_info,
    bool is_delete,
    ceph::os::Transaction& t);
  void on_global_recover (
    const hobject_t& soid,
    const object_stat_sum_t& stat_diff,
    bool is_delete);
  void on_failed_recover(
    const std::set<pg_shard_t>& from,
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

  // backfill begin
  std::unique_ptr<crimson::osd::BackfillState> backfill_state;
  std::map<pg_shard_t,
           MURef<MOSDPGBackfillRemove>> backfill_drop_requests;

  template <class EventT>
  void start_backfill_recovery(
    const EventT& evt);
  void request_replica_scan(
    const pg_shard_t& target,
    const hobject_t& begin,
    const hobject_t& end) final;
  void request_primary_scan(
    const hobject_t& begin) final;
  void enqueue_push(
    const hobject_t& obj,
    const eversion_t& v) final;
  void enqueue_drop(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) final;
  void maybe_flush() final;
  void update_peers_last_backfill(
    const hobject_t& new_last_backfill) final;
  bool budget_available() const final;
  void backfilled() final;
  friend crimson::osd::BackfillState::PGFacade;
  friend crimson::osd::PG;
  // backfill end
};
