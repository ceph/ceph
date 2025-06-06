// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/shard_services.h"

#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGScan.h"
#include "osd/recovery_types.h"
#include "osd/osd_types.h"

namespace crimson::osd{
  class PG;
}

class RecoveryBackend {
public:
  class WaitForObjectRecovery;
public:
  template <typename T = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;
  RecoveryBackend(crimson::osd::PG& pg,
		  crimson::osd::ShardServices& shard_services,
		  crimson::os::CollectionRef coll,
		  PGBackend* backend)
    : pg{pg},
      shard_services{shard_services},
      store{&shard_services.get_store()},
      coll{coll},
      backend{backend} {}
  virtual ~RecoveryBackend() {}
  std::pair<WaitForObjectRecovery&, bool> add_recovering(const hobject_t& soid) {
    auto [it, added] = recovering.emplace(soid, new WaitForObjectRecovery(pg));
    assert(it->second);
    return {*(it->second), added};
  }
  seastar::future<> add_unfound(const hobject_t &soid) {
    auto [it, added] = unfound.emplace(soid, seastar::shared_promise());
    return it->second.get_shared_future();
  }
  void found_and_remove(const hobject_t &soid) {
    auto it = unfound.find(soid);
    if (it != unfound.end()) {
      auto &found_promise = it->second;
      found_promise.set_value();
      unfound.erase(it);
    }
  }
  WaitForObjectRecovery& get_recovering(const hobject_t& soid) {
    assert(is_recovering(soid));
    return *(recovering.at(soid));
  }
  void remove_recovering(const hobject_t& soid) {
    recovering.erase(soid);
  }
  bool is_recovering(const hobject_t& soid) const {
    return recovering.count(soid) != 0;
  }
  uint64_t total_recovering() const {
    return recovering.size();
  }

  virtual interruptible_future<> handle_recovery_op(
    Ref<MOSDFastDispatchOp> m,
    crimson::net::ConnectionXcoreRef conn) = 0;

  virtual interruptible_future<> recover_object(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual interruptible_future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual interruptible_future<> push_delete(
    const hobject_t& soid,
    eversion_t need) = 0;

  interruptible_future<PrimaryBackfillInterval> scan_for_backfill_primary(
    const hobject_t from,
    std::int64_t min,
    std::int64_t max,
    const std::set<pg_shard_t> &backfill_targets);

  interruptible_future<ReplicaBackfillInterval> scan_for_backfill_replica(
    const hobject_t from,
    std::int64_t min,
    std::int64_t max);

  enum interrupt_cause_t : uint8_t {
    INTERVAL_CHANGE,
    MAX
  };
  void on_peering_interval_change(ceph::os::Transaction& t) {
    clean_up(t, interrupt_cause_t::INTERVAL_CHANGE);
  }

  seastar::future<> stop() {
    for (auto& [soid, recovery_waiter] : recovering) {
      recovery_waiter->stop();
    }
    for (auto& [soid, promise] : unfound) {
      promise.set_exception(
	crimson::common::system_shutdown_exception());
    }
    return on_stop();
  }

  template <typename Func>
  void for_each_recovery_waiter(Func &&f) {
    for (auto &[soid, recovery_waiter] : recovering) {
      std::forward<Func>(f)(soid, recovery_waiter);
    }
  }
protected:
  crimson::osd::PG& pg;
  crimson::osd::ShardServices& shard_services;
  crimson::os::FuturizedStore::Shard* store;
  crimson::os::CollectionRef coll;
  PGBackend* backend;

  struct pull_info_t {
    pg_shard_t from;
    hobject_t soid;
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    crimson::osd::ObjectContextRef head_ctx;
    crimson::osd::ObjectContextRef obc;
    object_stat_sum_t stat;
    bool is_complete() const {
      return recovery_progress.is_complete(recovery_info);
    }
  };

  struct push_info_t {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    crimson::osd::ObjectContextRef obc;
    object_stat_sum_t stat;
  };

public:
  class WaitForObjectRecovery :
    public boost::intrusive_ref_counter<
      WaitForObjectRecovery, boost::thread_unsafe_counter>,
    public crimson::BlockerT<WaitForObjectRecovery> {
      crimson::osd::PG &pg;
    std::optional<seastar::shared_promise<>> readable, recovered, pulled;
    std::map<pg_shard_t, seastar::shared_promise<>> pushes;
  public:
    static constexpr const char* type_name = "WaitForObjectRecovery";

    WaitForObjectRecovery(crimson::osd::PG &pg) : pg(pg) {}

    crimson::osd::ObjectContextRef obc;
    std::optional<pull_info_t> pull_info;
    std::map<pg_shard_t, push_info_t> pushing;

    seastar::future<> wait_for_readable() {
      if (!readable) {
	readable = seastar::shared_promise<>();
      }
      return readable->get_shared_future();
    }
    seastar::future<> wait_for_pushes(pg_shard_t shard) {
      return pushes[shard].get_shared_future();
    }
    seastar::future<> wait_for_recovered() {
      if (!recovered) {
	recovered = seastar::shared_promise<>();
      }
      return recovered->get_shared_future();
    }
    template <typename T, typename F>
    auto wait_track_blocking(T &trigger, F &&fut) {
      WaitForObjectRecoveryRef ref = this;
      return track_blocking(
	trigger,
	std::forward<F>(fut)
      ).finally([ref] {});
    }
    template <typename T>
    seastar::future<> wait_for_recovered(T &trigger) {
      WaitForObjectRecoveryRef ref = this;
      if (!recovered) {
	recovered = seastar::shared_promise<>();
      }
      return wait_track_blocking(trigger, recovered->get_shared_future());
    }
    seastar::future<> wait_for_pull() {
      if (!pulled) {
	pulled = seastar::shared_promise<>();
      }
      return pulled->get_shared_future();
    }
    void set_readable() {
      if (readable) {
	readable->set_value();
	readable.reset();
      }
    }
    void set_recovered() {
      if (recovered) {
	recovered->set_value();
	recovered.reset();
      }
    }
    void set_pushed(pg_shard_t shard) {
      auto it = pushes.find(shard);
      ceph_assert(it != pushes.end());
      it->second.set_value();
      pushes.erase(it);
    }
    void set_pulled() {
      if (pulled) {
	pulled->set_value();
	pulled.reset();
      }
    }
    void repeat_pull() {
      ceph_assert(pulled);
      pulled->set_exception(crimson::ct_error::eagain::exception_ptr());
    }
    bool is_pulling() const {
      return (bool)pulled;
    }
    void set_push_failed(pg_shard_t shard, std::exception_ptr e) {
      auto it = pushes.find(shard);
      if (it != pushes.end()) {
	auto &push_promise = it->second;
	push_promise.set_exception(e);
	pushes.erase(it);
      }
    }
    void interrupt(interrupt_cause_t why);
    void stop();
    void dump_detail(Formatter* f) const {
    }
  };
  using RecoveryBlockingEvent =
    crimson::AggregateBlockingEvent<WaitForObjectRecovery::BlockingEvent>;
  using WaitForObjectRecoveryRef = boost::intrusive_ptr<WaitForObjectRecovery>;
protected:
  std::map<hobject_t, WaitForObjectRecoveryRef> recovering;
  std::map<hobject_t, seastar::shared_promise<>> unfound;
  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version) const;

  void add_temp_obj(const hobject_t &oid);
  void clear_temp_obj(const hobject_t &oid);
  template <typename Func>
  void for_each_temp_obj(Func &&f) {
    backend->for_each_temp_obj(std::forward<Func>(f));
  }
  void clear_temp_objs() {
    backend->clear_temp_objs();
  }

  void clean_up(ceph::os::Transaction& t, interrupt_cause_t why);
  virtual seastar::future<> on_stop() = 0;

  virtual interruptible_future<> handle_backfill_op(
    Ref<MOSDFastDispatchOp> m,
    crimson::net::ConnectionXcoreRef conn);

  /**
   * replica_push_targets
   *
   * Holds obc on replica for in-progress pushes, see
   * ReplicatedRecoveryBackend::handle_push
   */
  std::map<hobject_t, crimson::osd::ObjectContextRef> replica_push_targets;
private:
  void handle_backfill_finish(
    MOSDPGBackfill& m,
    crimson::net::ConnectionXcoreRef conn);
  interruptible_future<> handle_backfill_progress(
    MOSDPGBackfill& m);
  interruptible_future<> handle_backfill_finish_ack(
    MOSDPGBackfill& m);
  interruptible_future<> handle_backfill(
    MOSDPGBackfill& m,
    crimson::net::ConnectionXcoreRef conn);

  interruptible_future<> handle_scan_get_digest(
    MOSDPGScan& m,
    crimson::net::ConnectionXcoreRef conn);
  interruptible_future<> handle_scan_digest(
    MOSDPGScan& m);
  interruptible_future<> handle_scan(
    MOSDPGScan& m,
    crimson::net::ConnectionXcoreRef conn);
  interruptible_future<> handle_backfill_remove(MOSDPGBackfillRemove& m);
};
