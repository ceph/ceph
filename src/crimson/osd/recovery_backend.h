// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/shard_services.h"

#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGScan.h"
#include "osd/recovery_types.h"
#include "osd/osd_types.h"

namespace crimson::osd{
  class PG;
}

class PGBackend;

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
    auto [it, added] = recovering.emplace(soid, new WaitForObjectRecovery{});
    assert(it->second);
    return {*(it->second), added};
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
    crimson::net::ConnectionXcoreRef conn);

  virtual interruptible_future<> recover_object(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual interruptible_future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual interruptible_future<> push_delete(
    const hobject_t& soid,
    eversion_t need) = 0;

  interruptible_future<BackfillInterval> scan_for_backfill(
    const hobject_t& from,
    std::int64_t min,
    std::int64_t max);

  void on_peering_interval_change(ceph::os::Transaction& t) {
    clean_up(t, "new peering interval");
  }

  seastar::future<> stop() {
    for (auto& [soid, recovery_waiter] : recovering) {
      recovery_waiter->stop();
    }
    return on_stop();
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
    seastar::shared_promise<> readable, recovered, pulled;
    std::map<pg_shard_t, seastar::shared_promise<>> pushes;
  public:
    static constexpr const char* type_name = "WaitForObjectRecovery";

    crimson::osd::ObjectContextRef obc;
    std::optional<pull_info_t> pull_info;
    std::map<pg_shard_t, push_info_t> pushing;

    seastar::future<> wait_for_readable() {
      return readable.get_shared_future();
    }
    seastar::future<> wait_for_pushes(pg_shard_t shard) {
      return pushes[shard].get_shared_future();
    }
    seastar::future<> wait_for_recovered() {
      return recovered.get_shared_future();
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
      return wait_track_blocking(trigger, recovered.get_shared_future());
    }
    seastar::future<> wait_for_pull() {
      return pulled.get_shared_future();
    }
    void set_readable() {
      readable.set_value();
    }
    void set_recovered() {
      recovered.set_value();
    }
    void set_pushed(pg_shard_t shard) {
      pushes[shard].set_value();
    }
    void set_pulled() {
      pulled.set_value();
    }
    void set_push_failed(pg_shard_t shard, std::exception_ptr e) {
      pushes.at(shard).set_exception(e);
    }
    void interrupt(std::string_view why) {
      readable.set_exception(std::system_error(
        std::make_error_code(std::errc::interrupted), why.data()));
      recovered.set_exception(std::system_error(
        std::make_error_code(std::errc::interrupted), why.data()));
      pulled.set_exception(std::system_error(
        std::make_error_code(std::errc::interrupted), why.data()));
      for (auto& [pg_shard, pr] : pushes) {
        pr.set_exception(std::system_error(
          std::make_error_code(std::errc::interrupted), why.data()));
      }
    }
    void stop();
    void dump_detail(Formatter* f) const {
    }
  };
  using RecoveryBlockingEvent =
    crimson::AggregateBlockingEvent<WaitForObjectRecovery::BlockingEvent>;
  using WaitForObjectRecoveryRef = boost::intrusive_ptr<WaitForObjectRecovery>;
protected:
  std::map<hobject_t, WaitForObjectRecoveryRef> recovering;
  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version) const;

  boost::container::flat_set<hobject_t> temp_contents;

  void add_temp_obj(const hobject_t &oid) {
    temp_contents.insert(oid);
  }
  void clear_temp_obj(const hobject_t &oid) {
    temp_contents.erase(oid);
  }
  void clean_up(ceph::os::Transaction& t, std::string_view why);
  virtual seastar::future<> on_stop() = 0;
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
