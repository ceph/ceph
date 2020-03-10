// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/shard_services.h"

#include "osd/osd_types.h"

namespace crimson::osd{
  class PG;
}

class PGBackend;

class RecoveryBackend {
protected:
  class WaitForObjectRecovery;
public:
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
  WaitForObjectRecovery& get_recovering(const hobject_t& soid) {
    return recovering[soid];
  }
  void remove_recovering(const hobject_t& soid) {
    recovering.erase(soid);
  }
  bool is_recovering(const hobject_t& soid) {
    return recovering.count(soid) != 0;
  }
  uint64_t total_recovering() {
    return recovering.size();
  }

  virtual seastar::future<> handle_recovery_op(
    Ref<MOSDFastDispatchOp> m);

  virtual seastar::future<> recover_object(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual seastar::future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual seastar::future<> push_delete(
    const hobject_t& soid,
    eversion_t need) = 0;

  void on_peering_interval_change(ceph::os::Transaction& t) {
    clean_up(t, "new peering interval");
  }

  seastar::future<> stop() {
    for (auto& [soid, recovery_waiter] : recovering) {
      recovery_waiter.stop();
    }
    return on_stop();
  }
protected:
  crimson::osd::PG& pg;
  crimson::osd::ShardServices& shard_services;
  crimson::os::FuturizedStore* store;
  crimson::os::CollectionRef coll;
  PGBackend* backend;

  struct PullInfo {
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

  struct PushInfo {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    crimson::osd::ObjectContextRef obc;
    object_stat_sum_t stat;
  };

  class WaitForObjectRecovery : public crimson::osd::BlockerT<WaitForObjectRecovery> {
    seastar::shared_promise<> readable, recovered, pulled;
    std::map<pg_shard_t, seastar::shared_promise<>> pushes;
  public:
    static constexpr const char* type_name = "WaitForObjectRecovery";

    crimson::osd::ObjectContextRef obc;
    PullInfo pi;
    std::map<pg_shard_t, PushInfo> pushing;

    seastar::future<> wait_for_readable() {
      return readable.get_shared_future();
    }
    seastar::future<> wait_for_pushes(pg_shard_t shard) {
      return pushes[shard].get_shared_future();
    }
    seastar::future<> wait_for_recovered() {
      return recovered.get_shared_future();
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
    void interrupt(const std::string& why) {
      readable.set_exception(std::system_error(
	    std::make_error_code(std::errc::interrupted), why));
      recovered.set_exception(std::system_error(
	    std::make_error_code(std::errc::interrupted), why));
      pulled.set_exception(std::system_error(
	    std::make_error_code(std::errc::interrupted), why));
      for (auto& [pg_shard, pr] : pushes) {
	pr.set_exception(std::system_error(
	      std::make_error_code(std::errc::interrupted), why));
      }
    }
    void stop();
    void dump_detail(Formatter* f) const {
    }
  };
  std::map<hobject_t, WaitForObjectRecovery> recovering;
  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version);

  boost::container::flat_set<hobject_t> temp_contents;

  void add_temp_obj(const hobject_t &oid) {
    temp_contents.insert(oid);
  }
  void clear_temp_obj(const hobject_t &oid) {
    temp_contents.erase(oid);
  }
  void clean_up(ceph::os::Transaction& t, const std::string& why);
  virtual seastar::future<> on_stop() = 0;
};
