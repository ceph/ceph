// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>

#include "include/common_fwd.h"
#include "osd_operation.h"
#include "msg/MessageRef.h"
#include "crimson/common/exception.h"
#include "crimson/os/futurized_collection.h"
#include "osd/PeeringState.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/object_context.h"
#include "common/AsyncReserver.h"

namespace crimson::net {
  class Messenger;
}

namespace crimson::mgr {
  class Client;
}

namespace crimson::mon {
  class Client;
}

namespace crimson::os {
  class FuturizedStore;
}

class OSDMap;
class PeeringCtx;
class BufferedRecoveryMessages;

namespace crimson::osd {

/**
 * Represents services available to each PG
 */
class ShardServices : public md_config_obs_t {
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;
  OSDMapService &osdmap_service;
  crimson::net::Messenger &cluster_msgr;
  crimson::net::Messenger &public_msgr;
  crimson::mon::Client &monc;
  crimson::mgr::Client &mgrc;
  crimson::os::FuturizedStore &store;

  crimson::common::CephContext cct;

  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
public:
  ShardServices(
    OSDMapService &osdmap_service,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);

  seastar::future<> send_to_osd(
    int peer,
    MessageRef m,
    epoch_t from_epoch);

  crimson::os::FuturizedStore &get_store() {
    return store;
  }

  crimson::common::CephContext *get_cct() {
    return &cct;
  }

  // OSDMapService
  const OSDMapService &get_osdmap_service() const {
    return osdmap_service;
  }

  // Op Management
  OperationRegistry registry;
  OperationThrottler throttler;

  template <typename T, typename... Args>
  auto start_operation(Args&&... args) {
    if (__builtin_expect(stopping, false)) {
      throw crimson::common::system_shutdown_exception();
    }
    auto op = registry.create_operation<T>(std::forward<Args>(args)...);
    return std::make_pair(op, op->start());
  }

  seastar::future<> stop() {
    stopping = true;
    return registry.stop();
  }

  // Loggers
  PerfCounters &get_recoverystate_perf_logger() {
    return *recoverystate_perf;
  }
  PerfCounters &get_perf_logger() {
    return *perf;
  }

  /// Dispatch and reset ctx transaction
  seastar::future<> dispatch_context_transaction(
    crimson::os::CollectionRef col, PeeringCtx &ctx);

  /// Dispatch and reset ctx messages
  seastar::future<> dispatch_context_messages(
    BufferedRecoveryMessages &&ctx);

  /// Dispatch ctx and dispose of context
  seastar::future<> dispatch_context(
    crimson::os::CollectionRef col,
    PeeringCtx &&ctx);

  /// Dispatch ctx and dispose of ctx, transaction must be empty
  seastar::future<> dispatch_context(
    PeeringCtx &&ctx) {
    return dispatch_context({}, std::move(ctx));
  }

  // PG Temp State
private:
  // TODO: hook into map processing and some kind of heartbeat/peering
  // message processing
  struct pg_temp_t {
    std::vector<int> acting;
    bool forced = false;
  };
  map<pg_t, pg_temp_t> pg_temp_wanted;
  map<pg_t, pg_temp_t> pg_temp_pending;
  void _sent_pg_temp();
  friend std::ostream& operator<<(std::ostream&, const pg_temp_t&);
public:
  void queue_want_pg_temp(pg_t pgid, const vector<int>& want,
			  bool forced = false);
  void remove_want_pg_temp(pg_t pgid);
  void requeue_pg_temp();
  seastar::future<> send_pg_temp();

  // Shard-local OSDMap
private:
  cached_map_t osdmap;
public:
  void update_map(cached_map_t new_osdmap);
  cached_map_t &get_osdmap();

  // PG Created State
private:
  set<pg_t> pg_created;
public:
  seastar::future<> send_pg_created(pg_t pgid);
  seastar::future<> send_pg_created();
  void prune_pg_created();

  unsigned get_pg_num() const {
    return num_pgs;
  }
  void inc_pg_num() {
    ++num_pgs;
  }
  void dec_pg_num() {
    --num_pgs;
  }

  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  // Time state
  ceph::mono_time startup_time = ceph::mono_clock::now();
  ceph::signedspan get_mnow() const {
    return ceph::mono_clock::now() - startup_time;
  }
  HeartbeatStampsRef get_hb_stamps(int peer);
  std::map<int, HeartbeatStampsRef> heartbeat_stamps;

  crimson::osd::ObjectContextRegistry obc_registry;

  // Async Reservers
private:
  unsigned num_pgs = 0;

  struct DirectFinisher {
    void queue(Context *c) {
      c->complete(0);
    }
  } finisher;
  // prevent creating new osd operations when system is shutting down,
  // this is necessary because there are chances that a new operation
  // is created, after the interruption of all ongoing operations, and
  // creats and waits on a new and may-never-resolve future, in which
  // case the shutdown may never succeed.
  bool stopping = false;
public:
  AsyncReserver<spg_t, DirectFinisher> local_reserver;
  AsyncReserver<spg_t, DirectFinisher> remote_reserver;
};

}
