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
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/state.h"
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
 * PerShardState
 *
 * Per-shard state holding instances local to each shard.
 */
class PerShardState {
  friend class ShardServices;
  friend class PGShardManager;

  const int whoami;
  crimson::common::CephContext cct;

  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

  // Op Management
  OSDOperationRegistry registry;
  OperationThrottler throttler;

  OSDMapService::cached_map_t osdmap;
  OSDMapService::cached_map_t &get_osdmap() { return osdmap; }
  void update_map(OSDMapService::cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }

  crimson::osd::ObjectContextRegistry obc_registry;

  // prevent creating new osd operations when system is shutting down,
  // this is necessary because there are chances that a new operation
  // is created, after the interruption of all ongoing operations, and
  // creats and waits on a new and may-never-resolve future, in which
  // case the shutdown may never succeed.
  bool stopping = false;
  seastar::future<> stop_registry() {
    crimson::get_logger(ceph_subsys_osd).info("PerShardState::{}", __func__);
    stopping = true;
    return registry.stop();
  }

  template <typename T, typename... Args>
  auto start_operation(Args&&... args) {
    if (__builtin_expect(stopping, false)) {
      throw crimson::common::system_shutdown_exception();
    }
    auto op = registry.create_operation<T>(std::forward<Args>(args)...);
    auto fut = op->start().then([op /* by copy */] {
      // ensure the op's lifetime is appropriate. It is not enough to
      // guarantee it's alive at the scheduling stages (i.e. `then()`
      // calling) but also during the actual execution (i.e. when passed
      // lambdas are actually run).
    });
    return std::make_pair(std::move(op), std::move(fut));
  }

  PerShardState(int whoami);
};

/**
 * CoreState
 *
 * OSD-wide singleton holding instances that need to be accessible
 * from all PGs.
 */
class CoreState : public md_config_obs_t {
  friend class ShardServices;
  friend class PGShardManager;
  CoreState(
    int whoami,
    OSDMapService &osdmap_service,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);

  const int whoami;

  crimson::common::CephContext cct;

  OSDState osd_state;

  OSDMapService &osdmap_service;
  OSDMapService::cached_map_t osdmap;
  OSDMapService::cached_map_t &get_osdmap() { return osdmap; }
  void update_map(OSDMapService::cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }
  OSD_OSDMapGate osdmap_gate;

  crimson::net::Messenger &cluster_msgr;
  crimson::net::Messenger &public_msgr;

  seastar::future<> send_to_osd(int peer, MessageURef m, epoch_t from_epoch);

  crimson::mon::Client &monc;
  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  crimson::mgr::Client &mgrc;

  crimson::os::FuturizedStore &store;

  // tids for ops i issue
  unsigned int next_tid{0};
  ceph_tid_t get_tid() {
    return (ceph_tid_t)next_tid++;
  }

  // global pg temp state
  struct pg_temp_t {
    std::vector<int> acting;
    bool forced = false;
  };
  std::map<pg_t, pg_temp_t> pg_temp_wanted;
  std::map<pg_t, pg_temp_t> pg_temp_pending;
  friend std::ostream& operator<<(std::ostream&, const pg_temp_t&);

  void queue_want_pg_temp(pg_t pgid, const std::vector<int>& want,
			  bool forced = false);
  void remove_want_pg_temp(pg_t pgid);
  void requeue_pg_temp();
  seastar::future<> send_pg_temp();

  unsigned num_pgs = 0;
  unsigned get_pg_num() const {
    return num_pgs;
  }
  void inc_pg_num() {
    ++num_pgs;
  }
  void dec_pg_num() {
    --num_pgs;
  }

  std::set<pg_t> pg_created;
  seastar::future<> send_pg_created(pg_t pgid);
  seastar::future<> send_pg_created();
  void prune_pg_created();

  // Time state
  ceph::mono_time startup_time = ceph::mono_clock::now();
  ceph::signedspan get_mnow() const {
    return ceph::mono_clock::now() - startup_time;
  }

  HeartbeatStampsRef get_hb_stamps(int peer);
  std::map<int, HeartbeatStampsRef> heartbeat_stamps;

  struct DirectFinisher {
    void queue(Context *c) {
      c->complete(0);
    }
  } finisher;
  AsyncReserver<spg_t, DirectFinisher> local_reserver;
  AsyncReserver<spg_t, DirectFinisher> remote_reserver;

  epoch_t up_thru_wanted = 0;
  seastar::future<> send_alive(epoch_t want);

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(
    const ConfigProxy& conf,
    const std::set <std::string> &changed) final;
};

#define FORWARD_CONST(FROM_METHOD, TO_METHOD, TARGET)		\
  template <typename... Args>					\
  auto FROM_METHOD(Args&&... args) const {			\
    return TARGET.TO_METHOD(std::forward<Args>(args)...);	\
  }

#define FORWARD(FROM_METHOD, TO_METHOD, TARGET)		\
  template <typename... Args>					\
  auto FROM_METHOD(Args&&... args) {				\
    return TARGET.TO_METHOD(std::forward<Args>(args)...);	\
  }

#define FORWARD_TO_LOCAL(METHOD) FORWARD(METHOD, METHOD, local_state)
#define FORWARD_TO_CORE(METHOD) FORWARD(METHOD, METHOD, core_state)

/**
 * Represents services available to each PG
 */
class ShardServices {
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;

  CoreState &core_state;
  PerShardState &local_state;
public:
  ShardServices(
    CoreState &core_state,
    PerShardState &local_state)
    : core_state(core_state), local_state(local_state) {}

  FORWARD_TO_CORE(send_to_osd)

  crimson::os::FuturizedStore &get_store() {
    return core_state.store;
  }

  crimson::common::CephContext *get_cct() {
    return &(local_state.cct);
  }

  // OSDMapService
  const OSDMapService &get_osdmap_service() const {
    return core_state.osdmap_service;
  }

  template <typename T, typename... Args>
  auto start_operation(Args&&... args) {
    return local_state.start_operation<T>(std::forward<Args>(args)...);
  }

  auto &get_registry() { return local_state.registry; }

  // Loggers
  PerfCounters &get_recoverystate_perf_logger() {
    return *local_state.recoverystate_perf;
  }
  PerfCounters &get_perf_logger() {
    return *local_state.perf;
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

  FORWARD_TO_LOCAL(get_osdmap)
  FORWARD_TO_CORE(get_pg_num)
  FORWARD(with_throttle_while, with_throttle_while, local_state.throttler)

  FORWARD_TO_CORE(osdmap_subscribe)
  FORWARD_TO_CORE(get_tid)
  FORWARD_TO_CORE(queue_want_pg_temp)
  FORWARD_TO_CORE(remove_want_pg_temp)
  FORWARD_TO_CORE(requeue_pg_temp)
  FORWARD_TO_CORE(send_pg_created)
  FORWARD_TO_CORE(inc_pg_num)
  FORWARD_TO_CORE(dec_pg_num)
  FORWARD_TO_CORE(send_alive)
  FORWARD_TO_CORE(send_pg_temp)
  FORWARD_CONST(get_mnow, get_mnow, core_state)
  FORWARD_TO_CORE(get_hb_stamps)

  FORWARD(
    maybe_get_cached_obc, maybe_get_cached_obc, local_state.obc_registry)
  FORWARD(
    get_cached_obc, get_cached_obc, local_state.obc_registry)

  FORWARD(
    local_request_reservation, request_reservation, core_state.local_reserver)
  FORWARD(
    local_update_priority, update_priority, core_state.local_reserver)
  FORWARD(
    local_cancel_reservation, cancel_reservation, core_state.local_reserver)
  FORWARD(
    remote_request_reservation, request_reservation, core_state.remote_reserver)
  FORWARD(
    remote_cancel_reservation, cancel_reservation, core_state.remote_reserver)
};

}
