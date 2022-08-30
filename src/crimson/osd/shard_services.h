// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>

#include "include/common_fwd.h"
#include "osd_operation.h"
#include "msg/MessageRef.h"
#include "crimson/common/exception.h"
#include "crimson/common/shared_lru.h"
#include "crimson/os/futurized_collection.h"
#include "osd/PeeringState.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_meta.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/pg_map.h"
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

class PGShardManager;

/**
 * PerShardState
 *
 * Per-shard state holding instances local to each shard.
 */
class PerShardState {
  friend class ShardServices;
  friend class PGShardManager;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  const int whoami;
  crimson::os::FuturizedStore &store;
  crimson::common::CephContext cct;

  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

  // Op Management
  OSDOperationRegistry registry;
  OperationThrottler throttler;

  epoch_t up_epoch = 0;
  OSDMapService::cached_map_t osdmap;
  const auto &get_osdmap() const { return osdmap; }
  void update_map(OSDMapService::cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }
  void set_up_epoch(epoch_t epoch) { up_epoch = epoch; }

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

  // PGMap state
  PGMap pg_map;

  seastar::future<> stop_pgs();
  std::map<pg_t, pg_stat_t> get_pg_stats() const;
  seastar::future<> broadcast_map_to_pgs(
    ShardServices &shard_services,
    epoch_t epoch);

  Ref<PG> get_pg(spg_t pgid);
  template <typename F>
  void for_each_pg(F &&f) const {
    for (auto &pg : pg_map.get_pgs()) {
      std::invoke(f, pg.first, pg.second);
    }
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

  // tids for ops i issue, prefixed with core id to ensure uniqueness
  ceph_tid_t next_tid;
  ceph_tid_t get_tid() {
    return next_tid++;
  }

  HeartbeatStampsRef get_hb_stamps(int peer);
  std::map<int, HeartbeatStampsRef> heartbeat_stamps;

public:
  PerShardState(
    int whoami,
    crimson::os::FuturizedStore &store);
};

/**
 * OSDSingletonState
 *
 * OSD-wide singleton holding instances that need to be accessible
 * from all PGs.
 */
class OSDSingletonState : public md_config_obs_t {
  friend class ShardServices;
  friend class PGShardManager;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

public:
  OSDSingletonState(
    int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc);

  const int whoami;

  crimson::common::CephContext cct;

  OSDState osd_state;

  SharedLRU<epoch_t, OSDMap> osdmaps;
  SimpleLRU<epoch_t, bufferlist, false> map_bl_cache;

  cached_map_t osdmap;
  cached_map_t &get_osdmap() { return osdmap; }
  void update_map(cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }
  OSD_OSDMapGate osdmap_gate;

  crimson::net::Messenger &cluster_msgr;
  crimson::net::Messenger &public_msgr;

  seastar::future<> send_to_osd(int peer, MessageURef m, epoch_t from_epoch);

  crimson::mon::Client &monc;
  seastar::future<> osdmap_subscribe(version_t epoch, bool force_request);

  crimson::mgr::Client &mgrc;

  std::unique_ptr<OSDMeta> meta_coll;
  template <typename... Args>
  void init_meta_coll(Args&&... args) {
    meta_coll = std::make_unique<OSDMeta>(std::forward<Args>(args)...);
  }
  OSDMeta &get_meta_coll() {
    assert(meta_coll);
    return *meta_coll;
  }

  auto get_pool_info(int64_t poolid) {
    return get_meta_coll().load_final_pool_info(poolid);
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

  // TODO: add config to control mapping
  PGShardMapping pg_to_shard_mapping{0, 1};

  std::set<pg_t> pg_created;
  seastar::future<> send_pg_created(pg_t pgid);
  seastar::future<> send_pg_created();
  void prune_pg_created();

  // Time state
  ceph::mono_time startup_time = ceph::mono_clock::now();
  ceph::signedspan get_mnow() const {
    return ceph::mono_clock::now() - startup_time;
  }

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

  seastar::future<local_cached_map_t> get_local_map(epoch_t e);
  seastar::future<std::unique_ptr<OSDMap>> load_map(epoch_t e);
  seastar::future<bufferlist> load_map_bl(epoch_t e);
  seastar::future<std::map<epoch_t, bufferlist>>
  load_map_bls(epoch_t first, epoch_t last);
  void store_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  seastar::future<> store_maps(ceph::os::Transaction& t,
                               epoch_t start, Ref<MOSDMap> m);
};

/**
 * Represents services available to each PG
 */
class ShardServices : public OSDMapService {
  friend class PGShardManager;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  PerShardState local_state;
  OSDSingletonState &osd_singleton_state;

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
#define FORWARD_TO_OSD_SINGLETON(METHOD) \
  FORWARD(METHOD, METHOD, osd_singleton_state)

  template <typename F, typename... Args>
  auto with_singleton(F &&f, Args&&... args) {
    return std::invoke(f, osd_singleton_state, std::forward<Args>(args)...);
  }
public:
  template <typename... PSSArgs>
  ShardServices(
    OSDSingletonState &osd_singleton_state,
    PSSArgs&&... args)
    : local_state(std::forward<PSSArgs>(args)...),
      osd_singleton_state(osd_singleton_state) {}

  FORWARD_TO_OSD_SINGLETON(send_to_osd)

  crimson::os::FuturizedStore &get_store() {
    return local_state.store;
  }

  crimson::common::CephContext *get_cct() {
    return &(local_state.cct);
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

  // Local PG Management
  seastar::future<Ref<PG>> make_pg(
    cached_map_t create_map,
    spg_t pgid,
    bool do_create);
  seastar::future<Ref<PG>> handle_pg_create_info(
    std::unique_ptr<PGCreateInfo> info);
  seastar::future<Ref<PG>> get_or_create_pg(
    PGMap::PGCreationBlockingEvent::TriggerI&&,
    spg_t pgid,
    epoch_t epoch,
    std::unique_ptr<PGCreateInfo> info);
  seastar::future<Ref<PG>> wait_for_pg(
    PGMap::PGCreationBlockingEvent::TriggerI&&, spg_t pgid);
  seastar::future<Ref<PG>> load_pg(spg_t pgid);

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

  /// Return per-core tid
  ceph_tid_t get_tid() { return local_state.get_tid(); }

  /// Return core-local pg count * number of cores
  unsigned get_num_local_pgs() const {
    return local_state.pg_map.get_pg_count();
  }

  // OSDMapService
  cached_map_t get_map() const final { return local_state.get_osdmap(); }
  epoch_t get_up_epoch() const final { return local_state.up_epoch; }
  seastar::future<cached_map_t> get_map(epoch_t e) final {
    return with_singleton(
      [](auto &sstate, epoch_t e) {
	return sstate.get_local_map(
	  e
	).then([](auto lmap) {
	  return seastar::foreign_ptr<local_cached_map_t>(lmap);
	});
      }, e).then([](auto fmap) {
	return make_local_shared_foreign(std::move(fmap));
      });
  }

  FORWARD_TO_OSD_SINGLETON(get_pool_info)
  FORWARD(with_throttle_while, with_throttle_while, local_state.throttler)

  FORWARD_TO_OSD_SINGLETON(osdmap_subscribe)
  FORWARD_TO_OSD_SINGLETON(queue_want_pg_temp)
  FORWARD_TO_OSD_SINGLETON(remove_want_pg_temp)
  FORWARD_TO_OSD_SINGLETON(requeue_pg_temp)
  FORWARD_TO_OSD_SINGLETON(send_pg_created)
  FORWARD_TO_OSD_SINGLETON(send_alive)
  FORWARD_TO_OSD_SINGLETON(send_pg_temp)
  FORWARD_CONST(get_mnow, get_mnow, osd_singleton_state)
  FORWARD_TO_LOCAL(get_hb_stamps)

  FORWARD(pg_created, pg_created, local_state.pg_map)

  FORWARD(
    maybe_get_cached_obc, maybe_get_cached_obc, local_state.obc_registry)
  FORWARD(
    get_cached_obc, get_cached_obc, local_state.obc_registry)

  FORWARD(
    local_request_reservation, request_reservation,
    osd_singleton_state.local_reserver)
  FORWARD(
    local_update_priority, update_priority,
    osd_singleton_state.local_reserver)
  FORWARD(
    local_cancel_reservation, cancel_reservation,
    osd_singleton_state.local_reserver)
  FORWARD(
    remote_request_reservation, request_reservation,
    osd_singleton_state.remote_reserver)
  FORWARD(
    remote_cancel_reservation, cancel_reservation,
    osd_singleton_state.remote_reserver)

#undef FORWARD_CONST
#undef FORWARD
#undef FORWARD_TO_OSD_SINGLETON
#undef FORWARD_TO_LOCAL
};

}
