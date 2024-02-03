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
#include "crimson/net/Connection.h"

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
  friend class OSD;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  const core_id_t core = seastar::this_shard_id();
#define assert_core() ceph_assert(seastar::this_shard_id() == core);

  const int whoami;
  crimson::os::FuturizedStore::Shard &store;
  crimson::common::CephContext cct;

  OSDState &osd_state;
  OSD_OSDMapGate osdmap_gate;

  PerShardPipeline client_request_pipeline;
  PerShardPipeline peering_request_pipeline;
  PerShardPipeline replicated_request_pipeline;

  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

  const epoch_t& get_osdmap_tlb() {
    return per_shard_superblock.cluster_osdmap_trim_lower_bound;
  }

  // Op Management
  OSDOperationRegistry registry;
  OperationThrottler throttler;

  seastar::future<> dump_ops_in_flight(Formatter *f) const;

  epoch_t up_epoch = 0;
  OSDMapService::cached_map_t osdmap;
  const auto &get_osdmap() const {
    assert_core();
    return osdmap;
  }
  void update_map(OSDMapService::cached_map_t new_osdmap) {
    assert_core();
    osdmap = std::move(new_osdmap);
  }
  void set_up_epoch(epoch_t epoch) {
    assert_core();
    up_epoch = epoch;
  }

  // prevent creating new osd operations when system is shutting down,
  // this is necessary because there are chances that a new operation
  // is created, after the interruption of all ongoing operations, and
  // creats and waits on a new and may-never-resolve future, in which
  // case the shutdown may never succeed.
  bool stopping = false;
  seastar::future<> stop_registry() {
    assert_core();
    crimson::get_logger(ceph_subsys_osd).info("PerShardState::{}", __func__);
    stopping = true;
    return registry.stop();
  }

  // PGMap state
  PGMap pg_map;

  seastar::future<> stop_pgs();
  std::map<pg_t, pg_stat_t> get_pg_stats();
  seastar::future<> broadcast_map_to_pgs(
    ShardServices &shard_services,
    epoch_t epoch);

  Ref<PG> get_pg(spg_t pgid);
  template <typename F>
  void for_each_pg(F &&f) const {
    assert_core();
    for (auto &pg : pg_map.get_pgs()) {
      std::invoke(f, pg.first, pg.second);
    }
  }

  template <typename T, typename... Args>
  auto start_operation(Args&&... args) {
    assert_core();
    if (__builtin_expect(stopping, false)) {
      throw crimson::common::system_shutdown_exception();
    }
    auto op = registry.create_operation<T>(std::forward<Args>(args)...);
    crimson::get_logger(ceph_subsys_osd).info(
      "PerShardState::{}, {}", __func__, *op);
    auto fut = seastar::yield().then([op] {
      return op->start().finally([op /* by copy */] {
	// ensure the op's lifetime is appropriate. It is not enough to
	// guarantee it's alive at the scheduling stages (i.e. `then()`
	// calling) but also during the actual execution (i.e. when passed
	// lambdas are actually run).
      });
    });
    return std::make_pair(std::move(op), std::move(fut));
  }

  template <typename InterruptorT, typename T, typename... Args>
  auto start_operation_may_interrupt(Args&&... args) {
    assert_core();
    if (__builtin_expect(stopping, false)) {
      throw crimson::common::system_shutdown_exception();
    }
    auto op = registry.create_operation<T>(std::forward<Args>(args)...);
    crimson::get_logger(ceph_subsys_osd).info(
      "PerShardState::{}, {}", __func__, *op);
    auto fut = InterruptorT::make_interruptible(
      seastar::yield()
    ).then_interruptible([op] {
      return op->start().finally([op /* by copy */] {
	// ensure the op's lifetime is appropriate. It is not enough to
	// guarantee it's alive at the scheduling stages (i.e. `then()`
	// calling) but also during the actual execution (i.e. when passed
	// lambdas are actually run).
      });
    });
    return std::make_pair(std::move(op), std::move(fut));
  }

  // tids for ops i issue, prefixed with core id to ensure uniqueness
  ceph_tid_t next_tid;
  ceph_tid_t get_tid() {
    assert_core();
    return next_tid++;
  }

  HeartbeatStampsRef get_hb_stamps(int peer);
  std::map<int, HeartbeatStampsRef> heartbeat_stamps;

  seastar::future<> update_shard_superblock(OSDSuperblock superblock);

  // Time state
  const ceph::mono_time startup_time;
  ceph::signedspan get_mnow() const {
    assert_core();
    return ceph::mono_clock::now() - startup_time;
  }

  OSDSuperblock per_shard_superblock;

public:
  PerShardState(
    int whoami,
    ceph::mono_time startup_time,
    PerfCounters *perf,
    PerfCounters *recoverystate_perf,
    crimson::os::FuturizedStore &store,
    OSDState& osd_state);
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
  friend class OSD;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;
  using read_errorator = crimson::os::FuturizedStore::Shard::read_errorator;

public:
  OSDSingletonState(
    int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc);

private:
  const int whoami;

  crimson::common::CephContext cct;
  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

  SharedLRU<epoch_t, OSDMap> osdmaps;
  SimpleLRU<epoch_t, bufferlist, false> map_bl_cache;
  SimpleLRU<epoch_t, bufferlist, false> inc_map_bl_cache;

  cached_map_t osdmap;
  cached_map_t &get_osdmap() { return osdmap; }
  void update_map(cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }

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

  OSDSuperblock superblock;
  void set_singleton_superblock(OSDSuperblock _superblock) {
    superblock = std::move(_superblock);
  }

  seastar::future<MURef<MOSDMap>> build_incremental_map_msg(
    epoch_t first,
    epoch_t last);

  seastar::future<> send_incremental_map(
    crimson::net::Connection &conn,
    epoch_t first);

  seastar::future<> send_incremental_map_to_osd(int osd, epoch_t first);

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

  std::set<pg_t> pg_created;
  seastar::future<> send_pg_created(pg_t pgid);
  seastar::future<> send_pg_created();
  void prune_pg_created();

  struct DirectFinisher {
    void queue(Context *c) {
      c->complete(0);
    }
  } finisher;
  AsyncReserver<spg_t, DirectFinisher> local_reserver;
  AsyncReserver<spg_t, DirectFinisher> remote_reserver;
  AsyncReserver<spg_t, DirectFinisher> snap_reserver;

  epoch_t up_thru_wanted = 0;
  seastar::future<> send_alive(epoch_t want);

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(
    const ConfigProxy& conf,
    const std::set <std::string> &changed) final;

  seastar::future<local_cached_map_t> get_local_map(epoch_t e);
  seastar::future<std::unique_ptr<OSDMap>> load_map(epoch_t e);
  seastar::future<bufferlist> load_map_bl(epoch_t e);
  read_errorator::future<ceph::bufferlist> load_inc_map_bl(epoch_t e);
  seastar::future<OSDMapService::bls_map_t>
  load_map_bls(epoch_t first, epoch_t last);
  void store_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  void store_inc_map_bl(ceph::os::Transaction& t,
                    epoch_t e, bufferlist&& bl);
  seastar::future<> store_maps(ceph::os::Transaction& t,
                               epoch_t start, Ref<MOSDMap> m);
  void trim_maps(ceph::os::Transaction& t, OSDSuperblock& superblock);
};

/**
 * Represents services available to each PG
 */
class ShardServices : public OSDMapService {
  friend class PGShardManager;
  friend class OSD;
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  PerShardState local_state;
  seastar::sharded<OSDSingletonState> &osd_singleton_state;
  PGShardMapping& pg_to_shard_mapping;

  template <typename F, typename... Args>
  auto with_singleton(F &&f, Args&&... args) {
    return osd_singleton_state.invoke_on(
      PRIMARY_CORE,
      std::forward<F>(f),
      std::forward<Args>(args)...
    );
  }

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
#define FORWARD_TO_LOCAL_CONST(METHOD) FORWARD_CONST(	\
    METHOD, METHOD, local_state)			\

#define FORWARD_TO_OSD_SINGLETON_TARGET(METHOD, TARGET)		\
  template <typename... Args>					\
  auto METHOD(Args&&... args) {				        \
    return with_singleton(                                      \
      [](auto &local_state, auto&&... args) {                   \
        return local_state.TARGET(                              \
	  std::forward<decltype(args)>(args)...);		\
      }, std::forward<Args>(args)...);				\
  }
#define FORWARD_TO_OSD_SINGLETON(METHOD) \
  FORWARD_TO_OSD_SINGLETON_TARGET(METHOD, METHOD)

public:
  template <typename... PSSArgs>
  ShardServices(
    seastar::sharded<OSDSingletonState> &osd_singleton_state,
    PGShardMapping& pg_to_shard_mapping,
    PSSArgs&&... args)
    : local_state(std::forward<PSSArgs>(args)...),
      osd_singleton_state(osd_singleton_state),
      pg_to_shard_mapping(pg_to_shard_mapping) {}

  FORWARD_TO_OSD_SINGLETON(send_to_osd)

  crimson::os::FuturizedStore::Shard &get_store() {
    return local_state.store;
  }

  auto remove_pg(spg_t pgid) {
    local_state.pg_map.remove_pg(pgid);
    return pg_to_shard_mapping.remove_pg_mapping(pgid);
  }

  crimson::common::CephContext *get_cct() {
    return &(local_state.cct);
  }

  template <typename T, typename... Args>
  auto start_operation(Args&&... args) {
    return local_state.start_operation<T>(std::forward<Args>(args)...);
  }

  template <typename InterruptorT, typename T, typename... Args>
  auto start_operation_may_interrupt(Args&&... args) {
    return local_state.start_operation_may_interrupt<
      InterruptorT, T>(std::forward<Args>(args)...);
  }

  auto &get_registry() { return local_state.registry; }

  // Loggers
  PerfCounters &get_recoverystate_perf_logger() {
    return *local_state.recoverystate_perf;
  }
  PerfCounters &get_perf_logger() {
    return *local_state.perf;
  }

  // Diagnostics
  FORWARD_TO_LOCAL_CONST(dump_ops_in_flight);

  // Local PG Management
  seastar::future<Ref<PG>> make_pg(
    cached_map_t create_map,
    spg_t pgid,
    bool do_create);
  seastar::future<Ref<PG>> handle_pg_create_info(
    std::unique_ptr<PGCreateInfo> info);

  using get_or_create_pg_ertr = PGMap::wait_for_pg_ertr;
  using get_or_create_pg_ret = get_or_create_pg_ertr::future<Ref<PG>>;
  get_or_create_pg_ret get_or_create_pg(
    PGMap::PGCreationBlockingEvent::TriggerI&&,
    spg_t pgid,
    std::unique_ptr<PGCreateInfo> info);

  using wait_for_pg_ertr = PGMap::wait_for_pg_ertr;
  using wait_for_pg_ret = wait_for_pg_ertr::future<Ref<PG>>;
  wait_for_pg_ret wait_for_pg(
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

  PerShardPipeline &get_client_request_pipeline() {
    return local_state.client_request_pipeline;
  }

  PerShardPipeline &get_peering_request_pipeline() {
    return local_state.peering_request_pipeline;
  }

  PerShardPipeline &get_replicated_request_pipeline() {
    return local_state.replicated_request_pipeline;
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

  FORWARD_TO_OSD_SINGLETON(build_incremental_map_msg)
  FORWARD_TO_OSD_SINGLETON(send_incremental_map)
  FORWARD_TO_OSD_SINGLETON(send_incremental_map_to_osd)

  FORWARD_TO_OSD_SINGLETON(osdmap_subscribe)
  FORWARD_TO_OSD_SINGLETON(queue_want_pg_temp)
  FORWARD_TO_OSD_SINGLETON(remove_want_pg_temp)
  FORWARD_TO_OSD_SINGLETON(requeue_pg_temp)
  FORWARD_TO_OSD_SINGLETON(send_pg_created)
  FORWARD_TO_OSD_SINGLETON(send_alive)
  FORWARD_TO_OSD_SINGLETON(send_pg_temp)
  FORWARD_TO_LOCAL_CONST(get_mnow)
  FORWARD_TO_LOCAL(get_hb_stamps)
  FORWARD_TO_LOCAL(update_shard_superblock)
  FORWARD_TO_LOCAL(get_osdmap_tlb)

  FORWARD(pg_created, pg_created, local_state.pg_map)

  FORWARD_TO_OSD_SINGLETON_TARGET(
    local_update_priority,
    local_reserver.update_priority)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    local_cancel_reservation,
    local_reserver.cancel_reservation)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    local_dump_reservations,
    local_reserver.dump)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    remote_cancel_reservation,
    remote_reserver.cancel_reservation)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    remote_dump_reservations,
    remote_reserver.dump)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    snap_cancel_reservation,
    snap_reserver.cancel_reservation)
  FORWARD_TO_OSD_SINGLETON_TARGET(
    snap_dump_reservations,
    snap_reserver.dump)

  Context *invoke_context_on_core(core_id_t core, Context *c) {
    if (!c) return nullptr;
    return new LambdaContext([core, c](int code) {
      std::ignore = seastar::smp::submit_to(
	core,
	[c, code] {
	  c->complete(code);
	});
    });
  }
  seastar::future<> local_request_reservation(
    spg_t item,
    Context *on_reserved,
    unsigned prio,
    Context *on_preempt) {
    return with_singleton(
      [item, prio](OSDSingletonState &singleton,
		   Context *wrapped_on_reserved, Context *wrapped_on_preempt) {
	return singleton.local_reserver.request_reservation(
	  item,
	  wrapped_on_reserved,
	  prio,
	  wrapped_on_preempt);
      },
      invoke_context_on_core(seastar::this_shard_id(), on_reserved),
      invoke_context_on_core(seastar::this_shard_id(), on_preempt));
  }
  seastar::future<> remote_request_reservation(
    spg_t item,
    Context *on_reserved,
    unsigned prio,
    Context *on_preempt) {
    return with_singleton(
      [item, prio](OSDSingletonState &singleton,
		   Context *wrapped_on_reserved, Context *wrapped_on_preempt) {
	return singleton.remote_reserver.request_reservation(
	  item,
	  wrapped_on_reserved,
	  prio,
	  wrapped_on_preempt);
      },
      invoke_context_on_core(seastar::this_shard_id(), on_reserved),
      invoke_context_on_core(seastar::this_shard_id(), on_preempt));
  }
  seastar::future<> snap_request_reservation(
    spg_t item,
    Context *on_reserved,
    unsigned prio) {
    return with_singleton(
      [item, prio](OSDSingletonState &singleton,
		   Context *wrapped_on_reserved) {
	return singleton.snap_reserver.request_reservation(
	  item,
	  wrapped_on_reserved,
	  prio);
      },
      invoke_context_on_core(seastar::this_shard_id(), on_reserved));
  }

#undef FORWARD_CONST
#undef FORWARD
#undef FORWARD_TO_OSD_SINGLETON
#undef FORWARD_TO_LOCAL
#undef FORWARD_TO_LOCAL_CONST
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::OSDSingletonState::pg_temp_t> : fmt::ostream_formatter {};
#endif
