// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>

#include "crimson/osd/shard_services.h"
#include "crimson/osd/pg_map.h"

namespace crimson::osd {

/**
 * PGShardManager
 *
 * Manages all state required to partition PGs over seastar reactors
 * as well as state required to route messages to pgs. Mediates access to
 * shared resources required by PGs (objectstore, messenger, monclient,
 * etc)
 */
class PGShardManager {
  std::unique_ptr<OSDSingletonState> osd_singleton_state;
  std::unique_ptr<ShardServices> shard_services;

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

#define FORWARD_TO_OSD_SINGLETON(METHOD) \
  FORWARD(METHOD, METHOD, get_osd_singleton_state())

public:
  using cached_map_t = OSDMapService::cached_map_t;
  using local_cached_map_t = OSDMapService::local_cached_map_t;

  PGShardManager() = default;

  seastar::future<> start(
    const int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);
  seastar::future<> stop();

  auto &get_osd_singleton_state() { return *osd_singleton_state; }
  auto &get_osd_singleton_state() const { return *osd_singleton_state; }
  auto &get_shard_services() { return *shard_services; }
  auto &get_shard_services() const { return *shard_services; }
  auto &get_local_state() { return shard_services->local_state; }
  auto &get_local_state() const { return shard_services->local_state; }

  seastar::future<> update_map(local_cached_map_t &&map) {
    auto fmap = make_local_shared_foreign(std::move(map));
    get_osd_singleton_state().update_map(fmap);
    get_local_state().update_map(std::move(fmap));
    return seastar::now();
  }

  auto stop_registries() {
    return get_local_state().stop_registry();
  }

  FORWARD_TO_OSD_SINGLETON(send_pg_created)

  // osd state forwards
  FORWARD(is_active, is_active, get_osd_singleton_state().osd_state)
  FORWARD(is_preboot, is_preboot, get_osd_singleton_state().osd_state)
  FORWARD(is_booting, is_booting, get_osd_singleton_state().osd_state)
  FORWARD(is_stopping, is_stopping, get_osd_singleton_state().osd_state)
  FORWARD(is_prestop, is_prestop, get_osd_singleton_state().osd_state)
  FORWARD(is_initializing, is_initializing, get_osd_singleton_state().osd_state)
  FORWARD(set_prestop, set_prestop, get_osd_singleton_state().osd_state)
  FORWARD(set_preboot, set_preboot, get_osd_singleton_state().osd_state)
  FORWARD(set_booting, set_booting, get_osd_singleton_state().osd_state)
  FORWARD(set_stopping, set_stopping, get_osd_singleton_state().osd_state)
  FORWARD(set_active, set_active, get_osd_singleton_state().osd_state)
  FORWARD(when_active, when_active, get_osd_singleton_state().osd_state)
  FORWARD_CONST(get_osd_state_string, to_string, get_osd_singleton_state().osd_state)

  FORWARD(got_map, got_map, get_osd_singleton_state().osdmap_gate)
  FORWARD(wait_for_map, wait_for_map, get_osd_singleton_state().osdmap_gate)

  // Metacoll
  FORWARD_TO_OSD_SINGLETON(init_meta_coll)
  FORWARD_TO_OSD_SINGLETON(get_meta_coll)

  // Core OSDMap methods
  FORWARD_TO_OSD_SINGLETON(get_local_map)
  FORWARD_TO_OSD_SINGLETON(load_map_bl)
  FORWARD_TO_OSD_SINGLETON(load_map_bls)
  FORWARD_TO_OSD_SINGLETON(store_maps)

  seastar::future<> set_up_epoch(epoch_t e);

  template <typename F>
  auto with_remote_shard_state(core_id_t core, F &&f) {
    ceph_assert(core == 0);
    auto &local_state_ref = get_local_state();
    auto &shard_services_ref = get_shard_services();
    return seastar::smp::submit_to(
      core,
      [f=std::forward<F>(f), &local_state_ref, &shard_services_ref]() mutable {
	return std::invoke(
	  std::move(f), local_state_ref, shard_services_ref);
      });
  }

  /// Runs opref on the appropriate core, creating the pg as necessary.
  template <typename T>
  seastar::future<> run_with_pg_maybe_create(
    typename T::IRef op
  ) {
    ceph_assert(op->use_count() == 1);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    static_assert(T::can_create());
    logger.debug("{}: can_create", *op);

    auto core = get_osd_singleton_state().pg_to_shard_mapping.maybe_create_pg(
      op->get_pgid());

    get_local_state().registry.remove_from_registry(*op);
    return with_remote_shard_state(
      core,
      [op=std::move(op)](
	PerShardState &per_shard_state,
	ShardServices &shard_services) mutable {
	per_shard_state.registry.add_to_registry(*op);
	auto &logger = crimson::get_logger(ceph_subsys_osd);
	auto &opref = *op;
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([&shard_services, &opref](
	      auto &&trigger) {
	    return shard_services.get_or_create_pg(
	      std::move(trigger),
	      opref.get_pgid(), opref.get_epoch(),
	      std::move(opref.get_create_info()));
	  }).then([&logger, &shard_services, &opref](Ref<PG> pgref) {
	    logger.debug("{}: have_pg", opref);
	    return opref.with_pg(shard_services, pgref);
	  }).then([op=std::move(op)] {});
      });
  }

  /// Runs opref on the appropriate core, waiting for pg as necessary
  template <typename T>
  seastar::future<> run_with_pg_maybe_wait(
    typename T::IRef op
  ) {
    ceph_assert(op->use_count() == 1);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    static_assert(!T::can_create());
    logger.debug("{}: !can_create", *op);

     auto core = get_osd_singleton_state().pg_to_shard_mapping.maybe_create_pg(
      op->get_pgid());

    get_local_state().registry.remove_from_registry(*op);
    return with_remote_shard_state(
      core,
      [op=std::move(op)](
	PerShardState &per_shard_state,
	ShardServices &shard_services) mutable {
	per_shard_state.registry.add_to_registry(*op);
	auto &logger = crimson::get_logger(ceph_subsys_osd);
	auto &opref = *op;
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([&shard_services, &opref](
	      auto &&trigger) {
	    return shard_services.wait_for_pg(
	      std::move(trigger), opref.get_pgid());
	  }).then([&logger, &shard_services, &opref](Ref<PG> pgref) {
	    logger.debug("{}: have_pg", opref);
	    return opref.with_pg(shard_services, pgref);
	  }).then([op=std::move(op)] {});
      });
  }

  seastar::future<> load_pgs();
  seastar::future<> stop_pgs();

  seastar::future<std::map<pg_t, pg_stat_t>> get_pg_stats() const;

  /**
   * for_each_pg
   *
   * Invokes f on each pg sequentially.  Caller may rely on f not being
   * invoked concurrently on multiple cores.
   */
  template <typename F>
  seastar::future<> for_each_pg(F &&f) const {
    for (auto &&pg: get_local_state().pg_map.get_pgs()) {
      std::apply(f, pg);
    }
    return seastar::now();
  }

  auto get_num_pgs() const {
    return get_osd_singleton_state().pg_to_shard_mapping.get_num_pgs();
  }

  seastar::future<> broadcast_map_to_pgs(epoch_t epoch);

  template <typename F>
  auto with_pg(spg_t pgid, F &&f) {
    core_id_t core = get_osd_singleton_state(
    ).pg_to_shard_mapping.get_pg_mapping(pgid);
    return with_remote_shard_state(
      core,
      [pgid, f=std::move(f)](auto &local_state, auto &local_service) mutable {
	return std::invoke(
	  std::move(f),
	  local_state.pg_map.get_pg(pgid));
      });
  }

  template <typename T, typename... Args>
  auto start_pg_operation(Args&&... args) {
    auto op = get_local_state().registry.create_operation<T>(
      std::forward<Args>(args)...);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    logger.debug("{}: starting {}", *op, __func__);

    auto &opref = *op;
    auto id = op->get_id();
    auto fut = opref.template enter_stage<>(
      opref.get_connection_pipeline().await_active
    ).then([this, &opref, &logger] {
      logger.debug("{}: start_pg_operation in await_active stage", opref);
      return get_osd_singleton_state().osd_state.when_active();
    }).then([&logger, &opref] {
      logger.debug("{}: start_pg_operation active, entering await_map", opref);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().await_map);
    }).then([this, &logger, &opref] {
      logger.debug("{}: start_pg_operation await_map stage", opref);
      using OSDMapBlockingEvent =
	OSD_OSDMapGate::OSDMapBlocker::BlockingEvent;
      return opref.template with_blocking_event<OSDMapBlockingEvent>(
	[this, &opref](auto &&trigger) {
	  std::ignore = this;
	  return get_osd_singleton_state().osdmap_gate.wait_for_map(
	    std::move(trigger),
	    opref.get_epoch(),
	    &get_shard_services());
	});
    }).then([&logger, &opref](auto epoch) {
      logger.debug("{}: got map {}, entering get_pg", opref, epoch);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().get_pg);
    }).then([this, &logger, &opref, op=std::move(op)]() mutable {
      logger.debug("{}: in get_pg", opref);
      if constexpr (T::can_create()) {
	logger.debug("{}: can_create", opref);
	return run_with_pg_maybe_create<T>(std::move(op));
      } else {
	logger.debug("{}: !can_create", opref);
	return run_with_pg_maybe_wait<T>(std::move(op));
      }
    });
    return std::make_pair(id, std::move(fut));
  }

#undef FORWARD
#undef FORWARD_CONST
#undef FORWARD_TO_OSD_SINGLETON
};

}
