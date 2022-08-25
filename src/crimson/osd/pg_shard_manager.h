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
  CoreState core_state;
  PerShardState local_state;
  ShardServices shard_services;

public:
  PGShardManager(
    const int whoami,
    crimson::net::Messenger &cluster_msgr,
    crimson::net::Messenger &public_msgr,
    crimson::mon::Client &monc,
    crimson::mgr::Client &mgrc,
    crimson::os::FuturizedStore &store);

  auto &get_shard_services() { return shard_services; }

  void update_map(OSDMapService::cached_map_t map) {
    core_state.update_map(map);
    local_state.update_map(map);
  }

  auto stop_registries() {
    return local_state.stop_registry();
  }

  FORWARD_TO_CORE(send_pg_created)

  // osd state forwards
  FORWARD(is_active, is_active, core_state.osd_state)
  FORWARD(is_preboot, is_preboot, core_state.osd_state)
  FORWARD(is_booting, is_booting, core_state.osd_state)
  FORWARD(is_stopping, is_stopping, core_state.osd_state)
  FORWARD(is_prestop, is_prestop, core_state.osd_state)
  FORWARD(is_initializing, is_initializing, core_state.osd_state)
  FORWARD(set_prestop, set_prestop, core_state.osd_state)
  FORWARD(set_preboot, set_preboot, core_state.osd_state)
  FORWARD(set_booting, set_booting, core_state.osd_state)
  FORWARD(set_stopping, set_stopping, core_state.osd_state)
  FORWARD(set_active, set_active, core_state.osd_state)
  FORWARD(when_active, when_active, core_state.osd_state)
  FORWARD_CONST(get_osd_state_string, to_string, core_state.osd_state)

  FORWARD(got_map, got_map, core_state.osdmap_gate)
  FORWARD(wait_for_map, wait_for_map, core_state.osdmap_gate)

  // Metacoll
  FORWARD_TO_CORE(init_meta_coll)
  FORWARD_TO_CORE(get_meta_coll)

  // Core OSDMap methods
  FORWARD_TO_CORE(get_map)
  FORWARD_TO_CORE(load_map_bl)
  FORWARD_TO_CORE(load_map_bls)
  FORWARD_TO_CORE(store_maps)
  FORWARD_TO_CORE(get_up_epoch)
  FORWARD_TO_CORE(set_up_epoch)

  FORWARD(pg_created, pg_created, core_state.pg_map)
  auto load_pgs() {
    return core_state.load_pgs(shard_services);
  }
  FORWARD_TO_CORE(stop_pgs)
  FORWARD_CONST(get_pg_stats, get_pg_stats, core_state)

  FORWARD_CONST(for_each_pg, for_each_pg, core_state)
  auto get_num_pgs() const { return core_state.pg_map.get_pgs().size(); }

  auto broadcast_map_to_pgs(epoch_t epoch) {
    return core_state.broadcast_map_to_pgs(
      *this, shard_services, epoch);
  }

  template <typename F>
  auto with_pg(spg_t pgid, F &&f) {
    return std::invoke(std::forward<F>(f), core_state.get_pg(pgid));
  }

  template <typename T, typename... Args>
  auto start_pg_operation(Args&&... args) {
    auto op = local_state.registry.create_operation<T>(
      std::forward<Args>(args)...);
    auto &logger = crimson::get_logger(ceph_subsys_osd);
    logger.debug("{}: starting {}", *op, __func__);
    auto &opref = *op;

    auto fut = opref.template enter_stage<>(
      opref.get_connection_pipeline().await_active
    ).then([this, &opref, &logger] {
      logger.debug("{}: start_pg_operation in await_active stage", opref);
      return core_state.osd_state.when_active();
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
	  return core_state.osdmap_gate.wait_for_map(
	    std::move(trigger),
	    opref.get_epoch(),
	    &shard_services);
	});
    }).then([&logger, &opref](auto epoch) {
      logger.debug("{}: got map {}, entering get_pg", opref, epoch);
      return opref.template enter_stage<>(
	opref.get_connection_pipeline().get_pg);
    }).then([this, &logger, &opref] {
      logger.debug("{}: in get_pg", opref);
      if constexpr (T::can_create()) {
	logger.debug("{}: can_create", opref);
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    return core_state.get_or_create_pg(
	      *this,
	      shard_services,
	      std::move(trigger),
	      opref.get_pgid(), opref.get_epoch(),
	      std::move(opref.get_create_info()));
	  });
      } else {
	logger.debug("{}: !can_create", opref);
	return opref.template with_blocking_event<
	  PGMap::PGCreationBlockingEvent
	  >([this, &opref](auto &&trigger) {
	    std::ignore = this; // avoid clang warning
	    return core_state.wait_for_pg(std::move(trigger), opref.get_pgid());
	  });
      }
    }).then([this, &logger, &opref](Ref<PG> pgref) {
      logger.debug("{}: have_pg", opref);
      return opref.with_pg(get_shard_services(), pgref);
    }).then([op] { /* Retain refcount on op until completion */ });

    return std::make_pair(std::move(op), std::move(fut));
  }
};

}
