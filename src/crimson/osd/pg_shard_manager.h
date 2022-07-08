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

  FORWARD_TO_CORE(get_or_create_pg)
  FORWARD_TO_CORE(wait_for_pg)
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
};

}
