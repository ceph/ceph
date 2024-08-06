// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_shard_manager.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> PGShardManager::load_pgs(crimson::os::FuturizedStore& store)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return store.list_collections(
  ).then([this](auto colls_cores) {
    return seastar::parallel_for_each(
      colls_cores,
      [this](auto coll_core) {
        auto[coll, shard_core] = coll_core;
	spg_t pgid;
	if (coll.is_pg(&pgid)) {
          return get_pg_to_shard_mapping().get_or_create_pg_mapping(
            pgid, shard_core
          ).then([this, pgid] (auto core) {
            return this->template with_remote_shard_state(
              core,
              [pgid](
	      PerShardState &per_shard_state,
	      ShardServices &shard_services) {
	      return shard_services.load_pg(
		pgid
	      ).then([pgid, &per_shard_state](auto &&pg) {
		logger().info("load_pgs: loaded {}", pgid);
		per_shard_state.pg_map.pg_loaded(pgid, std::move(pg));
		return seastar::now();
	      });
	    });
          });
	} else if (coll.is_temp(&pgid)) {
	  logger().warn(
	    "found temp collection on crimson osd, should be impossible: {}",
	    coll);
	  ceph_assert(0 == "temp collection on crimson osd, should be impossible");
	  return seastar::now();
	} else {
	  logger().warn("ignoring unrecognized collection: {}", coll);
	  return seastar::now();
	}
      });
  });
}

seastar::future<> PGShardManager::stop_pgs()
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all([](auto &local_service) {
    return local_service.local_state.stop_pgs();
  });
}

seastar::future<std::map<pg_t, pg_stat_t>>
PGShardManager::get_pg_stats() const
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.map_reduce0(
    [](auto &local) {
      return local.local_state.get_pg_stats();
    },
    std::map<pg_t, pg_stat_t>(),
    [](auto &&left, auto &&right) {
      left.merge(std::move(right));
      return std::move(left);
    });
}

seastar::future<> PGShardManager::broadcast_map_to_pgs(epoch_t epoch)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all([epoch](auto &local_service) {
    return local_service.local_state.broadcast_map_to_pgs(
      local_service, epoch
    );
  }).then([this, epoch] {
    logger().debug("PGShardManager::broadcast_map_to_pgs "
                   "broadcasted up to {}",
                    epoch);
    return shard_services.invoke_on_all([epoch](auto &local_service) {
      local_service.local_state.osdmap_gate.got_map(epoch);
      return seastar::now();
    });
  });
}

seastar::future<> PGShardManager::set_up_epoch(epoch_t e) {
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all(
    seastar::smp_submit_to_options{},
    [e](auto &local_service) {
      local_service.local_state.set_up_epoch(e);
      return seastar::now();
    });
}

seastar::future<> PGShardManager::set_superblock(OSDSuperblock superblock) {
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  get_osd_singleton_state().set_singleton_superblock(superblock);
  return shard_services.invoke_on_all(
  [superblock = std::move(superblock)](auto &local_service) {
    return local_service.local_state.update_shard_superblock(superblock);
  });
}

}
