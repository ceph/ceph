// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
        auto[coll, shard_core_index] = coll_core;
        auto[shard_core, store_index] = shard_core_index;
	spg_t pgid;
	if (coll.is_pg(&pgid)) {
          return get_pg_to_shard_mapping().get_or_create_pg_mapping(
            pgid, shard_core, store_index
          ).then([this, pgid] (auto core_store) {
            return this->with_remote_shard_state(
              core_store.first,
              [pgid, core_store](
	      PerShardState &per_shard_state,
	      ShardServices &shard_services) {
	      return shard_services.load_pg(
		pgid, core_store.second
	      ).then([pgid, &per_shard_state](auto &&pg) {
		logger().info("load_pgs: loaded {}", pgid);
		return pg->clear_temp_objects(
		).then([&per_shard_state, pg, pgid] {
		  per_shard_state.pg_map.pg_loaded(pgid, std::move(pg));
		});
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

seastar::future<> PGShardManager::prime_merges(epoch_t first, epoch_t last)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  const int whoami = get_local_state().whoami;

  // Collect every merge participant (target + sources) this OSD is in the
  // acting set for, keyed to the epoch the merge occurs at.  Derived purely
  // from the OSDMap (pg_num shrink + CRUSH acting set) so participants are
  // found even when none are live locally yet -- e.g. a target CRUSH has just
  // remapped onto this OSD because pgp_num dropped alongside pg_num.
  std::set<std::pair<spg_t, epoch_t>> merge_pgs;

  // If first-1 was trimmed away, we have no local pre-merge map to compare;
  // start one epoch later (first itself is in the just-stored range).
  if (!get_osd_singleton_state().superblock.get_maps().contains(first - 1)) {
    ++first;
  }
  for (epoch_t e = first; e <= last; ++e) {
    cached_map_t prev = co_await get_shard_services().get_map(e - 1);
    cached_map_t cur  = co_await get_shard_services().get_map(e);

    for (const auto& [poolid, pool] : cur->get_pools()) {
      if (!prev->have_pg_pool(poolid)) {
        continue;
      }
      const unsigned old_pg_num = prev->get_pg_num(poolid);
      const unsigned new_pg_num = pool.get_pg_num();
      if (!new_pg_num || new_pg_num >= old_pg_num) {
        continue;  // not a merge step
      }

      // Each ps in [new_pg_num, old_pg_num) is a merge source; from any one we
      // derive the surviving target (is_merge_source climbs ancestors until
      // the seed drops below new_pg_num) and the full sibling source set.
      //
      // Replicated pools only (NO_SHARD) for now.  spg_t::is_split() and
      // is_merge_source() preserve the shard, so classic does not need a
      // separate EC path: OSDShard::identify_splits_and_merges() seeds
      // discovery from each live pg_slot, which already carries the real
      // shard id (e.g. 2.7s2).  We derive participants from the OSDMap alone
      // (no live-PG walk), so we must synthesize spg_t from pg_t seeds; for
      // EC that means enumerating each acting shard explicitly (future work).
      for (unsigned ps = new_pg_num; ps < old_pg_num; ++ps) {
        spg_t src(pg_t(ps, poolid), shard_id_t::NO_SHARD);
        spg_t target;
        if (!src.is_merge_source(old_pg_num, new_pg_num, &target)) {
          continue;
        }
        std::set<spg_t> participants;  // the sibling sources...
        target.is_split(new_pg_num, old_pg_num, &participants);
        participants.insert(target);   // ...plus the target
        for (const auto& p : participants) {
          if (cur->is_up_acting_osd_shard(p, whoami)) {
            merge_pgs.emplace(p, e);
          }
        }
      }
    }
  }

  logger().debug("PGShardManager::prime_merges: {} participants in e{}..{}",
                 merge_pgs.size(), first, last);

  // Create an empty placeholder for each participant on the shard its mapping
  // resolves to.  prime_merge_participant() is a no-op for any already live or
  // not actually owned here, and registers the rest at (merge_epoch - 1)
  // WITHOUT advancing; broadcast_map_to_pgs() advances all PGs together
  // through the merge epoch.
  for (const auto& [pgid, merge_epoch] : merge_pgs) {
    auto [core, store_index] = co_await get_pg_to_shard_mapping().get_or_create_pg_mapping(pgid);
    co_await shard_services.invoke_on(
      core,
      [pgid, store_index, merge_epoch](ShardServices& tss) {
        return tss.prime_merge_participant(pgid, store_index, merge_epoch);
      });
  }
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

seastar::future<uint64_t>
PGShardManager::calc_snap_trim_queue_total() const
{
  uint64_t total = 0;
  co_await for_each_pg([&total](const auto&, const auto &pg) {
    if (pg->is_primary()) {
      total += pg->get_snap_trimq_size();
    }
  });
  co_return total;
}

}
