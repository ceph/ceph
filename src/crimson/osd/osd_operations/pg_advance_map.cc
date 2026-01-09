// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/coroutine/parallel_for_each.hh>
#include "osd/PeeringState.h"

SET_SUBSYS(osd);

namespace crimson::osd {

PGAdvanceMap::PGAdvanceMap(
  Ref<PG> pg, ShardServices &shard_services, epoch_t to,
  PeeringCtx &&rctx, bool do_init)
  : pg(pg), shard_services(shard_services), to(to),
    rctx(std::move(rctx)), do_init(do_init)
{
  LOG_PREFIX(PGAdvanceMap);
  DEBUG("{}: created", *this);
}

PGAdvanceMap::~PGAdvanceMap() {}

void PGAdvanceMap::print(std::ostream &lhs) const
{
  lhs << "PGAdvanceMap("
      << "pg=" << pg->get_pgid()
      << " from=" << (from ? *from : -1)
      << " to=" << to;
  if (do_init) {
    lhs << " do_init";
  }
  lhs << ")";
}

void PGAdvanceMap::dump_detail(Formatter *f) const
{
  f->open_object_section("PGAdvanceMap");
  f->dump_stream("pgid") << pg->get_pgid();
  if (from) {
    f->dump_int("from", *from);
  }
  f->dump_int("to", to);
  f->dump_bool("do_init", do_init);
  f->close_section();
}

PGPeeringPipeline &PGAdvanceMap::peering_pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

seastar::future<> PGAdvanceMap::start()
{
  LOG_PREFIX(PGAdvanceMap::start);
  using cached_map_t = OSDMapService::cached_map_t;

  DEBUG("{}: start", *this);
  IRef ref = this;

  // This executes when the function returns
  auto finally = seastar::defer([this, FNAME] {
    DEBUG("{}: exit", *this);
    handle.exit();
  });

  co_await enter_stage<>(peering_pp(*pg).process);
  /*
   * PGAdvanceMap is scheduled at pg creation and when
   * broadcasting new osdmaps to pgs. We are not able to serialize
   * between the two different PGAdvanceMap callers since a new pg
   * will get advanced to the latest osdmap at it's creation.
   * As a result, we may need to adjust the PGAdvance operation
   * 'from' epoch.
   * See: https://tracker.ceph.com/issues/61744
   */
  from = pg->get_osdmap_epoch();
  if (do_init) {
    pg->handle_initialize(rctx);
    pg->handle_activate_map(rctx);
  }
  ceph_assert(std::cmp_less_equal(*from, to));

  // is_merge ensures we stop advancing maps immediately if a merge occurs.
  // In merge_pg(), we take care of continuing advancing map for the target (parent).
  bool is_merge = false;
  for (epoch_t next_epoch = *from + 1; next_epoch <= to; ++next_epoch) {
    DEBUG("{}: start: getting map {}", *this, next_epoch);
    cached_map_t next_map = co_await shard_services.get_map(next_epoch);
    is_merge = co_await check_for_merges(*from, next_map, rctx);
    if (is_merge) {
      DEBUG("{}: stopping advance due to merge", *this);
      break;
    }
    DEBUG("{}: advancing map to {}", *this, next_map->get_epoch());
    pg->handle_advance_map(next_map, rctx);
    co_await check_for_splits(*from, next_map);
  }

  if (is_merge) {
    // If we merged, the merge_pg logic has already handled the activation
    // and rctx completion. We exit here.
    DEBUG("{}: skipping activation because merge is in progress", *this);
  } else {
    pg->handle_activate_map(rctx);
    DEBUG("{}: map activated", *this);

    if (do_init) {
        shard_services.pg_created(pg->get_pgid(), pg);
        INFO("PGAdvanceMap::start new pg {}", *pg);
    }
    co_await pg->complete_rctx(std::move(rctx));
  }
  DEBUG("{}: complete", *this);
  co_await handle.complete();
  co_return;
}

seastar::future<> PGAdvanceMap::check_for_splits(
    epoch_t old_epoch,
    cached_map_t next_map)
{
  LOG_PREFIX(PGAdvanceMap::check_for_splits);
  using cached_map_t = OSDMapService::cached_map_t;
  cached_map_t old_map = co_await shard_services.get_map(old_epoch);
  if (!old_map->have_pg_pool(pg->get_pgid().pool())) {
    DEBUG("{} pool doesn't exist in epoch {}", pg->get_pgid(),
	old_epoch);
    co_return;
  }
  auto old_pg_num = old_map->get_pg_num(pg->get_pgid().pool());
  if (!next_map->have_pg_pool(pg->get_pgid().pool())) {
    DEBUG("{} pool doesn't exist in epoch {}", pg->get_pgid(),
        next_map->get_epoch());
    co_return;
  }
  auto new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
  DEBUG(" pg_num change in e{} {} -> {}", next_map->get_epoch(),
                 old_pg_num, new_pg_num);
  std::set<spg_t> children;
  if (new_pg_num && new_pg_num > old_pg_num) {
    if (pg->pgid.is_split(
	    old_pg_num,
	    new_pg_num,
	    &children)) {
      co_await split_pg(children, next_map);
    }
  }
  co_return;
}

seastar::future<> PGAdvanceMap::split_pg(
    std::set<spg_t> split_children,
    cached_map_t next_map)
{
  LOG_PREFIX(PGAdvanceMap::split_pg);
  DEBUG("{}: start", *this);
  auto pg_epoch = next_map->get_epoch();
  DEBUG("{}: epoch: {}", *this, pg_epoch);

  unsigned new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
  pg->update_snap_mapper_bits(pg->get_pgid().get_split_bits(new_pg_num));

  co_await seastar::coroutine::parallel_for_each(split_children, [this, &next_map,
  pg_epoch, FNAME] (auto child_pgid) -> seastar::future<> {
    children_pgids.insert(child_pgid);

    // Map each child pg ID to a core
    auto core = co_await shard_services.create_split_pg_mapping(child_pgid, seastar::this_shard_id());
    DEBUG(" PG {} mapped to {}", child_pgid.pgid, core);
    DEBUG(" {} map epoch: {}", child_pgid.pgid, pg_epoch);
    auto map = next_map;
    auto child_pg = co_await shard_services.make_pg(std::move(map), child_pgid, true);

    DEBUG(" Parent pgid: {}", pg->get_pgid());
    DEBUG(" Child pgid: {}", child_pg->get_pgid());
    unsigned new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
    // Depending on the new_pg_num the parent PG's collection is split.
    // The child PG will be initiated with this split collection.
    unsigned split_bits = child_pg->get_pgid().get_split_bits(new_pg_num);
    DEBUG(" pg num is {}, m_seed is {}, split bits is {}",
	new_pg_num, child_pg->get_pgid().ps(), split_bits);

    co_await pg->split_colls(child_pg->get_pgid(), split_bits, child_pg->get_pgid().ps(),
                             &child_pg->get_pgpool().info, rctx.transaction);
    DEBUG(" {} split collection done", child_pg->get_pgid());
    // Update the child PG's info from the parent PG
    pg->split_into(child_pg->get_pgid().pgid, child_pg, split_bits);
    // Make SnapMapper OID for the child PG
    auto child_coll_ref = child_pg->get_collection_ref();
    rctx.transaction.touch(child_coll_ref->get_cid(), child_pg->get_pgid().make_snapmapper_oid());

    co_await handle_split_pg_creation(child_pg, next_map);
    split_pgs.insert(child_pg);
  });

  split_stats(split_pgs, children_pgids);
  co_return;
}

seastar::future<> PGAdvanceMap::handle_split_pg_creation(
    Ref<PG> child_pg,
    cached_map_t next_map)
{
  LOG_PREFIX(PGAdvanceMap::handle_split_pg_creation);
  // We must create a new Trigger instance for each pg.
  // The BlockingEvent object which tracks whether a pg creation is complete
  // or still blocking, shouldn't be used across multiple pgs so we can track
  // each splitted pg creation separately.
  using EventT = PGMap::PGCreationBlockingEvent;
  using TriggerT = EventT::Trigger<PGAdvanceMap>;
  EventT event;
  TriggerT trigger(event, *this);
  // create_split_pg waits for the pg to be created.
  // The completion of the creation depends on running PGAdvanceMap operation on that pg
  // Therefore, we must invoke PGAdvanceMap before awaiting the future returned by create_split_pg.
  // Otherwise, we would be waiting indefinitely, as the pg creation
  // cannot complete without PGAdvanceMap being executed.
  // See: ShardServices::get_or_create_pg for similar dependency
  // when calling handle_pg_create_info before returning a pg creation future.
  auto fut = shard_services.create_split_pg(std::move(trigger),
      child_pg->get_pgid()).handle_error(crimson::ct_error::ecanceled::handle([FNAME](auto) {
	DEBUG("PG creation canceled");
	return seastar::make_ready_future<Ref<PG>>();
  }));
  co_await shard_services.start_operation<PGAdvanceMap>(
      child_pg, shard_services, next_map->get_epoch(),
      std::move(rctx), true).second;
  co_await std::move(fut);
}


void PGAdvanceMap::split_stats(std::set<Ref<PG>> children_pgs,
                              const std::set<spg_t> &children_pgids)
{
  std::vector<object_stat_sum_t> updated_stats;
  pg->start_split_stats(children_pgids, &updated_stats);
  std::vector<object_stat_sum_t>::iterator stat_iter = updated_stats.begin();
  for (std::set<Ref<PG>>::const_iterator iter = children_pgs.begin();
       iter != children_pgs.end();
       ++iter, ++stat_iter) {
        (*iter)->finish_split_stats(*stat_iter, rctx.transaction);
      }
  pg->finish_split_stats(*stat_iter, rctx.transaction);
}

seastar::future<bool> PGAdvanceMap::check_for_merges(
    epoch_t old_epoch,
    cached_map_t next_map,
    PeeringCtx &rctx)
{
  LOG_PREFIX(PGAdvanceMap::check_for_merges);
  using cached_map_t = OSDMapService::cached_map_t;
  cached_map_t old_map = co_await shard_services.get_map(old_epoch);
  if (!old_map->have_pg_pool(pg->get_pgid().pool())) {
    DEBUG("{} pool doesn't exist in epoch {}", pg->get_pgid(),
	old_epoch);
    co_return false;
  }
  auto old_pg_num = old_map->get_pg_num(pg->get_pgid().pool());
  if (!next_map->have_pg_pool(pg->get_pgid().pool())) {
    DEBUG("{} pool doesn't exist in epoch {}", pg->get_pgid(),
        next_map->get_epoch());
    co_return false;
  }
  auto new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
  DEBUG("{} pg_num change in e{} {} -> {}", pg->get_pgid(), next_map->get_epoch(),
                 old_pg_num, new_pg_num);
  if (new_pg_num && new_pg_num < old_pg_num) {
      bool merged = co_await merge_pg(next_map, new_pg_num, old_pg_num, rctx);
      co_return merged;
  }
  co_return false;
}

seastar::future<bool> PGAdvanceMap::merge_pg(
    cached_map_t next_map,
    unsigned new_pg_num,
    unsigned old_pg_num,
    PeeringCtx &rctx)
{
  LOG_PREFIX(PGAdvanceMap::merge_pg);
  DEBUG("{}: start", *this);
  spg_t parent;
  std::map<spg_t, Ref<PG>> source_pgs;
  std::set<spg_t> merge_sources;
  // The PG is a Source (to be merged into a parent)
  if (pg->pgid.is_merge_source(old_pg_num,
	                       new_pg_num,
			       &parent)) {
    parent.is_split(new_pg_num, old_pg_num, &merge_sources);
    DEBUG("Source pg {} shutdown to avoid any operations", pg->get_pgid());
    co_await pg->stop();
    DEBUG("pg {} is a merge source, register it", pg->get_pgid());
    co_await shard_services.register_merge_source(parent, pg->get_pgid(),
	                                          merge_sources.size());
    co_return true;
  // The PG is a Target
  } else if (pg->pgid.is_merge_target(old_pg_num,
	                              new_pg_num)) {
    DEBUG("Target PG {} identified. Waiting for sources...", pg->get_pgid());
    pg->pgid.is_split(new_pg_num, old_pg_num, &merge_sources);
    
    // Block until all source PGs (potentially from other shards) arrive
    auto sources = co_await shard_services.wait_for_merge_sources(pg->get_pgid(), std::move(merge_sources));
    
    // Perform merge
    unsigned split_bits = pg->get_pgid().get_split_bits(new_pg_num);
    const auto& merge_meta = next_map->get_pg_pool(pg->get_pgid().pool())->last_pg_merge_meta;
    pg->merge_from(sources, rctx, split_bits, merge_meta);

    // Update PeeringState and activate the new map
    pg->handle_advance_map(next_map, rctx);
    pg->handle_activate_map(rctx); 

    co_await pg->complete_rctx(std::move(rctx));
    // Return sources to their home shards for destruction
    co_await shard_services.perform_source_cleanup(pg->get_pgid());

    co_return true;
  } else {
    co_return false;
  }
}

}
