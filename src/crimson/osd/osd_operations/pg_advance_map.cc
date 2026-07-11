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

  DEBUG("{}: start", *this);
  auto exit_handle = seastar::defer([FNAME, thiz = IRef{this}] {
    DEBUG("{}: exit", *thiz);
    thiz->handle.exit();
  });

  co_await enter_stage<>(peering_pp(*pg).process);

  // pg may have been deleted while this op was queued; see PG::do_delete_work.
  if (pg->is_deleted()) {
    DEBUG("{}: pg is deleted, skipping advance", *this);
    co_await pg->complete_rctx(std::move(rctx));
    co_await handle.complete();
    exit_handle.cancel();
    co_return;
  }

  epoch_t from = pg->get_osdmap_epoch();
  if (do_init) {
    pg->handle_initialize(rctx);
    pg->handle_activate_map(rctx);
  }
  ceph_assert(std::cmp_less_equal(from, to));

  for (epoch_t current_epoch = from; current_epoch < to; ++current_epoch) {
    epoch_t next_epoch = current_epoch + 1;
    DEBUG("{}: start: getting map {}", *this, next_epoch);
    cached_map_t next_map = co_await shard_services.get_map(next_epoch);
    merge_result_t merge_result =
      co_await check_for_merges(current_epoch, next_map, rctx);
    if (merge_result.role == merge_role_t::Source) {
      // Merge source: hand the PG off to the target and finish.  The PG
      // stops existing locally, so there is nothing left to advance here.
      DEBUG("{}: merge source, handing off after e{}", *this, current_epoch);
      co_await finish_merge_source(merge_result.parent, rctx);
      co_await handle.complete();
      exit_handle.cancel();
      co_return;
    }
    // A successful merge target (merge already applied by check_for_merges)
    // and the no-merge case both advance this epoch and keep iterating, so
    // the PG advances all the way to `to`, matching the classic OSD instead
    // of stalling at an intermediate epoch.
    DEBUG("{}: advancing map to {}", *this, next_map->get_epoch());
    // Baseline epoch for split detection: PG map epoch before advancing to next_map.
    epoch_t split_epoch = pg->get_osdmap_epoch();
    pg->handle_advance_map(next_map, rctx);
    DEBUG("{}: checking for splits between {} and {}",
          *this, split_epoch, next_map->get_epoch());
    co_await check_for_splits(split_epoch, std::move(next_map));
  }

  // Normal completion, which also covers a completed merge target: the PG was
  // fully advanced to `to` in the loop above, so just activate the final map
  // and commit.
  pg->handle_activate_map(rctx);
  DEBUG("{}: map activated", *this);
  if (do_init) {
    shard_services.pg_created(pg->get_pgid(), pg);
    INFO("PGAdvanceMap::start new pg {}", *pg);
  }
  co_await pg->complete_rctx(std::move(rctx));
  DEBUG("{}: complete", *this);

  co_await handle.complete();
  exit_handle.cancel();
}

seastar::future<> PGAdvanceMap::check_for_splits(
    epoch_t old_epoch,
    cached_map_t next_map)
{
  LOG_PREFIX(PGAdvanceMap::check_for_splits);
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

  // Process children sequentially to avoid a race where concurrent
  // split_collection transactions overwrite the parent's split_bits
  // with a lower value. A future optimisation could re-introduce
  // parallel_for_each once we can guarantee that the bits would
  // be applied in order. 
  for (auto child_pgid : split_children) {
    children_pgids.insert(child_pgid);
    auto child_pg = co_await handle_split_pg_creation(child_pgid, next_map);
    split_pgs.insert(child_pg);
  }

  split_stats(split_pgs, children_pgids);
}

seastar::future<Ref<PG>> PGAdvanceMap::handle_split_pg_creation(
    spg_t child_pgid,
    cached_map_t next_map)
{
  LOG_PREFIX(PGAdvanceMap::handle_split_pg_creation);

  // Map each child pg ID to a core
  auto core = co_await shard_services.create_split_pg_mapping(child_pgid, seastar::this_shard_id(), pg->get_store_index());
  DEBUG(" PG {} mapped to {}", child_pgid.pgid, core);
  DEBUG(" {} map epoch: {}", child_pgid.pgid, next_map->get_epoch());
  auto child_pg = co_await shard_services.make_pg(next_map, child_pgid, pg->get_store_index(), true);

  DEBUG(" Parent pgid: {}", pg->get_pgid());
  DEBUG(" Child pgid: {}", child_pg->get_pgid());
  unsigned new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
  unsigned split_bits = child_pg->get_pgid().get_split_bits(new_pg_num);
  DEBUG(" pg num is {}, m_seed is {}, split bits is {}",
	new_pg_num, child_pg->get_pgid().ps(), split_bits);

  PeeringCtx child_rctx;
  co_await pg->split_colls(child_pg->get_pgid(), split_bits, child_pg->get_pgid().ps(),
                           &child_pg->get_pgpool().info, child_rctx.transaction);
  DEBUG(" {} split collection done", child_pg->get_pgid());
  pg->split_into(child_pg->get_pgid().pgid, child_pg, split_bits);
  auto child_coll_ref = child_pg->get_collection_ref();
  child_rctx.transaction.touch(child_coll_ref->get_cid(), child_pg->get_pgid().make_snapmapper_oid());

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
      std::move(child_rctx), true).second;
  co_await std::move(fut);
  co_return child_pg;
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

seastar::future<PGAdvanceMap::merge_result_t> PGAdvanceMap::check_for_merges(
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
    co_return merge_result_t{};
  }
  auto old_pg_num = old_map->get_pg_num(pg->get_pgid().pool());
  if (!next_map->have_pg_pool(pg->get_pgid().pool())) {
    DEBUG("{} pool doesn't exist in epoch {}", pg->get_pgid(),
        next_map->get_epoch());
    co_return merge_result_t{};
  }
  auto new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
  DEBUG("{} pg_num change in e{} {} -> {}", pg->get_pgid(), next_map->get_epoch(),
                 old_pg_num, new_pg_num);
  if (new_pg_num && new_pg_num < old_pg_num) {
    co_return co_await merge_pg(next_map, new_pg_num, old_pg_num, rctx);
  }
  co_return merge_result_t{};
}

seastar::future<PGAdvanceMap::merge_result_t> PGAdvanceMap::merge_pg(
    cached_map_t next_map,
    unsigned new_pg_num,
    unsigned old_pg_num,
    PeeringCtx &rctx)
{
  LOG_PREFIX(PGAdvanceMap::merge_pg);
  DEBUG("{}: start", *this);
  spg_t parent;
  std::set<spg_t> merge_sources;
  if (pg->pgid.is_merge_source(old_pg_num,
                               new_pg_num,
                               &parent)) {
    parent.is_split(new_pg_num, old_pg_num, &merge_sources);
    if (!co_await shard_services.seastore_merge_shards_ok(
          parent, merge_sources)) {
      co_return merge_result_t{};
    }
    co_return merge_result_t{merge_role_t::Source, parent};
  } else if (pg->pgid.is_merge_target(old_pg_num,
                                       new_pg_num)) {
    DEBUG("Target PG {} identified. Waiting for sources...", pg->get_pgid());
    pg->pgid.is_split(new_pg_num, old_pg_num, &merge_sources);

    if (!co_await shard_services.seastore_merge_shards_ok(
          pg->get_pgid(), merge_sources)) {
      co_return merge_result_t{};
    }
    // Block until all source PGs (potentially from other shards) arrive
    // on this PG's rendezvous
    auto sources = co_await pg->collect_merge_sources(merge_sources.size());
    if (sources.empty()) {
      co_return merge_result_t{};
    }

    unsigned split_bits = pg->get_pgid().get_split_bits(new_pg_num);
    const auto& merge_meta =
      next_map->get_pg_pool(pg->get_pgid().pool())->last_pg_merge_meta;
    pg->merge_from(sources, rctx, split_bits, merge_meta);

    co_return merge_result_t{merge_role_t::Target};
  } else {
    co_return merge_result_t{};
  }
}

seastar::future<> PGAdvanceMap::finish_merge_source(
    spg_t parent,
    PeeringCtx &rctx)
{
  LOG_PREFIX(PGAdvanceMap::finish_merge_source);
  DEBUG("{}: committing rctx before handoff to target {}", *this, parent);
  co_await pg->complete_rctx(std::move(rctx));
  co_await pg->stop();
  co_await shard_services.register_merge_source(parent, pg->get_pgid());
}

}
