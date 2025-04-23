// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include <boost/iterator/counting_iterator.hpp>
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGAdvanceMap::PGAdvanceMap(
  Ref<PG> pg, ShardServices &shard_services, epoch_t to,
  PeeringCtx &&rctx, bool do_init,
  std::optional<std::set<std::pair<spg_t, epoch_t>>> split_children)
  : pg(pg), shard_services(shard_services), to(to),
    rctx(std::move(rctx)), do_init(do_init), split_children(split_children)
{
  logger().debug("{}: created", *this);
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
  using cached_map_t = OSDMapService::cached_map_t;

  logger().debug("{}: start", *this);

  IRef ref = this;
  return enter_stage<>(
    peering_pp(*pg).process
  ).then([this] {
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
    return seastar::do_for_each(
      boost::make_counting_iterator(*from + 1),
      boost::make_counting_iterator(to + 1),
      [this](epoch_t next_epoch) {
	logger().debug("{}: start: getting map {}",
		       *this, next_epoch);
	return shard_services.get_map(next_epoch).then(
	  [this] (cached_map_t&& next_map) {
	    logger().debug("{}: advancing map to {}",
			   *this, next_map->get_epoch());
	    pg->handle_advance_map(next_map, rctx);
	    if (split_children.has_value()) {
	      logger().debug("{}: split PGs detected", *this);
	      auto split_children_info = split_children.value();
	      return split_pg(split_children_info,
		              next_map);
	    } else {
	      return seastar::now();
	    }
	  });
      }).then([this] {
	pg->handle_activate_map(rctx);
	logger().debug("{}: map activated", *this);
	if (do_init) {
	  shard_services.pg_created(pg->get_pgid(), pg);
	  logger().info("PGAdvanceMap::start new pg {}", *pg);
	}
	return pg->complete_rctx(std::move(rctx));
      });
  }).then([this] {
    logger().debug("{}: complete", *this);
    return handle.complete();
  }).finally([this, ref=std::move(ref)] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

seastar::future<> PGAdvanceMap::split_pg(
    std::set<std::pair<spg_t, epoch_t>> split_children_info,
    cached_map_t next_map)
{
  using cached_map_t = OSDMapService::cached_map_t;
  logger().debug("{}: start", *this);

  for (auto& child_pg_info : split_children_info) {
    auto child_pgid = child_pg_info.first;
    auto pg_epoch = child_pg_info.second;
    children_pgids.insert(child_pgid);

    // Map each child pg ID to a core
    auto core = co_await shard_services.create_split_pg_mapping(child_pgid, seastar::this_shard_id());
    logger().debug(" PG {} mapped to {}", child_pgid.pgid, core);
    logger().debug(" {} map epoch: {}", child_pgid.pgid, pg_epoch);
    cached_map_t map = co_await shard_services.get_map(pg_epoch);
    auto child_pg = co_await shard_services.make_pg(std::move(map), child_pgid, true);

    logger().debug(" Parent PG: {}", pg->get_pgid());
    logger().debug(" Child PG ID: {}", child_pg->get_pgid());
    unsigned new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
    // Depending on the new_pg_num the parent PG's collection is split.
    // The child PG will be initiated with this split collection.
    unsigned split_bits = child_pg->get_pgid().get_split_bits(new_pg_num);
    logger().debug(" pg num is {}, m_seed is {}, split bits is {}",
	            new_pg_num, child_pg->get_pgid().ps(), split_bits);

    co_await pg->split_colls(child_pg->get_pgid(), split_bits, child_pg->get_pgid().ps(),
                             &child_pg->get_pgpool().info, rctx.transaction);
    logger().debug(" {} split collection done", child_pg->get_pgid());
    // Update the child PG's info from the parent PG
    pg->split_into(child_pg->get_pgid().pgid, child_pg, split_bits);

    co_await this->template with_blocking_event<PGMap::PGCreationBlockingEvent>(
	[this, child_pg, next_map] (auto&& trigger) -> seastar::future<> {
	auto fut = shard_services.create_split_pg(
	    std::move(trigger),
            child_pg->get_pgid());
	co_await shard_services.start_operation<PGAdvanceMap>(
            child_pg, shard_services, next_map->get_epoch(),
            std::move(rctx), true).second;
	co_await fut.safe_then([this] (Ref<PG> child_pgref) {
	    logger().debug(" Child PG creation done! {}", child_pgref->get_pgid());
	    split_pgs.insert(child_pgref);
	}).handle_error(crimson::ct_error::ecanceled::handle([](auto) {
	    logger().debug("PG creation canceled");
            return seastar::now();
        }));
	co_return;
    });
  }

  split_stats(split_pgs, children_pgids);
  co_return;
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


}
