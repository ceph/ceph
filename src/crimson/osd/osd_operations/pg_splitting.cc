// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/pg_splitting.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGSplitting::PGSplitting(
  Ref<PG> pg, ShardServices &shard_services, OSDMapRef cur_map, epoch_t next_epoch,
  PeeringCtx &&rctx)
  : pg(pg), shard_services(shard_services), cur_map(cur_map), next_epoch(next_epoch),
    rctx(std::move(rctx))
{
  logger().debug("{}: initiated", *this);
}

PGSplitting::~PGSplitting() {}

void PGSplitting::print(std::ostream &lhs) const
{
  lhs << "PGSplitting("
      << "pg=" << pg->get_pgid()
      << " from=" << cur_map->get_epoch()
      << " to=" << next_epoch;
  lhs << ")";
}

void PGSplitting::dump_detail(Formatter *f) const
{
    f->open_object_section("PGSplitting");
    f->dump_stream("pgid") << pg->get_pgid();
    f->dump_int("from", cur_map->get_epoch());
    f->dump_int("to", next_epoch);
    f->close_section();
}


seastar::future<> PGSplitting::start()
{
  logger().debug("{}: start", *this);
  unsigned old_pg_num;
  using cached_map_t = OSDMapService::cached_map_t;

  if (cur_map->have_pg_pool(pg->get_pgid().pool())) {
    old_pg_num = cur_map->get_pg_num(pg->get_pgid().pool());
  } else {
    logger().debug("{} pool doesn't exist in {}", pg->get_pgid(), cur_map);
    return seastar::now();
  }
  
  return shard_services.get_map(next_epoch).then(
    [this, old_pg_num] (cached_map_t&& next_map) {
    unsigned new_pg_num = next_map->get_pg_num(pg->get_pgid().pool());
    if (new_pg_num && new_pg_num > old_pg_num) {
      logger().debug(" New PGs: {} and Old PGs: {}", new_pg_num, old_pg_num);
      logger().debug(" Checking for PG splitting ");
      std::set<spg_t> children;
      if (pg->get_pgid().is_split(old_pg_num, new_pg_num, &children)) {
        logger().debug(" {} PG has split ", pg->get_pgid());
        logger().debug(" {} has {} children ", pg->get_pgid(), children.size());
        std::set<Ref<PG>> split_pgs;
        return seastar::do_with(pg, children,
	                        std::move(next_map),
				new_pg_num,
				split_pgs, [this] (auto &pg,
						   auto &children,
						   auto &next_map,
						   auto &new_pg_num,
						   auto &split_pgs) {
	  return seastar::do_for_each(children, [this, &pg, &next_map, &new_pg_num, &split_pgs] (auto& child_pg) {
	    return shard_services.get_or_create_pg(child_pg).then([child_pg]
		 (auto core) {
		 logger().debug(" PG {} mapped to {}", child_pg.pgid, core);
		 return seastar::now();
	       }).then([this, child_pg, next_map=next_map] {
		 logger().debug(" {} map epoch: {}", child_pg.pgid, next_map->get_epoch());
		 return shard_services.make_pg(std::move(next_map), child_pg, true);
	       }).then([this, &pg, &new_pg_num, &split_pgs] (Ref<PG> child_pg) {
		 logger().debug(" Parent PG: {}", pg->get_pgid());
		 logger().debug(" Child PG ID: {}", child_pg->get_pgid());

		 const coll_t cid{child_pg->get_pgid()};
		 unsigned split_bits = child_pg->get_pgid().get_split_bits(new_pg_num);
		 logger().debug(" pg num is {}, m_seed is {}, split bits is {}", new_pg_num, child_pg->get_pgid().ps(), split_bits);
		 return pg->split_colls(child_pg->get_pgid(), split_bits, child_pg->get_pgid().ps(),
					&child_pg->get_pgpool().info, rctx.transaction).then(
					[this, &pg, child_pg=std::move(child_pg), split_bits, &split_pgs] () {
					logger().debug(" {} split collection done", child_pg->get_pgid());
					pg->split_into(child_pg->get_pgid().pgid, child_pg, split_bits);
					split_pgs.insert(child_pg);
				      });
		 });
	 }).then([this, &children, &next_map, &pg, &split_pgs] {
	   split_stats(split_pgs, children);
	   return seastar::do_for_each(split_pgs, [this, &next_map] (auto& child_pg) {
	       logger().debug(" {} advance map for {}", child_pg->get_pgid(), shard_services.get_map()->get_epoch());
	       return shard_services.start_operation<PGAdvanceMap>(
		 child_pg, shard_services, shard_services.get_map()->get_epoch(), std::move(rctx), true).second.then([this] {
		 return seastar::now();
	       });
	    });
	   });
         });
      }
    } else {
      if (new_pg_num && new_pg_num < old_pg_num) {
	logger().debug("New PGs: {} and Old PGs: {}", new_pg_num, old_pg_num);
	logger().debug("PG Merging is not supported yet");
      }
      logger().debug(" No split in {}", next_epoch);
      return seastar::now();
    }
    return seastar::now();
  });
}

void PGSplitting::split_stats(std::set<Ref<PG>> children_pgs,
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

