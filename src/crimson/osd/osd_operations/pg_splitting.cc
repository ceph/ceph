// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operations/pg_splitting.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "osd/PeeringState.h"

SET_SUBSYS(osd);

namespace crimson::osd {

PGSplitting::PGSplitting(
  Ref<PG> pg, ShardServices &shard_services, OSDMapRef new_map, std::set<std::pair<spg_t, epoch_t>> children,
  PeeringCtx &&rctx)
  : pg(pg), shard_services(shard_services), new_map(new_map), children(children),
    rctx(std::move(rctx))
{}

PGSplitting::~PGSplitting() {}

void PGSplitting::print(std::ostream &lhs) const
{
  lhs << "PGSplitting("
      << "pg=" << pg->get_pgid()
      << " to=" << new_map->get_epoch();
  lhs << ")";
}

void PGSplitting::dump_detail(Formatter *f) const
{
    f->open_object_section("PGSplitting");
    f->dump_stream("pgid") << pg->get_pgid();
    f->dump_int("to", new_map->get_epoch());
    f->close_section();
}


seastar::future<> PGSplitting::start()
{
  LOG_PREFIX(PGSplitting::start);
  DEBUG("start");
  return seastar::do_for_each(children, [this, FNAME] (auto& child_pg_info) {
    auto child_pg = child_pg_info.first;
    auto pg_epoch = child_pg_info.second;
    children_pgids.insert(child_pg);
    // Map each child pg ID to a core
    return shard_services.get_or_create_pg(child_pg).then([FNAME, child_pg]
      (auto core) {
      DEBUG(" PG {} mapped to {}", child_pg.pgid, core);
      return seastar::now();
    }).then([this, FNAME, child_pg, pg_epoch] {
      DEBUG(" {} map epoch: {}", child_pg.pgid, pg_epoch);
      return shard_services.get_map(pg_epoch).then(
	[this, child_pg] (auto&& map) {
	  return shard_services.make_pg(std::move(map), child_pg, true);
        });
    }).then([this, FNAME] (Ref<PG> child_pg) {
      DEBUG(" Parent PG: {}", pg->get_pgid());
      DEBUG(" Child PG ID: {}", child_pg->get_pgid());

      unsigned new_pg_num = new_map->get_pg_num(pg->get_pgid().pool());
      const coll_t cid{child_pg->get_pgid()};
      // Depending on the new_pg_num the parent PG's collection is split.
      // The child PG will be initiated with this split collection.
      unsigned split_bits = child_pg->get_pgid().get_split_bits(new_pg_num);
      DEBUG(" pg num is {}, m_seed is {}, split bits is {}", new_pg_num, child_pg->get_pgid().ps(), split_bits);
      return pg->split_colls(child_pg->get_pgid(), split_bits, child_pg->get_pgid().ps(),
        &child_pg->get_pgpool().info, rctx.transaction).then(
	[this, FNAME, child_pg=std::move(child_pg), split_bits] () {
          DEBUG(" {} split collection done", child_pg->get_pgid());
	  // Update the child PG's info from the parent PG
	  pg->split_into(child_pg->get_pgid().pgid, child_pg, split_bits);
	  split_pgs.insert(child_pg);
	});
      });
  }).then([this, FNAME] {
    // Split the parent PG's stats into the children PGs.
    split_stats(split_pgs, children_pgids);
    return seastar::do_for_each(split_pgs, [this, FNAME] (auto& child_pg) {
      // For each child PG initiate PGAdvanceMap operation in order to start the PGs.
      DEBUG(" {} advance map from {} to {}", child_pg->get_pgid(), child_pg->get_osdmap_epoch(),
	  shard_services.get_map()->get_epoch());
      return shard_services.start_operation<PGAdvanceMap>(
        child_pg, shard_services, shard_services.get_map()->get_epoch(),
	std::move(rctx), true).second.then([] {
	  return seastar::now();
	});
    });
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

