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
#include "crimson/osd/osd_operations/pg_merging.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "osd/PeeringState.h"

SET_SUBSYS(osd);

namespace crimson::osd {

PGMerging::PGMerging(
  Ref<PG> pg, ShardServices &shard_services, OSDMapRef new_map, OSDMapRef old_map,
  PeeringCtx &&rctx)
  : pg(pg), shard_services(shard_services), new_map(new_map), old_map(old_map),
    rctx(std::move(rctx))
{}

PGMerging::~PGMerging() {}

void PGMerging::print(std::ostream &lhs) const
{
  lhs << "PGMerging("
      << "pg=" << pg->get_pgid()
      << " to=" << new_map->get_epoch();
  lhs << ")";
}

void PGMerging::dump_detail(Formatter *f) const
{
    f->open_object_section("PGMerging");
    f->dump_stream("pgid") << pg->get_pgid();
    f->dump_int("to", new_map->get_epoch());
    f->close_section();
}

seastar::future<> PGMerging::start()
{
  LOG_PREFIX(PGMerging::start);
  unsigned old_pg_num = old_map->get_pg_num(pg->get_pgid().pool());
  unsigned new_pg_num = new_map->get_pg_num(pg->get_pgid().pool());
  DEBUG(" old_pg_num: {}, new_pg_num: {}", old_pg_num, new_pg_num);
  std::set<spg_t> merge_sources;
  auto pg_id = pg->get_pgid();
  pg->get_pgid().is_split(new_pg_num, old_pg_num, &merge_sources);
  DEBUG(" {} needs {} sources for merge to complete", pg->get_pgid(), merge_sources.size());

  // Get the current merge waiters for this PG.
  // This will contain the merge sources that have already completed their PGAdvanceMap.
  auto &s = shard_services.merge_waiters[new_map->get_epoch()][pg_id];
  std::map<spg_t, Ref<PG>> sources;
  // If all sources have completed PGAdvanceMap we can commence with the merge
  if (s.size() == merge_sources.size()) {
    sources.swap(s);
    shard_services.merge_waiters[new_map->get_epoch()].erase(pg_id);
    if (shard_services.merge_waiters[new_map->get_epoch()].empty()) {
	shard_services.merge_waiters.erase(new_map->get_epoch());
	shard_services.merge_target_pgs.erase(pg_id);
    }
    unsigned split_bits = pg_id.get_split_bits(new_pg_num);
    auto ready_sources = std::move(sources);
    return pg->merge_from(
	ready_sources, rctx, split_bits, 
	new_map->get_pg_pool(pg_id.pool())->last_pg_merge_meta).then(
	 [this] {
	   // After merge_from completes, start a PGAdvanceMap operation
           // to update the PG state based on the merged data.
	   return shard_services.start_operation<PGAdvanceMap>(
	     pg, shard_services, new_map->get_epoch(),
	     std::move(rctx), false, false).second;
	});
  }
  return seastar::now();
}

}
