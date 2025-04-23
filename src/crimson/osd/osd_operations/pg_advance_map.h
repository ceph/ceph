// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg_map.h"
#include "crimson/osd/osdmap_service.h"
#include "osd/osd_types.h"
#include "crimson/common/type_helpers.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;
class PG;

class PGAdvanceMap : public PhasedOperationT<PGAdvanceMap> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::pg_advance_map;

protected:
  Ref<PG> pg;
  ShardServices &shard_services;
  PipelineHandle handle;

  std::optional<epoch_t> from;
  epoch_t to;

  PeeringCtx rctx;
  const bool do_init;

  // For splitting
  std::set<spg_t> children_pgids;
  std::set<Ref<PG>> split_pgs;

public:
  PGAdvanceMap(
    Ref<PG> pg, ShardServices &shard_services, epoch_t to,
    PeeringCtx &&rctx, bool do_init);
  ~PGAdvanceMap();

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter *f) const final;
  seastar::future<> start();
  PipelineHandle &get_handle() { return handle; }

  using cached_map_t = OSDMapService::cached_map_t;
  seastar::future<> check_for_splits(epoch_t old_epoch,
                                     cached_map_t next_map);
  seastar::future<> split_pg(std::set<spg_t> split_children,
                             cached_map_t next_map);
  void split_stats(std::set<Ref<PG>> child_pgs,
		   const std::set<spg_t> &child_pgids);

  std::tuple<
    PGPeeringPipeline::Process::BlockingEvent,
    PGMap::PGCreationBlockingEvent
  > tracking_events;

  epoch_t get_epoch_sent_at() const {
    return to;
  }

private:
  PGPeeringPipeline &peering_pp(PG &pg);
  seastar::future<> handle_split_pg_creation(
    Ref<PG> child_pg,
    cached_map_t next_map);
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PGAdvanceMap> : fmt::ostream_formatter {};
#endif
