// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/peering_event.h"
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

public:
  PGAdvanceMap(
    Ref<PG> pg, ShardServices &shard_services, epoch_t to,
    PeeringCtx &&rctx, bool do_init);
  ~PGAdvanceMap();

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter *f) const final;
  seastar::future<> start();
  PipelineHandle &get_handle() { return handle; }

  std::tuple<
    PGPeeringPipeline::Process::BlockingEvent
  > tracking_events;

private:
  PGPeeringPipeline &peering_pp(PG &pg);
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PGAdvanceMap> : fmt::ostream_formatter {};
#endif
