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

class PGSplitting : public PhasedOperationT<PGSplitting> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::pg_splitting;

protected:
  Ref<PG> pg;
  ShardServices &shard_services;
  PipelineHandle handle;

  OSDMapRef new_map;
  std::set<std::pair<spg_t, epoch_t>> children;
  std::set<spg_t> children_pgids;
  PeeringCtx rctx;
  std::set<Ref<PG>> split_pgs;

public:
  PGSplitting(
    Ref<PG> pg, ShardServices &shard_services, OSDMapRef new_map, std::set<std::pair<spg_t, epoch_t>> children,
    PeeringCtx &&rctx);
  ~PGSplitting();

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter *f) const final;
  seastar::future<> start();
  void split_stats(std::set<Ref<PG>> child_pgs,
		   const std::set<spg_t> &child_pgids);
  //PipelineHandle &get_handle() { return handle; }
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PGSplitting> : fmt::ostream_formatter {};
#endif
