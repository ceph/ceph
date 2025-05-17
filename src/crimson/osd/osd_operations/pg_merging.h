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

class PGMerging : public PhasedOperationT<PGMerging> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::pg_merging;

protected:
  Ref<PG> pg;
  ShardServices &shard_services;
  PipelineHandle handle;

  OSDMapRef new_map;
  OSDMapRef old_map;
  PeeringCtx rctx;

public:
  PGMerging(
    Ref<PG> pg, ShardServices &shard_services, OSDMapRef new_map,
    OSDMapRef old_map,
    PeeringCtx &&rctx);
  ~PGMerging();

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter *f) const final;
  seastar::future<> start();
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PGMerging> : fmt::ostream_formatter {};
#endif

