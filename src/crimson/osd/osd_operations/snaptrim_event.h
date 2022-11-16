// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "osd/osd_types.h"
#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class OSD;
class ShardServices;
class PG;

class SnapTrimEvent final : public PhasedOperationT<SnapTrimEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::snaptrim_event;

  SnapTrimEvent(Ref<PG> pg, snapid_t snapid)
    : pg(std::move(pg)),
      snapid(snapid) {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

private:
  CommonPGPipeline& pp();

  PipelineHandle handle;
  Ref<PG> pg;
  const snapid_t snapid;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    CommonPGPipeline::WaitForActive::BlockingEvent,
    PGActivationBlocker::BlockingEvent,
    CommonPGPipeline::RecoverMissing::BlockingEvent,
    CommonPGPipeline::GetOBC::BlockingEvent,
    CommonPGPipeline::Process::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

} // namespace crimson::osd

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::SnapTrimEvent> : fmt::ostream_formatter {};
#endif
