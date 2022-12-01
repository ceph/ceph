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

class SnapMapper;

namespace crimson::osd {

class OSD;
class ShardServices;
class PG;

// trim up to `max` objects for snapshot `snapid
class SnapTrimRequest final : public PhasedOperationT<SnapTrimRequest> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::snaptrim_request;

  SnapTrimRequest(Ref<PG> pg, SnapMapper& snap_mapper, snapid_t snapid)
    : pg(std::move(pg)),
      snap_mapper(snap_mapper),
      snapid(snapid) {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<seastar::stop_iteration> start();
  seastar::future<seastar::stop_iteration> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

private:
  CommonPGPipeline& pp();

  // bases on 998cb8c141bb89aafae298a9d5e130fbd78fe5f2
  struct SubOpBlocker : crimson::BlockerT<SubOpBlocker> {
    static constexpr const char* type_name = "CompoundOpBlocker";

    using id_done_t = std::pair<crimson::Operation::id_t, seastar::future<>>;

    void dump_detail(Formatter *f) const final;

    template <class... Args>
    void emplace_back(Args&&... args);

    seastar::future<> wait_completion();
  private:
    std::vector<id_done_t> subops;
  } subop_blocker;
  PipelineHandle handle;
  Ref<PG> pg;
  SnapMapper& snap_mapper;
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

// remove single object. a SnapTrimRequest can create multiple subrequests.
// the division of labour is needed because of the restriction that an Op
// cannot revisite a pipeline's stage it already saw.
class SnapTrimObjSubRequest : public PhasedOperationT<SnapTrimObjSubRequest> {
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::snaptrimobj_subrequest;

  SnapTrimObjSubRequest(const hobject_t& coid, snapid_t snap_to_trim) :
    coid(coid),
    snap_to_trim(snap_to_trim) {
  }

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  CommonPGPipeline& pp();

private:
  object_stat_sum_t delta_stats;

  std::pair<ceph::os::Transaction,
            std::vector<pg_log_entry_t>>
  remove_or_update(ObjectContextRef obc, ObjectContextRef head_obc);

  // we don't need to synchronize with other instances started by
  // SnapTrimRequest; it's here for the sake of op tracking.
  struct WaitRepop : OrderedConcurrentPhaseT<WaitRepop> {
    static constexpr auto type_name = "SnapTrimObjSubRequest::wait_repop";
  } wait_repop;

  Ref<PG> pg;
  PipelineHandle handle;
  const hobject_t coid;
  const snapid_t snap_to_trim;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    CommonPGPipeline::WaitForActive::BlockingEvent,
    PGActivationBlocker::BlockingEvent,
    CommonPGPipeline::RecoverMissing::BlockingEvent,
    CommonPGPipeline::GetOBC::BlockingEvent,
    CommonPGPipeline::Process::BlockingEvent,
    WaitRepop::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

} // namespace crimson::osd
