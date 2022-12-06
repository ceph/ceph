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
class SnapTrimEvent final : public PhasedOperationT<SnapTrimEvent> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::snaptrim_event;

  SnapTrimEvent(Ref<PG> pg, SnapMapper& snap_mapper, snapid_t snapid)
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

  // we don't need to synchronize with other instances of SnapTrimEvent;
  // it's here for the sake of op tracking.
  struct WaitSubop : OrderedConcurrentPhaseT<WaitSubop> {
    static constexpr auto type_name = "SnapTrimEvent::wait_subop";
  } wait_subop;

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
    WaitSubop::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

// remove single object. a SnapTrimEvent can create multiple subrequests.
// the division of labour is needed because of the restriction that an Op
// cannot revisite a pipeline's stage it already saw.
class SnapTrimObjSubEvent : public PhasedOperationT<SnapTrimObjSubEvent> {
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::snaptrimobj_subevent;

  SnapTrimObjSubEvent(
    Ref<PG> pg,
    const hobject_t& coid,
    snapid_t snap_to_trim)
  : pg(std::move(pg)),
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

  using remove_or_update_ret_t =
    std::pair<ceph::os::Transaction, std::vector<pg_log_entry_t>>;
  tl::expected<remove_or_update_ret_t, int>
  remove_or_update(ObjectContextRef obc, ObjectContextRef head_obc);

  // we don't need to synchronize with other instances started by
  // SnapTrimEvent; it's here for the sake of op tracking.
  struct WaitRepop : OrderedConcurrentPhaseT<WaitRepop> {
    static constexpr auto type_name = "SnapTrimObjSubEvent::wait_repop";
  } wait_repop;

  Ref<PG> pg;
  PipelineHandle handle;
  osd_op_params_t osd_op_p;
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

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::SnapTrimEvent> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::SnapTrimObjSubEvent> : fmt::ostream_formatter {};
#endif
