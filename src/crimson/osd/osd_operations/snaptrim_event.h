// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <seastar/core/future.hh>

#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/subop_blocker.h"
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

// trim up to `max` objects for snapshot `snapid
class SnapTrimEvent final : public PhasedOperationT<SnapTrimEvent> {
public:
  using remove_or_update_ertr =
    crimson::errorator<crimson::ct_error::enoent>;
  using remove_or_update_iertr =
    crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, remove_or_update_ertr>;
  using snap_trim_iertr = remove_or_update_iertr;
  using snap_trim_event_ret_t =
    snap_trim_iertr::future<seastar::stop_iteration>;
  using snap_trim_obj_subevent_ret_t =
      remove_or_update_iertr::future<>;

  static constexpr OperationTypeCode type = OperationTypeCode::snaptrim_event;

  SnapTrimEvent(Ref<PG> pg,
                SnapMapper& snap_mapper,
                const snapid_t snapid,
                const bool needs_pause)
    : pg(std::move(pg)),
      snap_mapper(snap_mapper),
      snapid(snapid),
      needs_pause(needs_pause) {}

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  snap_trim_event_ret_t start();

private:
  CommonPGPipeline& client_pp();

  SubOpBlocker<snap_trim_obj_subevent_ret_t> subop_blocker;

  Ref<PG> pg;
  PipelineHandle handle;
  SnapMapper& snap_mapper;
  const snapid_t snapid;
  const bool needs_pause;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    PG::BackgroundProcessLock::Wait::BlockingEvent,
    CompletionEvent
  > tracking_events;

  friend class PG::BackgroundProcessLock;
};

// remove single object. a SnapTrimEvent can create multiple subrequests.
// the division of labour is needed because of the restriction that an Op
// cannot revisite a pipeline's stage it already saw.
class SnapTrimObjSubEvent : public PhasedOperationT<SnapTrimObjSubEvent> {
public:
  using remove_or_update_ertr =
    crimson::errorator<crimson::ct_error::enoent>;
  using remove_or_update_iertr =
    crimson::interruptible::interruptible_errorator<
      IOInterruptCondition, remove_or_update_ertr>;
  using snap_trim_obj_subevent_ret_t =
      remove_or_update_iertr::future<>;

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
  snap_trim_obj_subevent_ret_t start();

  CommonPGPipeline& client_pp();

private:
  object_stat_sum_t delta_stats;

  ObjectContextLoader::load_obc_iertr::future<> process_and_submit(
    ObjectContextRef head_obc,
    ObjectContextRef clone_obc);

  snap_trim_obj_subevent_ret_t remove_clone(
    ObjectContextRef obc,
    ObjectContextRef head_obc,
    ceph::os::Transaction& txn);
  void remove_head_whiteout(
    ObjectContextRef obc,
    ObjectContextRef head_obc,
    ceph::os::Transaction& txn);
  interruptible_future<> adjust_snaps(
    ObjectContextRef obc,
    ObjectContextRef head_obc,
    const std::set<snapid_t>& new_snaps,
    ceph::os::Transaction& txn);
  void update_head(
    ObjectContextRef obc,
    ObjectContextRef head_obc,
    ceph::os::Transaction& txn);

  remove_or_update_iertr::future<ceph::os::Transaction>
  remove_or_update(ObjectContextRef obc, ObjectContextRef head_obc);

  pg_log_entry_t& add_log_entry(
    int _op,
    const hobject_t& _soid,
    const eversion_t& pv,
    version_t uv,
    const osd_reqid_t& rid,
    const utime_t& mt,
    int return_code) {
    log_entries.emplace_back(
      _op,
      _soid,
      osd_op_p.at_version,
      pv,
      uv,
      rid,
      mt,
      return_code);
    return log_entries.back();
  }

  Ref<PG> pg;
  PipelineHandle handle;
  osd_op_params_t osd_op_p;
  const hobject_t coid;
  const snapid_t snap_to_trim;
  std::vector<pg_log_entry_t> log_entries;

public:
  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    StartEvent,
    CommonPGPipeline::CheckAlreadyCompleteGetObc::BlockingEvent,
    CommonPGPipeline::Process::BlockingEvent,
    CommonPGPipeline::WaitRepop::BlockingEvent,
    CompletionEvent
  > tracking_events;
};

} // namespace crimson::osd

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::SnapTrimEvent> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::SnapTrimObjSubEvent> : fmt::ostream_formatter {};
#endif
