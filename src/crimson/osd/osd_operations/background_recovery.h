// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/event_base.hpp>

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg.h"

namespace crimson::osd {
class PG;
class ShardServices;

template <class T>
class BackgroundRecoveryT : public PhasedOperationT<T> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::background_recovery;

  BackgroundRecoveryT(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started,
    crimson::osd::scheduler::scheduler_class_t scheduler_class, float delay = 0);

  virtual void print(std::ostream &) const;
  seastar::future<> start();

protected:
  Ref<PG> pg;
  const epoch_t epoch_started;
  float delay = 0;

private:
  virtual void dump_detail(Formatter *f) const;
  crimson::osd::scheduler::params_t get_scheduler_params() const {
    return {
      1, // cost
      0, // owner
      scheduler_class
    };
  }
  using do_recovery_ret_t = typename PhasedOperationT<T>::template interruptible_future<bool>;
  virtual do_recovery_ret_t do_recovery() = 0;
  ShardServices &ss;
  const crimson::osd::scheduler::scheduler_class_t scheduler_class;
};

/// represent a recovery initiated for serving a client request
///
/// unlike @c PglogBasedRecovery and @c BackfillRecovery,
/// @c UrgentRecovery is not throttled by the scheduler. and it
/// utilizes @c RecoveryBackend directly to recover the unreadable
/// object.
class UrgentRecovery final : public BackgroundRecoveryT<UrgentRecovery> {
public:
  UrgentRecovery(
    const hobject_t& soid,
    const eversion_t& need,
    Ref<PG> pg,
    ShardServices& ss,
    epoch_t epoch_started);
  void print(std::ostream&) const final;

  std::tuple<
    OperationThrottler::BlockingEvent,
    RecoveryBackend::RecoveryBlockingEvent
  > tracking_events;

private:
  void dump_detail(Formatter* f) const final;
  interruptible_future<bool> do_recovery() override;
  const hobject_t soid;
  const eversion_t need;
};

class PglogBasedRecovery final : public BackgroundRecoveryT<PglogBasedRecovery> {
public:
  PglogBasedRecovery(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started,
    float delay = 0);

  std::tuple<
    OperationThrottler::BlockingEvent,
    RecoveryBackend::RecoveryBlockingEvent
  > tracking_events;

private:
  interruptible_future<bool> do_recovery() override;
};

class BackfillRecovery final : public BackgroundRecoveryT<BackfillRecovery> {
public:

  template <class EventT>
  BackfillRecovery(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started,
    const EventT& evt);

  PipelineHandle& get_handle() { return handle; }

  std::tuple<
    OperationThrottler::BlockingEvent,
    PGPeeringPipeline::Process::BlockingEvent
  > tracking_events;

private:
  boost::intrusive_ptr<const boost::statechart::event_base> evt;
  PipelineHandle handle;

  static PGPeeringPipeline &peering_pp(PG &pg);
  interruptible_future<bool> do_recovery() override;
};

template <class EventT>
BackfillRecovery::BackfillRecovery(
  Ref<PG> pg,
  ShardServices &ss,
  const epoch_t epoch_started,
  const EventT& evt)
  : BackgroundRecoveryT(
      std::move(pg),
      ss,
      epoch_started,
      crimson::osd::scheduler::scheduler_class_t::background_best_effort),
    evt(evt.intrusive_from_this())
{}

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::BackfillRecovery> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::PglogBasedRecovery> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::UrgentRecovery> : fmt::ostream_formatter {};
template <class T> struct fmt::formatter<crimson::osd::BackgroundRecoveryT<T>> : fmt::ostream_formatter {};
#endif
