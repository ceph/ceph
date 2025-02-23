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
/// unlike @c PglogBasedRecovery, @c UrgentRecovery is not throttled
/// by the scheduler. and it utilizes @c RecoveryBackend directly to
/// recover the unreadable object.
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

  void cancel() {
    cancelled = true;
  }

  bool is_cancelled() const {
    return cancelled;
  }

  epoch_t get_epoch_started() const {
    return epoch_started;
  }
private:
  interruptible_future<bool> do_recovery() override;
  bool cancelled = false;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::PglogBasedRecovery> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::osd::UrgentRecovery> : fmt::ostream_formatter {};
template <class T> struct fmt::formatter<crimson::osd::BackgroundRecoveryT<T>> : fmt::ostream_formatter {};
#endif
