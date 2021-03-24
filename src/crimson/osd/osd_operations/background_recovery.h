// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/event_base.hpp>

#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"

#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class ShardServices;

class BackgroundRecovery : public OperationT<BackgroundRecovery> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::background_recovery;

  BackgroundRecovery(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started,
    crimson::osd::scheduler::scheduler_class_t scheduler_class);

  virtual void print(std::ostream &) const;
  seastar::future<> start();

protected:
  Ref<PG> pg;
  const epoch_t epoch_started;

private:
  virtual void dump_detail(Formatter *f) const;
  crimson::osd::scheduler::params_t get_scheduler_params() const {
    return {
      1, // cost
      0, // owner
      scheduler_class
    };
  }
  virtual interruptible_future<bool> do_recovery() = 0;
  ShardServices &ss;
  const crimson::osd::scheduler::scheduler_class_t scheduler_class;
};

/// represent a recovery initiated for serving a client request
///
/// unlike @c PglogBasedRecovery and @c BackfillRecovery,
/// @c UrgentRecovery is not throttled by the scheduler. and it
/// utilizes @c RecoveryBackend directly to recover the unreadable
/// object.
class UrgentRecovery final : public BackgroundRecovery {
public:
  UrgentRecovery(
    const hobject_t& soid,
    const eversion_t& need,
    Ref<PG> pg,
    ShardServices& ss,
    epoch_t epoch_started)
  : BackgroundRecovery{pg, ss, epoch_started,
                       crimson::osd::scheduler::scheduler_class_t::immediate},
    soid{soid}, need(need) {}
  void print(std::ostream&) const final;

private:
  void dump_detail(Formatter* f) const final;
  interruptible_future<bool> do_recovery() override;
  const hobject_t soid;
  const eversion_t need;
};

class PglogBasedRecovery final : public BackgroundRecovery {
public:
  PglogBasedRecovery(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started);

private:
  interruptible_future<bool> do_recovery() override;
};

class BackfillRecovery final : public BackgroundRecovery {
public:
  class BackfillRecoveryPipeline {
    OrderedExclusivePhase process = {
      "BackfillRecovery::PGPipeline::process"
    };
    friend class BackfillRecovery;
    friend class PeeringEvent;
  };

  template <class EventT>
  BackfillRecovery(
    Ref<PG> pg,
    ShardServices &ss,
    epoch_t epoch_started,
    const EventT& evt);

  static BackfillRecoveryPipeline &bp(PG &pg);

private:
  boost::intrusive_ptr<const boost::statechart::event_base> evt;
  PipelineHandle handle;
  interruptible_future<bool> do_recovery() override;
};

template <class EventT>
BackfillRecovery::BackfillRecovery(
  Ref<PG> pg,
  ShardServices &ss,
  const epoch_t epoch_started,
  const EventT& evt)
  : BackgroundRecovery(
      std::move(pg),
      ss,
      epoch_started,
      crimson::osd::scheduler::scheduler_class_t::background_best_effort),
    evt(evt.intrusive_from_this())
{}


}
