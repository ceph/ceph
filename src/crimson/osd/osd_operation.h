// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/osd/osd_operation_sequencer.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/scheduler/scheduler.h"

namespace crimson::osd {

enum class OperationTypeCode {
  client_request = 0,
  peering_event,
  compound_peering_request,
  pg_advance_map,
  pg_creation,
  replicated_request,
  background_recovery,
  background_recovery_sub,
  last_op
};

static constexpr const char* const OP_NAMES[] = {
  "client_request",
  "peering_event",
  "compound_peering_request",
  "pg_advance_map",
  "pg_creation",
  "replicated_request",
  "background_recovery",
  "background_recovery_sub",
};

// prevent the addition of OperationTypeCode-s with no matching OP_NAMES entry:
static_assert(
  (sizeof(OP_NAMES)/sizeof(OP_NAMES[0])) ==
  static_cast<int>(OperationTypeCode::last_op));

template <typename T>
class OperationT : public Operation {
public:
  template <typename ValuesT = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, ValuesT>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;
  static constexpr const char *type_name = OP_NAMES[static_cast<int>(T::type)];
  using IRef = boost::intrusive_ptr<T>;

  unsigned get_type() const final {
    return static_cast<unsigned>(T::type);
  }

  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~OperationT() = default;

private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;
};

/**
 * Maintains a set of lists of all active ops.
 */
using OSDOperationRegistry = OperationRegistryT<
  static_cast<size_t>(OperationTypeCode::last_op)
  >;

/**
 * Throttles set of currently running operations
 *
 * Very primitive currently, assumes all ops are equally
 * expensive and simply limits the number that can be
 * concurrently active.
 */
class OperationThrottler : public Blocker,
			private md_config_obs_t {
public:
  OperationThrottler(ConfigProxy &conf);

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
  void update_from_config(const ConfigProxy &conf);

  template <typename F>
  auto with_throttle(
    OperationRef op,
    crimson::osd::scheduler::params_t params,
    F &&f) {
    if (!max_in_progress) return f();
    auto fut = acquire_throttle(params);
    return op->with_blocking_future(std::move(fut))
      .then(std::forward<F>(f))
      .then([this](auto x) {
	release_throttle();
	return x;
      });
  }

  template <typename F>
  seastar::future<> with_throttle_while(
    OperationRef op,
    crimson::osd::scheduler::params_t params,
    F &&f) {
    return with_throttle(op, params, f).then([this, params, op, f](bool cont) {
      if (cont)
	return with_throttle_while(op, params, f);
      else
	return seastar::make_ready_future<>();
    });
  }

private:
  void dump_detail(Formatter *f) const final;
  const char *get_type_name() const final {
    return "OperationThrottler";
  }

private:
  crimson::osd::scheduler::SchedulerRef scheduler;

  uint64_t max_in_progress = 0;
  uint64_t in_progress = 0;

  uint64_t pending = 0;

  void wake();

  blocking_future<> acquire_throttle(
    crimson::osd::scheduler::params_t params);

  void release_throttle();
};

}
