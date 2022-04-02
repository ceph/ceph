// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/operation.h"
#include "crimson/osd/pg_interval_interrupt_condition.h"
#include "crimson/osd/scheduler/scheduler.h"
#include "osd/osd_types.h"

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
  internal_client_request,
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
  "internal_client_request",
};

// prevent the addition of OperationTypeCode-s with no matching OP_NAMES entry:
static_assert(
  (sizeof(OP_NAMES)/sizeof(OP_NAMES[0])) ==
  static_cast<int>(OperationTypeCode::last_op));

struct InterruptibleOperation : Operation {
  template <typename ValuesT = void>
  using interruptible_future =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, ValuesT>;
  using interruptor =
    ::crimson::interruptible::interruptor<
      ::crimson::osd::IOInterruptCondition>;
};

template <typename T>
class OperationT : public InterruptibleOperation {
  std::vector<Blocker*> blockers;

  void add_blocker(Blocker *b) {
    blockers.push_back(b);
  }

  void clear_blocker(Blocker *b) {
    auto iter = std::find(blockers.begin(), blockers.end(), b);
    if (iter != blockers.end()) {
      blockers.erase(iter);
    }
  }

public:
  template <typename U>
  seastar::future<U> with_blocking_future(blocking_future<U> &&f) {
    if (f.fut.available()) {
      return std::move(f.fut);
    }
    assert(f.blocker);
    add_blocker(f.blocker);
    return std::move(f.fut).then_wrapped([this, blocker=f.blocker](auto &&arg) {
      clear_blocker(blocker);
      return std::move(arg);
    });
  }

  template <typename InterruptCond, typename U>
  ::crimson::interruptible::interruptible_future<InterruptCond, U>
  with_blocking_future_interruptible(blocking_future<U> &&f) {
    if (f.fut.available()) {
      return std::move(f.fut);
    }
    assert(f.blocker);
    add_blocker(f.blocker);
    auto fut = std::move(f.fut).then_wrapped([this, blocker=f.blocker](auto &&arg) {
      clear_blocker(blocker);
      return std::move(arg);
    });
    return ::crimson::interruptible::interruptible_future<
      InterruptCond, U>(std::move(fut));
  }

  template <typename InterruptCond, typename U>
  ::crimson::interruptible::interruptible_future<InterruptCond, U>
  with_blocking_future_interruptible(
    blocking_interruptible_future<InterruptCond, U> &&f) {
    if (f.fut.available()) {
      return std::move(f.fut);
    }
    assert(f.blocker);
    add_blocker(f.blocker);
    return std::move(f.fut).template then_wrapped_interruptible(
      [this, blocker=f.blocker](auto &&arg) {
      clear_blocker(blocker);
      return std::move(arg);
    });
  }

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

template <class T>
class TrackableOperationT : public OperationT<T> {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  template<class EventT>
  decltype(auto) get_event() {
    // all out derivates are supposed to define the list of tracking
    // events accessible via `std::get`. This will usually boil down
    // into an instance of `std::tuple`.
    return std::get<EventT>(that()->tracking_events);
  }

protected:
  using OperationT<T>::OperationT;

  template <class EventT, class... Args>
  void track_event(Args&&... args) {
    // the idea is to have a visitor-like interface that allows to double
    // dispatch (backend, blocker type)
    get_event<EventT>().trigger(*that(), std::forward<Args>(args)...);
  }
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
class OperationThrottler : public BlockerT<OperationThrottler>,
			private md_config_obs_t {
  friend BlockerT<OperationThrottler>;
  static constexpr const char* type_name = "OperationThrottler";

  template <typename OperationT, typename F>
  auto with_throttle(
    OperationT* op,
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

public:
  OperationThrottler(ConfigProxy &conf);

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
  void update_from_config(const ConfigProxy &conf);

  template <typename OperationT, typename F>
  seastar::future<> with_throttle_while(
    OperationT* op,
    crimson::osd::scheduler::params_t params,
    F &&f) {
    return with_throttle(op, params, f).then([this, params, op, f](bool cont) {
      return cont ? with_throttle_while(op, params, f) : seastar::now();
    });
  }

private:
  void dump_detail(Formatter *f) const final;

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
