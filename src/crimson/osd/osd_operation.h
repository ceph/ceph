// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <array>
#include <set>
#include <vector>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>

#include "include/ceph_assert.h"
#include "crimson/osd/scheduler/scheduler.h"

namespace ceph {
  class Formatter;
}

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

class OperationRegistry;

using registry_hook_t = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

class Operation;
class Blocker;

/**
 * Provides an abstraction for registering and unregistering a blocker
 * for the duration of a future becoming available.
 */
template <typename Fut>
class blocking_future_detail {
  friend class Operation;
  friend class Blocker;
  Blocker *blocker;
  Fut fut;
  blocking_future_detail(Blocker *b, Fut &&f)
    : blocker(b), fut(std::move(f)) {}

  template <typename... V, typename... U>
  friend blocking_future_detail<seastar::future<V...>> make_ready_blocking_future(U&&... args);
  template <typename... V, typename Exception>
  friend blocking_future_detail<seastar::future<V...>>
  make_exception_blocking_future(Exception&& e);

  template <typename U>
  friend blocking_future_detail<seastar::future<>> join_blocking_futures(U &&u);

  template <typename U>
  friend class blocking_future_detail;

public:
  template <typename F>
  auto then(F &&f) && {
    using result = decltype(std::declval<Fut>().then(f));
    return blocking_future_detail<seastar::futurize_t<result>>(
      blocker,
      std::move(fut).then(std::forward<F>(f)));
  }
};

template <typename... T>
using blocking_future = blocking_future_detail<seastar::future<T...>>;

template <typename... V, typename... U>
blocking_future_detail<seastar::future<V...>> make_ready_blocking_future(U&&... args) {
  return blocking_future<V...>(
    nullptr,
    seastar::make_ready_future<V...>(std::forward<U>(args)...));
}

template <typename... V, typename Exception>
blocking_future_detail<seastar::future<V...>>
make_exception_blocking_future(Exception&& e) {
  return blocking_future<V...>(
    nullptr,
    seastar::make_exception_future<V...>(e));
}

/**
 * Provides an interface for dumping diagnostic information about
 * why a particular op is not making progress.
 */
class Blocker {
protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

public:
  template <typename... T>
  blocking_future<T...> make_blocking_future(seastar::future<T...> &&f) {
    return blocking_future<T...>(this, std::move(f));
  }

  void dump(ceph::Formatter *f) const;

  virtual const char *get_type_name() const = 0;

  virtual ~Blocker() = default;
};

template <typename T>
class BlockerT : public Blocker {
public:
  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~BlockerT() = default;
};

class AggregateBlocker : public BlockerT<AggregateBlocker> {
  vector<Blocker*> parent_blockers;
protected:
  void dump_detail(ceph::Formatter *f) const final;
public:
  AggregateBlocker(vector<Blocker*> &&parent_blockers)
    : parent_blockers(std::move(parent_blockers)) {}
  static constexpr const char *type_name = "AggregateBlocker";
};

template <typename T>
blocking_future<> join_blocking_futures(T &&t) {
  vector<Blocker*> blockers;
  blockers.reserve(t.size());
  for (auto &&bf: t) {
    blockers.push_back(bf.blocker);
    bf.blocker = nullptr;
  }
  auto agg = std::make_unique<AggregateBlocker>(std::move(blockers));
  return agg->make_blocking_future(
    seastar::parallel_for_each(
      std::forward<T>(t),
      [](auto &&bf) {
	return std::move(bf.fut);
      }).then([agg=std::move(agg)] {
	return seastar::now();
      }));
}


/**
 * Common base for all crimson-osd operations.  Mainly provides
 * an interface for registering ops in flight and dumping
 * diagnostic information.
 */
class Operation : public boost::intrusive_ref_counter<
  Operation, boost::thread_unsafe_counter> {
 public:
  uint64_t get_id() const {
    return id;
  }

  virtual OperationTypeCode get_type() const = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(std::ostream &) const = 0;

  template <typename... T>
  seastar::future<T...> with_blocking_future(blocking_future<T...> &&f) {
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

  void dump(ceph::Formatter *f);
  void dump_brief(ceph::Formatter *f);
  virtual ~Operation() = default;

 protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

 private:
  registry_hook_t registry_hook;

  std::vector<Blocker*> blockers;
  uint64_t id = 0;
  void set_id(uint64_t in_id) {
    id = in_id;
  }

  void add_blocker(Blocker *b) {
    blockers.push_back(b);
  }

  void clear_blocker(Blocker *b) {
    auto iter = std::find(blockers.begin(), blockers.end(), b);
    if (iter != blockers.end()) {
      blockers.erase(iter);
    }
  }

  friend class OperationRegistry;
};
using OperationRef = boost::intrusive_ptr<Operation>;

std::ostream &operator<<(std::ostream &, const Operation &op);

template <typename T>
class OperationT : public Operation {

protected:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

public:
  static constexpr const char *type_name = OP_NAMES[static_cast<int>(T::type)];
  using IRef = boost::intrusive_ptr<T>;

  OperationTypeCode get_type() const final {
    return T::type;
  }

  const char *get_type_name() const final {
    return T::type_name;
  }

  virtual ~OperationT() = default;
};

/**
 * Maintains a set of lists of all active ops.
 */
class OperationRegistry {
  friend class Operation;
  using op_list_member_option = boost::intrusive::member_hook<
    Operation,
    registry_hook_t,
    &Operation::registry_hook
    >;
  using op_list = boost::intrusive::list<
    Operation,
    op_list_member_option,
    boost::intrusive::constant_time_size<false>>;

  std::array<
    op_list,
    static_cast<int>(OperationTypeCode::last_op)
  > registries;

  std::array<
    uint64_t,
    static_cast<int>(OperationTypeCode::last_op)
  > op_id_counters = {};

  seastar::timer<seastar::lowres_clock> shutdown_timer;
  seastar::promise<> shutdown;
public:
  template <typename T, typename... Args>
  typename T::IRef create_operation(Args&&... args) {
    typename T::IRef op = new T(std::forward<Args>(args)...);
    registries[static_cast<int>(T::type)].push_back(*op);
    op->set_id(op_id_counters[static_cast<int>(T::type)]++);
    return op;
  }

  seastar::future<> stop() {
    shutdown_timer.set_callback([this] {
	if (std::all_of(registries.begin(),
			registries.end(),
			[](auto& opl) {
			  return opl.empty();
			})) {
	  shutdown.set_value();
	  shutdown_timer.cancel();
	}
      });
    shutdown_timer.arm_periodic(std::chrono::milliseconds(100/*TODO: use option instead*/));
    return shutdown.get_future();
  }
};

/**
 * Throttles set of currently running operations
 *
 * Very primitive currently, assumes all ops are equally
 * expensive and simply limits the number that can be
 * concurrently active.
 */
class OperationThrottler : public Blocker, md_config_obs_t {
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
	return seastar::now();
    });
  }
protected:
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

/**
 * Ensures that at most one op may consider itself in the phase at a time.
 * Ops will see enter() unblock in the order in which they tried to enter
 * the phase.  entering (though not necessarily waiting for the future to
 * resolve) a new phase prior to exiting the previous one will ensure that
 * the op ordering is preserved.
 */
class OrderedPipelinePhase : public Blocker {
  const char * name;

protected:
  virtual void dump_detail(ceph::Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

public:
  seastar::shared_mutex mutex;

  /**
   * Used to encapsulate pipeline residency state.
   */
  class Handle {
    OrderedPipelinePhase *phase = nullptr;

  public:
    Handle() = default;

    Handle(const Handle&) = delete;
    Handle(Handle&&) = delete;
    Handle &operator=(const Handle&) = delete;
    Handle &operator=(Handle&&) = delete;

    /**
     * Returns a future which unblocks when the handle has entered the passed
     * OrderedPipelinePhase.  If already in a phase, enter will also release
     * that phase after placing itself in the queue for the next one to preserve
     * ordering.
     */
    blocking_future<> enter(OrderedPipelinePhase &phase);

    /**
     * Releases the current phase if there is one.  Called in ~Handle().
     */
    void exit();

    ~Handle();
  };

  OrderedPipelinePhase(const char *name) : name(name) {}
};

}
