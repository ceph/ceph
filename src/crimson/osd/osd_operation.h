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
#include "include/utime.h"
#include "common/Clock.h"
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
  historic_client_request,
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
  "historic_client_request",
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
// TODO: make blocking_future a thin wrapper over seastar::future but
// ONLY in the debug builds; for production this be nothing more than
// alias. The idea is to verify types but without paying the price in
// no production.
template <typename Fut>
class blocking_future_detail {
  friend class Operation;
  friend class Blocker;
  Blocker *blocker;
  Fut fut;
  blocking_future_detail(Blocker *b, Fut &&f)
    : blocker(b), fut(std::move(f)) {}

  template <typename V, typename U>
  friend blocking_future_detail<seastar::future<V>> make_ready_blocking_future(U&& args);
  template <typename V, typename Exception>
  friend blocking_future_detail<seastar::future<V>>
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

template <typename T=void>
using blocking_future = blocking_future_detail<seastar::future<T>>;

template <typename V, typename U>
blocking_future_detail<seastar::future<V>> make_ready_blocking_future(U&& args) {
  return blocking_future<V>(
    nullptr,
    seastar::make_ready_future<V>(std::forward<U>(args)));
}

template <typename V, typename Exception>
blocking_future_detail<seastar::future<V>>
make_exception_blocking_future(Exception&& e) {
  return blocking_future<V>(
    nullptr,
    seastar::make_exception_future<V>(e));
}

/**
 * Provides an interface for dumping diagnostic information about
 * why a particular op is not making progress.
 */
// TODO: replace with TimedBlocker.
class Blocker {
public:
  template <typename T>
  blocking_future<T> make_blocking_future(seastar::future<T> &&f) {
    return blocking_future<T>(this, std::move(f));
  }
  void dump(ceph::Formatter *f) const;
  virtual ~Blocker() = default;

private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;
  virtual const char *get_type_name() const = 0;
};

template <typename T>
class BlockerT : public Blocker {
public:
  virtual ~BlockerT() = default;
private:
  const char *get_type_name() const final {
    return T::type_name;
  }
};

class AggregateBlocker : public BlockerT<AggregateBlocker> {
  vector<Blocker*> parent_blockers;
public:
  AggregateBlocker(vector<Blocker*> &&parent_blockers)
    : parent_blockers(std::move(parent_blockers)) {}
  static constexpr const char *type_name = "AggregateBlocker";
private:
  void dump_detail(ceph::Formatter *f) const final;
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
	return seastar::make_ready_future<>();
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

  template <typename T>
  seastar::future<T> with_blocking_future(blocking_future<T> &&f) {
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

  void dump(ceph::Formatter *f) const;
  void dump_brief(ceph::Formatter *f) const;
  virtual ~Operation() = default;

 private:
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
public:
  static constexpr const char *type_name = OP_NAMES[static_cast<int>(T::type)];
  using IRef = boost::intrusive_ptr<T>;
  using ICRef = boost::intrusive_ptr<const T>;

  OperationTypeCode get_type() const final {
    return T::type;
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
class OperationRegistry {
  friend class Operation;
  friend class HistoricBackend;
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

  void dump_client_requests(ceph::Formatter* f) const;
  void dump_historic_client_requests(ceph::Formatter* f) const;
};

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

// the main template. by default an operation has no extenral
// event handler (the empty tuple). specializing the template
// allows to define backends on per-operation-type manner.
// NOTE: basically this could be a function but C++ disallows
// differentiating return type among specializations.
template <class T>
struct EventBackendRegistry {
  template <typename...> static constexpr bool always_false = false;

  static std::tuple<> get_backends() {
    static_assert(always_false<T>, "Registry specializarion not found");
    return {};
  }
};

template <class T>
constexpr static auto EVENT_NAMES = "UnknownEvent";

void dump_event_default(const char name[],
                        const utime_t& timestamp,
                        ceph::Formatter* f);

template <class T>
struct Event {
  // IDEA 0: should this be ExternalBackend? Or ExtraBackend?
  struct Backend {
    virtual void handle(T&, const Operation&) = 0;
  };

  struct InternalBackend final : Backend {
    utime_t timestamp;
    void handle(T&, const Operation&) override {
      timestamp = ceph_clock_now();
    }
  } internal_backend;

  // QUESTION: how to convey get_registered_backends() and glue it
  // with particular Operation? The `events` are supposed to vary.
  // QUESTION: do we need to handle any other Operation than Client
  // Request when it comes to Historic?
  template <class U>
  void trigger(U& op) {
    internal_backend.handle(static_cast<T&>(*this), op);

    std::apply([&op, this] (auto... backend) {
      (..., backend.handle(static_cast<T&>(*this), op));
    }, EventBackendRegistry<U>::get_backends());
  }

  void dump(ceph::Formatter* f) const {
    dump_event_default(EVENT_NAMES<T>, internal_backend.timestamp, f);
  }
};


struct EnqueuedEvent : Event<EnqueuedEvent> {
};
template <> constexpr auto EVENT_NAMES<EnqueuedEvent> = "EnqueuedEvent";

struct ResponseEvent : Event<ResponseEvent> {
};
template <> constexpr auto EVENT_NAMES<ResponseEvent> = "ResponseEvent";


// TODO: replace Blocker with TimedBlocker
template <class T>
struct TimedBlocker {
  struct TimedPtr : Event<typename T::TimedPtr> {
    T* blocker = nullptr;
  };

  // having a dedicated `blocking_future` enforces type safety but imposes
  // extra moving (mempcy + a bunch of conditionals). The idea here is to
  // alias `blocking_future<T>` to `seastar::future<T>` but only in production
  // builds.
  template <typename U>
  decltype(auto) make_blocking_future2(TimedPtr& handle,
                                      seastar::future<U> &&f) {
    handle.blocker = static_cast<T*>(this);
    return std::move(f);
  }
};


/**
 * Ensures that at most one op may consider itself in the phase at a time.
 * Ops will see enter() unblock in the order in which they tried to enter
 * the phase.  entering (though not necessarily waiting for the future to
 * resolve) a new phase prior to exiting the previous one will ensure that
 * the op ordering is preserved.
 */
class OrderedPipelinePhase : public Blocker {
private:
  void dump_detail(ceph::Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

public:
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

    template <class NewPhaseT>
    seastar::future<> enter(NewPhaseT &new_phase,
                            typename NewPhaseT::TimedPtr& new_timedref);

    /**
     * Releases the current phase if there is one.  Called in ~Handle().
     */
    void exit();

    ~Handle();
  };

  OrderedPipelinePhase(const char *name) : name(name) {}

private:
  const char * name;
  seastar::shared_mutex mutex;
};


// TODO: this will be dropped after migrating phases.
template <class T>
struct OrderedPipelinePhaseT : public OrderedPipelinePhase,
                               public TimedBlocker<T> {
  OrderedPipelinePhaseT() : OrderedPipelinePhase(T::name) {}
};

template <class T>
void track_event(const T& blocker, std::uint64_t timestamp);

template <class T>
// TODO: class BlockingPhasedOperationT : public OperationT<T> {
class BlockingOperationT : public OperationT<T> {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  // TODO: drop thr `2` after transitioning to `with_blocker<T>()`.
  friend class ClientRequest;
  OrderedPipelinePhase::Handle handle2;

public:
  using OperationT<T>::OperationT;

  template <class BlockerT, class Func>
  auto with_blocker(Func&& f) {
    using HandleT = typename BlockerT::TimedPtr;
    HandleT new_timedref;
    auto fut = std::forward<Func>(f)(new_timedref);
    // mimic the with_blocking_future's behaviour exactly; don't worry
    // about correct timestamping yet.

    // the idea is to have a visitor-like interface that allows to double
    // dispatch (backend, blocker type)
    assert(new_timedref.blocker);
    std::get<HandleT>(that()->blockers).trigger(static_cast<T&>(*this));
    if (fut.available()) {
      return fut;
    } else {
      // FIXME: use after std::move
      std::get<HandleT>(that()->blockers).blocker = new_timedref.blocker;
      return std::move(fut).then_wrapped([this] (auto&& arg) {
        std::get<HandleT>(that()->blockers).blocker = nullptr;
        return std::move(arg);
      });
    }
  }

  template <class PhaseT>
  seastar::future<> enter_phase(PhaseT& new_phase) {
    // yes, every phase is a blocker
    return with_blocker<PhaseT>([this, &new_phase] (auto& new_timedref) {
      return handle2.enter(new_phase, new_timedref);
    });
  }

  void dump_detail(ceph::Formatter* f) const override {
    std::apply([f] (auto... event) {
      (..., event.dump(f));
    }, that()->blockers);
  }
};

template <class NewPhaseT>
inline seastar::future<> OrderedPipelinePhase::Handle::enter(
  NewPhaseT &new_phase,
  typename NewPhaseT::TimedPtr& new_timedref)
{
  auto fut = new_phase.mutex.lock();
  exit();
  phase = &new_phase;
  return new_phase.make_blocking_future2(new_timedref, std::move(fut));
}

}
