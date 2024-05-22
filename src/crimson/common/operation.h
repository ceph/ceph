// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <algorithm>
#include <array>
#include <set>
#include <vector>
#include <boost/core/demangle.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/future-util.hh>

#include "include/ceph_assert.h"
#include "include/utime.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "crimson/common/interruptible_future.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/common/log.h"

namespace ceph {
  class Formatter;
}

namespace crimson {

using registry_hook_t = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

class Operation;
class Blocker;


namespace detail {
void dump_time_event(const char* name,
		     const utime_t& timestamp,
		     ceph::Formatter* f);
void dump_blocking_event(const char* name,
			 const utime_t& timestamp,
			 const Blocker* blocker,
			 ceph::Formatter* f);
} // namespace detail

/**
 * Provides an interface for dumping diagnostic information about
 * why a particular op is not making progress.
 */
class Blocker {
public:
  void dump(ceph::Formatter *f) const;
  virtual ~Blocker() = default;

private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;
  virtual const char *get_type_name() const = 0;
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
    static_assert(always_false<T>, "Registry specialization not found");
    return {};
  }
};

template <class T>
struct Event {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  template <class OpT, class... Args>
  void trigger(OpT&& op, Args&&... args) {
    that()->internal_backend.handle(*that(),
                                    std::forward<OpT>(op),
                                    std::forward<Args>(args)...);
    // let's call `handle()` for concrete event type from each single
    // of our backends. the order in the registry matters.
    std::apply([&, //args=std::forward_as_tuple(std::forward<Args>(args)...),
		this] (auto... backend) {
      (..., backend.handle(*that(),
                           std::forward<OpT>(op),
                           std::forward<Args>(args)...));
    }, EventBackendRegistry<std::decay_t<OpT>>::get_backends());
  }
};


// simplest event type for recording things like beginning or end
// of TrackableOperation's life.
template <class T>
struct TimeEvent : Event<T> {
  struct Backend {
    // `T` is passed solely to let implementations to discriminate
    // basing on the type-of-event.
    virtual void handle(T&, const Operation&) = 0;
  };

  // for the sake of dumping ops-in-flight.
  struct InternalBackend final : Backend {
    void handle(T&, const Operation&) override {
      timestamp = ceph_clock_now();
    }
    utime_t timestamp;
  } internal_backend;

  void dump(ceph::Formatter *f) const {
    auto demangled_name = boost::core::demangle(typeid(T).name());
    detail::dump_time_event(
      demangled_name.c_str(),
      internal_backend.timestamp, f);
  }

  auto get_timestamp() const {
    return internal_backend.timestamp;
  }
};


template <typename T>
class BlockerT : public Blocker {
public:
  struct BlockingEvent : Event<typename T::BlockingEvent> {
    using Blocker = std::decay_t<T>;

    struct Backend {
      // `T` is based solely to let implementations to discriminate
      // basing on the type-of-event.
      virtual void handle(typename T::BlockingEvent&, const Operation&, const T&) = 0;
    };

    struct InternalBackend : Backend {
      void handle(typename T::BlockingEvent&,
                  const Operation&,
                  const T& blocker) override {
        this->timestamp = ceph_clock_now();
        this->blocker = &blocker;
      }

      utime_t timestamp;
      const T* blocker;
    } internal_backend;

    // we don't want to make any BlockerT to be aware and coupled with
    // an operation. to not templatize an entire path from an op to
    // a blocker, type erasuring is used.
    struct TriggerI {
      TriggerI(BlockingEvent& event) : event(event) {}

      template <class FutureT>
      auto maybe_record_blocking(FutureT&& fut, const T& blocker) {
        if (!fut.available()) {
	  // a full blown call via vtable. that's the cost for templatization
	  // avoidance. anyway, most of the things actually have the type
	  // knowledge.
	  record_blocking(blocker);
	  return std::forward<FutureT>(fut).finally(
	    [&event=this->event, &blocker] () mutable {
	    // beware trigger instance may be already dead when this
	    // is executed!
	    record_unblocking(event, blocker);
	  });
	}
	return std::forward<FutureT>(fut);
      }
      virtual ~TriggerI() = default;
    protected:
      // it's for the sake of erasing the OpT type
      virtual void record_blocking(const T& blocker) = 0;

      static void record_unblocking(BlockingEvent& event, const T& blocker) {
	assert(event.internal_backend.blocker == &blocker);
	event.internal_backend.blocker = nullptr;
      }

      BlockingEvent& event;
    };

    template <class OpT>
    struct Trigger : TriggerI {
      Trigger(BlockingEvent& event, const OpT& op) : TriggerI(event), op(op) {}

      template <class FutureT>
      auto maybe_record_blocking(FutureT&& fut, const T& blocker) {
        if (!fut.available()) {
	  // no need for the dynamic dispatch! if we're lucky, a compiler
	  // should collapse all these abstractions into a bunch of movs.
	  this->Trigger::record_blocking(blocker);
	  return std::forward<FutureT>(fut).finally(
	    [&event=this->event, &blocker] () mutable {
	    Trigger::record_unblocking(event, blocker);
	  });
	}
	return std::forward<FutureT>(fut);
      }

      const OpT &get_op() { return op; }

    protected:
      void record_blocking(const T& blocker) override {
	this->event.trigger(op, blocker);
      }

      const OpT& op;
    };

    void dump(ceph::Formatter *f) const {
      auto demangled_name = boost::core::demangle(typeid(T).name());
      detail::dump_blocking_event(
	demangled_name.c_str(),
	internal_backend.timestamp,
	internal_backend.blocker,
	f);
    }
  };

  virtual ~BlockerT() = default;
  template <class TriggerT, class... Args>
  decltype(auto) track_blocking(TriggerT&& trigger, Args&&... args) {
    return std::forward<TriggerT>(trigger).maybe_record_blocking(
      std::forward<Args>(args)..., static_cast<const T&>(*this));
  }

private:
  const char *get_type_name() const final {
    return static_cast<const T*>(this)->type_name;
  }
};

template <class T>
struct AggregateBlockingEvent {
  struct TriggerI {
  protected:
    struct TriggerContainerI {
      virtual typename T::TriggerI& get_trigger() = 0;
      virtual ~TriggerContainerI() = default;
    };
    using TriggerContainerIRef = std::unique_ptr<TriggerContainerI>;
    virtual TriggerContainerIRef create_part_trigger() = 0;

  public:
    template <class FutureT>
    auto maybe_record_blocking(FutureT&& fut,
			       const typename T::Blocker& blocker) {
      // AggregateBlockingEvent is supposed to be used on relatively cold
      // paths (recovery), so we don't need to worry about the dynamic
      // polymothps / dynamic memory's overhead.
      auto tcont = create_part_trigger();
      return tcont->get_trigger().maybe_record_blocking(
	std::move(fut), blocker
      ).finally([tcont=std::move(tcont)] {});
    }

    virtual ~TriggerI() = default;
  };

  template <class OpT>
  struct Trigger final : TriggerI {
    Trigger(AggregateBlockingEvent& event, const OpT& op)
      : event(event), op(op) {}

    class TriggerContainer final : public TriggerI::TriggerContainerI {
      AggregateBlockingEvent& event;
      typename decltype(event.events)::iterator iter;
      typename T::template Trigger<OpT> trigger;

      typename T::TriggerI &get_trigger() final {
	return trigger;
      }

    public:
      TriggerContainer(AggregateBlockingEvent& _event, const OpT& op) :
	event(_event),
	iter(event.events.emplace(event.events.end())),
	trigger(*iter, op) {}

      ~TriggerContainer() final {
	event.events.erase(iter);
      }
    };

  protected:
    typename TriggerI::TriggerContainerIRef create_part_trigger() final {
      return std::make_unique<TriggerContainer>(event, op);
    }

  private:
    AggregateBlockingEvent& event;
    const OpT& op;
  };

private:
  std::list<T> events;
  template <class OpT>
  friend class Trigger;
};

/**
 * Common base for all crimson-osd operations.  Mainly provides
 * an interface for registering ops in flight and dumping
 * diagnostic information.
 */
class Operation : public boost::intrusive_ref_counter<Operation> {
 public:
  using id_t = uint64_t;
  static constexpr id_t NULL_ID = std::numeric_limits<uint64_t>::max();
  id_t get_id() const {
    return id;
  }

  static constexpr bool is_trackable = false;

  virtual unsigned get_type() const = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(std::ostream &) const = 0;

  void dump(ceph::Formatter *f) const;
  void dump_brief(ceph::Formatter *f) const;
  virtual ~Operation() = default;

 private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

  registry_hook_t registry_hook;

  id_t id = 0;
  void set_id(id_t in_id) {
    id = in_id;
  }

  friend class OperationRegistryI;
  template <size_t>
  friend class OperationRegistryT;
};
using OperationRef = boost::intrusive_ptr<Operation>;

std::ostream &operator<<(std::ostream &, const Operation &op);

/**
 * Maintains a set of lists of all active ops.
 */
class OperationRegistryI {
  using op_list_member_option = boost::intrusive::member_hook<
    Operation,
    registry_hook_t,
    &Operation::registry_hook
    >;

  friend class Operation;
  seastar::timer<seastar::lowres_clock> shutdown_timer;
  seastar::promise<> shutdown;

protected:
  virtual void do_register(Operation *op) = 0;
  virtual bool registries_empty() const = 0;
  virtual void do_stop() = 0;

public:
  using op_list = boost::intrusive::list<
    Operation,
    op_list_member_option,
    boost::intrusive::constant_time_size<false>>;

  template <typename T, typename... Args>
  auto create_operation(Args&&... args) {
    boost::intrusive_ptr<T> op = new T(std::forward<Args>(args)...);
    do_register(&*op);
    return op;
  }

  seastar::future<> stop() {
    crimson::get_logger(ceph_subsys_osd).info("OperationRegistryI::{}", __func__);
    do_stop();
    shutdown_timer.set_callback([this] {
      if (registries_empty()) {
	shutdown.set_value();
	shutdown_timer.cancel();
      }
    });
    shutdown_timer.arm_periodic(
      std::chrono::milliseconds(100/*TODO: use option instead*/));
    return shutdown.get_future();
  }
};


template <size_t NUM_REGISTRIES>
class OperationRegistryT : public OperationRegistryI {
  Operation::id_t next_id = 0;
  std::array<
    op_list,
    NUM_REGISTRIES
  > registries;

protected:
  void do_register(Operation *op) final {
    const auto op_type = op->get_type();
    registries[op_type].push_back(*op);
    op->set_id(++next_id);
  }

  bool registries_empty() const final {
    return std::all_of(registries.begin(),
		       registries.end(),
		       [](auto& opl) {
			 return opl.empty();
		       });
  }

protected:
  OperationRegistryT(core_id_t core)
    // Use core to initialize upper 8 bits of counters to ensure that
    // ids generated by different cores are disjoint
    : next_id(static_cast<id_t>(core) <<
	      (std::numeric_limits<id_t>::digits - 8))
  {}

  template <size_t REGISTRY_INDEX>
  const op_list& get_registry() const {
    static_assert(
      REGISTRY_INDEX < std::tuple_size<decltype(registries)>::value);
    return registries[REGISTRY_INDEX];
  }

  template <size_t REGISTRY_INDEX>
  op_list& get_registry() {
    static_assert(
      REGISTRY_INDEX < std::tuple_size<decltype(registries)>::value);
    return registries[REGISTRY_INDEX];
  }

public:
  /// Iterate over live ops
  template <typename F>
  void for_each_op(F &&f) const {
    for (const auto &registry: registries) {
      for (const auto &op: registry) {
	std::invoke(f, op);
      }
    }
  }

  /// Removes op from registry
  void remove_from_registry(Operation &op) {
    const auto op_type = op.get_type();
    registries[op_type].erase(op_list::s_iterator_to(op));
  }

  /// Adds op to registry
  void add_to_registry(Operation &op) {
    const auto op_type = op.get_type();
    registries[op_type].push_back(op);
  }
};

class PipelineExitBarrierI {
public:
  using Ref = std::unique_ptr<PipelineExitBarrierI>;

  /// Waits for exit barrier
  virtual std::optional<seastar::future<>> wait() = 0;

  /// Releases pipeline resources, after or without waiting
  // FIXME: currently, exit() will discard the associated future even if it is
  // still unresolved, which is discouraged by seastar.
  virtual void exit() = 0;

  /// Must ensure that resources are released, likely by calling exit()
  virtual ~PipelineExitBarrierI() {}
};

template <class T>
class PipelineStageIT : public BlockerT<T> {
public:
#ifndef NDEBUG
  const core_id_t core = seastar::this_shard_id();
#endif

  template <class... Args>
  decltype(auto) enter(Args&&... args) {
    return static_cast<T*>(this)->enter(std::forward<Args>(args)...);
  }
};

class PipelineHandle {
  PipelineExitBarrierI::Ref barrier;

  std::optional<seastar::future<>> wait_barrier() {
    return barrier ? barrier->wait() : std::nullopt;
  }

public:
  PipelineHandle() = default;

  PipelineHandle(const PipelineHandle&) = delete;
  PipelineHandle(PipelineHandle&&) = default;
  PipelineHandle &operator=(const PipelineHandle&) = delete;
  PipelineHandle &operator=(PipelineHandle&&) = default;

  /**
   * Returns a future which unblocks when the handle has entered the passed
   * OrderedPipelinePhase.  If already in a phase, enter will also release
   * that phase after placing itself in the queue for the next one to preserve
   * ordering.
   */
  template <typename OpT, typename T>
  seastar::future<>
  enter(T &stage, typename T::BlockingEvent::template Trigger<OpT>&& t) {
    assert(stage.core == seastar::this_shard_id());
    auto wait_fut = wait_barrier();
    if (wait_fut.has_value()) {
      return wait_fut.value().then([this, &stage, t=std::move(t)] () mutable {
        auto fut = t.maybe_record_blocking(stage.enter(t), stage);
        exit();
        return std::move(fut).then(
          [this, t=std::move(t)](auto &&barrier_ref) mutable {
          barrier = std::move(barrier_ref);
          return seastar::now();
        });
      });
    } else {
        auto fut = t.maybe_record_blocking(stage.enter(t), stage);
        exit();
        return std::move(fut).then(
          [this, t=std::move(t)](auto &&barrier_ref) mutable {
          barrier = std::move(barrier_ref);
          return seastar::now();
        });
    }
  }

  /**
   * Completes pending exit barrier without entering a new one.
   */
  seastar::future<> complete() {
    auto ret = wait_barrier();
    barrier.reset();
    return ret ? std::move(ret.value()) : seastar::now();
  }

  /**
   * Exits current phase, skips exit barrier, should only be used for op
   * failure.  Permitting the handle to be destructed as the same effect.
   */
  void exit() {
    barrier.reset();
  }

};

/**
 * Ensures that at most one op may consider itself in the phase at a time.
 * Ops will see enter() unblock in the order in which they tried to enter
 * the phase.  entering (though not necessarily waiting for the future to
 * resolve) a new phase prior to exiting the previous one will ensure that
 * the op ordering is preserved.
 */
template <class T>
class OrderedExclusivePhaseT : public PipelineStageIT<T> {
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_unsigned("waiting", waiting);
    if (held_by != Operation::NULL_ID) {
      f->dump_unsigned("held_by_operation_id", held_by);
    }
  }

  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedExclusivePhaseT *phase;
    Operation::id_t op_id;
  public:
    ExitBarrier(OrderedExclusivePhaseT &phase, Operation::id_t id)
      : phase(&phase), op_id(id) {}

    std::optional<seastar::future<>> wait() final {
      return std::nullopt;
    }

    void exit() final {
      if (phase) {
        assert(phase->core == seastar::this_shard_id());
        phase->exit(op_id);
        phase = nullptr;
      }
    }

    ~ExitBarrier() final {
      exit();
    }
  };

  void exit(Operation::id_t op_id) {
    clear_held_by(op_id);
    mutex.unlock();
  }

public:
  template <class TriggerT>
  seastar::future<PipelineExitBarrierI::Ref> enter(TriggerT& t) {
    waiting++;
    return mutex.lock().then([this, op_id=t.get_op().get_id()] {
      ceph_assert_always(waiting > 0);
      --waiting;
      set_held_by(op_id);
      return PipelineExitBarrierI::Ref(new ExitBarrier{*this, op_id});
    });
  }

private:
  void set_held_by(Operation::id_t id) {
    ceph_assert_always(held_by == Operation::NULL_ID);
    held_by = id;
  }

  void clear_held_by(Operation::id_t id) {
    ceph_assert_always(held_by == id);
    held_by = Operation::NULL_ID;
  }

  unsigned waiting = 0;
  seastar::shared_mutex mutex;
  Operation::id_t held_by = Operation::NULL_ID;
};

/**
 * Permits multiple ops to inhabit the stage concurrently, but ensures that
 * they will proceed to the next stage in the order in which they called
 * enter.
 */
template <class T>
class OrderedConcurrentPhaseT : public PipelineStageIT<T> {
  using base_t = PipelineStageIT<T>;
public:
  struct BlockingEvent : base_t::BlockingEvent {
    using base_t::BlockingEvent::BlockingEvent;

    struct ExitBarrierEvent : TimeEvent<ExitBarrierEvent> {};

    template <class OpT>
    struct Trigger : base_t::BlockingEvent::template Trigger<OpT> {
      using base_t::BlockingEvent::template Trigger<OpT>::Trigger;

      template <class FutureT>
      decltype(auto) maybe_record_exit_barrier(FutureT&& fut) {
        if (!fut.available()) {
	  exit_barrier_event.trigger(this->op);
	}
	return std::forward<FutureT>(fut);
      }

      ExitBarrierEvent exit_barrier_event;
    };
  };

private:
  void dump_detail(ceph::Formatter *f) const final {}

  template <class TriggerT>
  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedConcurrentPhaseT *phase;
    std::optional<seastar::future<>> barrier;
    TriggerT trigger;
  public:
    ExitBarrier(
      OrderedConcurrentPhaseT &phase,
      seastar::future<> &&barrier,
      TriggerT& trigger) : phase(&phase), barrier(std::move(barrier)), trigger(trigger) {}

    std::optional<seastar::future<>> wait() final {
      assert(phase);
      assert(barrier);
      auto ret = std::move(*barrier);
      barrier = std::nullopt;
      return trigger.maybe_record_exit_barrier(std::move(ret));
    }

    void exit() final {
      if (barrier) {
        assert(phase);
        assert(phase->core == seastar::this_shard_id());
        std::ignore = std::move(*barrier
        ).then([phase=this->phase] {
          phase->mutex.unlock();
        });
        barrier = std::nullopt;
        phase = nullptr;
      } else if (phase) {
        assert(phase->core == seastar::this_shard_id());
        phase->mutex.unlock();
        phase = nullptr;
      }
    }

    ~ExitBarrier() final {
      exit();
    }
  };

public:
  template <class TriggerT>
  seastar::future<PipelineExitBarrierI::Ref> enter(TriggerT& t) {
    return seastar::make_ready_future<PipelineExitBarrierI::Ref>(
      new ExitBarrier<TriggerT>{*this, mutex.lock(), t});
  }

private:
  seastar::shared_mutex mutex;
};

/**
 * Imposes no ordering or exclusivity at all.  Ops enter without constraint and
 * may exit in any order.  Useful mainly for informational purposes between
 * stages with constraints.
 */
template <class T>
class UnorderedStageT : public PipelineStageIT<T> {
  void dump_detail(ceph::Formatter *f) const final {}

  class ExitBarrier final : public PipelineExitBarrierI {
  public:
    ExitBarrier() = default;

    std::optional<seastar::future<>> wait() final {
      return std::nullopt;
    }

    void exit() final {}

    ~ExitBarrier() final {}
  };

public:
  template <class... IgnoreArgs>
  seastar::future<PipelineExitBarrierI::Ref> enter(IgnoreArgs&&...) {
    return seastar::make_ready_future<PipelineExitBarrierI::Ref>(
      new ExitBarrier);
  }
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::Operation> : fmt::ostream_formatter {};
#endif
