// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

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
#include <seastar/core/future-util.hh>

#include "include/ceph_assert.h"
#include "crimson/common/interruptible_future.h"

namespace ceph {
  class Formatter;
}

namespace crimson {

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

  template <typename V, typename... U>
  friend blocking_future_detail<seastar::future<V>>
  make_ready_blocking_future(U&&... args);

  template <typename V, typename Exception>
  friend blocking_future_detail<seastar::future<V>>
  make_exception_blocking_future(Exception&& e);

  template <typename U>
  friend blocking_future_detail<seastar::future<>> join_blocking_futures(U &&u);

  template <typename InterruptCond, typename T>
  friend blocking_future_detail<
    ::crimson::interruptible::interruptible_future<InterruptCond>>
  join_blocking_interruptible_futures(T&& t);

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
  template <typename InterruptCond, typename F>
  auto then_interruptible(F &&f) && {
    using result = decltype(std::declval<Fut>().then_interruptible(f));
    return blocking_future_detail<
      typename ::crimson::interruptible::interruptor<
	InterruptCond>::template futurize<result>::type>(
      blocker,
      std::move(fut).then_interruptible(std::forward<F>(f)));
  }
};

template <typename T=void>
using blocking_future = blocking_future_detail<seastar::future<T>>;

template <typename InterruptCond, typename T = void>
using blocking_interruptible_future = blocking_future_detail<
  ::crimson::interruptible::interruptible_future<InterruptCond, T>>;

template <typename InterruptCond, typename V, typename U>
blocking_interruptible_future<InterruptCond, V>
make_ready_blocking_interruptible_future(U&& args) {
  return blocking_interruptible_future<InterruptCond, V>(
    nullptr,
    seastar::make_ready_future<V>(std::forward<U>(args)));
}

template <typename InterruptCond, typename V, typename Exception>
blocking_interruptible_future<InterruptCond, V>
make_exception_blocking_interruptible_future(Exception&& e) {
  return blocking_interruptible_future<InterruptCond, V>(
    nullptr,
    seastar::make_exception_future<InterruptCond, V>(e));
}

template <typename V=void, typename... U>
blocking_future_detail<seastar::future<V>> make_ready_blocking_future(U&&... args) {
  return blocking_future<V>(
    nullptr,
    seastar::make_ready_future<V>(std::forward<U>(args)...));
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
class Blocker {
public:
  template <typename T>
  blocking_future<T> make_blocking_future(seastar::future<T> &&f) {
    return blocking_future<T>(this, std::move(f));
  }

  template <typename InterruptCond, typename T>
  blocking_interruptible_future<InterruptCond, T>
  make_blocking_future(
      crimson::interruptible::interruptible_future<InterruptCond, T> &&f) {
    return blocking_interruptible_future<InterruptCond, T>(
      this, std::move(f));
  }
  template <typename InterruptCond, typename T = void>
  blocking_interruptible_future<InterruptCond, T>
  make_blocking_interruptible_future(seastar::future<T> &&f) {
    return blocking_interruptible_future<InterruptCond, T>(
	this,
	::crimson::interruptible::interruptor<InterruptCond>::make_interruptible(
	  std::move(f)));
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
  std::vector<Blocker*> parent_blockers;
public:
  AggregateBlocker(std::vector<Blocker*> &&parent_blockers)
    : parent_blockers(std::move(parent_blockers)) {}
  static constexpr const char *type_name = "AggregateBlocker";
private:
  void dump_detail(ceph::Formatter *f) const final;
};

template <typename T>
blocking_future<> join_blocking_futures(T &&t) {
  std::vector<Blocker*> blockers;
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

template <typename InterruptCond, typename T>
blocking_interruptible_future<InterruptCond>
join_blocking_interruptible_futures(T&& t) {
  std::vector<Blocker*> blockers;
  blockers.reserve(t.size());
  for (auto &&bf: t) {
    blockers.push_back(bf.blocker);
    bf.blocker = nullptr;
  }
  auto agg = std::make_unique<AggregateBlocker>(std::move(blockers));
  return agg->make_blocking_future(
    ::crimson::interruptible::interruptor<InterruptCond>::parallel_for_each(
      std::forward<T>(t),
      [](auto &&bf) {
	return std::move(bf.fut);
      }).then_interruptible([agg=std::move(agg)] {
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

  virtual unsigned get_type() const = 0;
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

  template <typename InterruptCond, typename T>
  ::crimson::interruptible::interruptible_future<InterruptCond, T>
  with_blocking_future_interruptible(blocking_future<T> &&f) {
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
      InterruptCond, T>(std::move(fut));
  }

  template <typename InterruptCond, typename T>
  ::crimson::interruptible::interruptible_future<InterruptCond, T>
  with_blocking_future_interruptible(
    blocking_interruptible_future<InterruptCond, T> &&f) {
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

  void dump(ceph::Formatter *f);
  void dump_brief(ceph::Formatter *f);
  virtual ~Operation() = default;

 private:
  virtual void dump_detail(ceph::Formatter *f) const = 0;

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
  friend class Operation;
  seastar::timer<seastar::lowres_clock> shutdown_timer;
  seastar::promise<> shutdown;

protected:
  virtual void do_register(Operation *op) = 0;
  virtual bool registries_empty() const = 0;

public:
  template <typename T, typename... Args>
  typename T::IRef create_operation(Args&&... args) {
    typename T::IRef op = new T(std::forward<Args>(args)...);
    do_register(&*op);
    return op;
  }

  seastar::future<> stop() {
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
    NUM_REGISTRIES
  > registries;

  std::array<
    uint64_t,
    NUM_REGISTRIES
  > op_id_counters = {};

protected:
  void do_register(Operation *op) final {
    registries[op->get_type()].push_back(*op);
    op->set_id(++op_id_counters[op->get_type()]);
  }

  bool registries_empty() const final {
    return std::all_of(registries.begin(),
		       registries.end(),
		       [](auto& opl) {
			 return opl.empty();
		       });
  }
};

class PipelineExitBarrierI {
public:
  using Ref = std::unique_ptr<PipelineExitBarrierI>;

  /// Waits for exit barrier
  virtual seastar::future<> wait() = 0;

  /// Releases pipeline stage, can only be called after wait
  virtual void exit() = 0;

  /// Releases pipeline resources without waiting on barrier
  virtual void cancel() = 0;

  /// Must ensure that resources are released, likely by calling cancel()
  virtual ~PipelineExitBarrierI() {}
};

class PipelineStageI : public Blocker {
public:
  virtual seastar::future<PipelineExitBarrierI::Ref> enter() = 0;
};

class PipelineHandle {
  PipelineExitBarrierI::Ref barrier;

  auto wait_barrier() {
    return barrier ? barrier->wait() : seastar::now();
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
  template <typename T>
  blocking_future<> enter(T &t) {
    /* Strictly speaking, we probably want the blocker to be registered on
     * the previous stage until wait_barrier() resolves and on the next
     * until enter() resolves, but blocking_future will need some refactoring
     * to permit that.  TODO
     */
    return t.make_blocking_future(
      wait_barrier().then([this, &t] {
	auto fut = t.enter();
	exit();
	return std::move(fut).then([this](auto &&barrier_ref) {
	  barrier = std::move(barrier_ref);
	  return seastar::now();
	});
      })
    );
  }

  /**
   * Completes pending exit barrier without entering a new one.
   */
  seastar::future<> complete() {
    auto ret = wait_barrier();
    barrier.reset();
    return ret;
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
class OrderedExclusivePhase : public PipelineStageI {
  void dump_detail(ceph::Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedExclusivePhase *phase;
  public:
    ExitBarrier(OrderedExclusivePhase *phase) : phase(phase) {}

    seastar::future<> wait() final {
      return seastar::now();
    }

    void exit() final {
      if (phase) {
	phase->exit();
	phase = nullptr;
      }
    }

    void cancel() final {
      exit();
    }

    ~ExitBarrier() final {
      cancel();
    }
  };

  void exit() {
    mutex.unlock();
  }

public:
  seastar::future<PipelineExitBarrierI::Ref> enter() final {
    return mutex.lock().then([this] {
      return PipelineExitBarrierI::Ref(new ExitBarrier{this});
    });
  }

  OrderedExclusivePhase(const char *name) : name(name) {}

private:
  const char * name;
  seastar::shared_mutex mutex;
};

/**
 * Permits multiple ops to inhabit the stage concurrently, but ensures that
 * they will proceed to the next stage in the order in which they called
 * enter.
 */
class OrderedConcurrentPhase : public PipelineStageI {
  void dump_detail(ceph::Formatter *f) const final;
  const char *get_type_name() const final {
    return name;
  }

  class ExitBarrier final : public PipelineExitBarrierI {
    OrderedConcurrentPhase *phase;
    std::optional<seastar::future<>> barrier;
  public:
    ExitBarrier(
      OrderedConcurrentPhase *phase,
      seastar::future<> &&barrier) : phase(phase), barrier(std::move(barrier)) {}

    seastar::future<> wait() final {
      assert(phase);
      assert(barrier);
      auto ret = std::move(*barrier);
      barrier = std::nullopt;
      return ret;
    }

    void exit() final {
      if (barrier) {
	static_cast<void>(
	  std::move(*barrier).then([phase=this->phase] { phase->mutex.unlock(); }));
	barrier = std::nullopt;
	phase = nullptr;
      }
      if (phase) {
	phase->mutex.unlock();
	phase = nullptr;
      }
    }

    void cancel() final {
      exit();
    }

    ~ExitBarrier() final {
      cancel();
    }
  };

public:
  seastar::future<PipelineExitBarrierI::Ref> enter() final {
    return seastar::make_ready_future<PipelineExitBarrierI::Ref>(
      new ExitBarrier{this, mutex.lock()});
  }

  OrderedConcurrentPhase(const char *name) : name(name) {}

private:
  const char * name;
  seastar::shared_mutex mutex;
};

}
