// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <seastar/core/coroutine.hh>

#include "crimson/common/errorator.h"
#include "crimson/common/interruptible_future.h"


namespace crimson {
namespace internal {

template <typename Interruptor, typename Errorator>
struct to_future {
  template <typename T>
  using future = crimson::interruptible::interruptible_future_detail<
    typename Interruptor::condition,
    typename Errorator::template future<T>>;
};

template <typename Errorator>
struct to_future<void, Errorator> {
  template <typename T>
  using future = typename Errorator::template future<T>;
};


template <typename Interruptor>
struct to_future<Interruptor, void> {
  template <typename T>
  using future = ::crimson::interruptible::interruptible_future<
    typename Interruptor::condition, T>;
};

template <>
struct to_future<void, void> {
  template <typename T>
  using future = seastar::future<T>;
};


template <typename Future>
struct cond_checker {
  using ref = std::unique_ptr<cond_checker>;
  virtual std::optional<Future> may_interrupt() = 0;
  virtual ~cond_checker() = default;
};

template <typename Interruptor>
struct interrupt_cond_capture {
  using InterruptCond = Interruptor::condition;
  interruptible::InterruptCondRef<InterruptCond> cond;

  template <typename Future>
  struct type_erased_cond_checker final : cond_checker<Future> {
    interruptible::InterruptCondRef<InterruptCond> cond;

    template <typename T>
    type_erased_cond_checker(T &&t) : cond(std::forward<T>(t)) {}

    std::optional<Future> may_interrupt() final {
      return cond->template may_interrupt<Future>();
    }
  };

  template <typename Future>
  typename cond_checker<Future>::ref capture_and_get_checker() {
    ceph_assert(interruptible::interrupt_cond<InterruptCond>.interrupt_cond);
    cond = interruptible::interrupt_cond<InterruptCond>.interrupt_cond;
    return typename cond_checker<Future>::ref{
      new type_erased_cond_checker<Future>{cond}
    };
  }

  void restore() {
    ceph_assert(cond);
    interruptible::interrupt_cond<InterruptCond>.set(cond);
  }

  void reset() {
    interruptible::interrupt_cond<InterruptCond>.reset();
  }
};

template <>
struct interrupt_cond_capture<void> {
  template <typename Future>
  typename cond_checker<Future>::ref capture_and_get_checker() {
    return nullptr;
  }
};

template <typename Interruptor>
struct seastar_task_ancestor : protected seastar::task {};

template <>
struct seastar_task_ancestor<void> : public seastar::task {};

template <typename Interruptor, typename Errorator, typename T>
class promise_base : public seastar_task_ancestor<Interruptor> {
protected:
  seastar::promise<T> _promise;

public:
  interrupt_cond_capture<Interruptor> cond;

  using errorator_type = Errorator;
  using interruptor = Interruptor;
  static constexpr bool is_errorated = !std::is_void<Errorator>::value;
  static constexpr bool is_interruptible = !std::is_void<Interruptor>::value;

  using _to_future =  to_future<Interruptor, Errorator>;

  template <typename U=void>
  using future = typename _to_future::template future<U>;

  promise_base() = default;
  promise_base(promise_base&&) = delete;
  promise_base(const promise_base&) = delete;

  void set_exception(std::exception_ptr&& eptr) noexcept {
    _promise.set_exception(std::move(eptr));
  }

  void unhandled_exception() noexcept {
    _promise.set_exception(std::current_exception());
  }

  future<T> get_return_object() noexcept {
    return _promise.get_future();
  }

  std::suspend_never initial_suspend() noexcept { return { }; }
  std::suspend_never final_suspend() noexcept { return { }; }

  void run_and_dispose() noexcept final {
    if constexpr (is_interruptible) {
      cond.restore();
    }
    auto handle = std::coroutine_handle<promise_base>::from_promise(*this);
    handle.resume();
    if constexpr (is_interruptible) {
      cond.reset();
    }
  }

  seastar::task *waiting_task() noexcept override {
    return _promise.waiting_task();
  }
  seastar::task *get_seastar_task() { return this; }
};

template <typename Interruptor, typename Errorator, typename T=void>
class coroutine_traits {
public:
  class promise_type final : public promise_base<Interruptor, Errorator, T> {
    using base = promise_base<Interruptor, Errorator, T>;
  public:
    template <typename... U>
    void return_value(U&&... value) {
      base::_promise.set_value(std::forward<U>(value)...);
    }
  };
};


template <typename Interruptor, typename Errorator>
class coroutine_traits<Interruptor, Errorator> {
public:
  class promise_type final : public promise_base<Interruptor, Errorator, void> {
    using base = promise_base<Interruptor, Errorator, void>;
  public:
    void return_void() noexcept {
      base::_promise.set_value();
    }
  };
};

template <typename Interruptor, typename Errorator,
	  bool CheckPreempt, typename T=void>
struct awaiter {
  static constexpr bool is_errorated = !std::is_void<Errorator>::value;
  static constexpr bool is_interruptible = !std::is_void<Interruptor>::value;

  template <typename U=void>
  using future = typename to_future<Interruptor, Errorator>::template future<U>;

  future<T> _future;

  cond_checker<future<T>>::ref checker;
public:
  explicit awaiter(future<T>&& f) noexcept : _future(std::move(f)) { }

  awaiter(const awaiter&) = delete;
  awaiter(awaiter&&) = delete;

  bool await_ready() const noexcept {
    return _future.available() && (!CheckPreempt || !seastar::need_preempt());
  }

  template <typename U>
  void await_suspend(std::coroutine_handle<U> hndl) noexcept {
    if constexpr (is_errorated) {
      using dest_errorator_t  = U::errorator_type;
      static_assert(dest_errorator_t::template contains_once_v<Errorator>,
		    "conversion is possible to more-or-eq errorated future!");
    }

    checker =
      hndl.promise().cond.template capture_and_get_checker<future<T>>();
    if (!CheckPreempt || !_future.available()) {
      _future.set_coroutine(*hndl.promise().get_seastar_task());
    } else {
      ::seastar::schedule(hndl.promise().get_seastar_task());
    }
  }

  T await_resume() {
    if (auto maybe_fut = checker ? checker->may_interrupt() : std::nullopt) {
      if constexpr (is_errorated) {
	return maybe_fut->unsafe_get0();
      } else {
	return maybe_fut->get0();
      }
    } else {
      if constexpr (is_errorated) {
	return _future.unsafe_get0();
      } else {
	return _future.get0();
      }
    }
  }
};

}
}

template <template <typename> typename Container, typename T>
auto operator co_await(
  Container<crimson::errorated_future_marker<T>> f) noexcept {
  using Errorator = seastar::futurize<decltype(f)>::errorator_type;
  return crimson::internal::awaiter<void, Errorator, true, T>(std::move(f));
}

template <typename InterruptCond, typename T>
auto operator co_await(
  crimson::interruptible::interruptible_future_detail<
    InterruptCond, seastar::future<T>
  > f) noexcept {
  return crimson::internal::awaiter<
    crimson::interruptible::interruptor<InterruptCond>, void, true, T>(
  std::move(f));
}

template <template <typename> typename Container,
	  typename InterruptCond, typename T>
auto operator co_await(
  crimson::interruptible::interruptible_future_detail<
    InterruptCond, Container<crimson::errorated_future_marker<T>>
  > f) noexcept {
  using Errorator = seastar::futurize<decltype(f)>::errorator_type;
  return crimson::internal::awaiter<
    crimson::interruptible::interruptor<InterruptCond>,
    typename Errorator::base_ertr, true, T>(
  std::move(f));
}

namespace std {

template <template <typename> typename Container,
	  typename T, typename... Args>
class coroutine_traits<Container<crimson::errorated_future_marker<T>>, Args...> :
    public crimson::internal::coroutine_traits<
      void,
      typename seastar::futurize<
	Container<crimson::errorated_future_marker<T>>
	>::errorator_type,
  T> {};

template <typename InterruptCond,
	  typename T, typename... Args>
class coroutine_traits<
  crimson::interruptible::interruptible_future_detail<
    InterruptCond, seastar::future<T>
    >, Args...> : public crimson::internal::coroutine_traits<
  crimson::interruptible::interruptor<InterruptCond>,
  void,
  T> {};

template <template <typename> typename Container,
	  typename InterruptCond,
	  typename T, typename... Args>
class coroutine_traits<
  crimson::interruptible::interruptible_future_detail<
    InterruptCond, Container<crimson::errorated_future_marker<T>>
    >, Args...> :
    public crimson::internal::coroutine_traits<
      crimson::interruptible::interruptor<InterruptCond>,
      typename seastar::futurize<
        crimson::interruptible::interruptible_future_detail<
	  InterruptCond,
          Container<crimson::errorated_future_marker<T>>
	  >
      >::errorator_type::base_ertr,
      T> {};
}
