// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/thread.hh>

#include "crimson/common/errorator.h"

// The interrupt condition generally works this way:
//
//  1. It is created by call_with_interruption_impl method, and is recorded in the thread
//     local global variable "::crimson::interruptible::interrupt_cond".
//  2. Any continuation that's created within the execution of the continuation
//     that calls the call_with_interruption_impl method will capture the "interrupt_cond";
//     and when they starts to run, they will put that capture interruption condition
//     into "::crimson::interruptible::interrupt_cond" so that further continuations
//     created can also capture the interruption condition;
//  3. At the end of the continuation run, the global "interrupt_cond" will be cleared
//     to prevent other continuations that are not supposed to be interrupted wrongly
//     capture an interruption condition.
// With this approach, continuations capture the interrupt condition at their creation,
// restore the interrupt conditions at the beginning of their execution and clear those
// interrupt conditions at the end of their execution. So the global "interrupt_cond"
// only hold valid interrupt conditions when the corresponding continuations are actually
// running after which it gets cleared. Since continuations can't be executed simultaneously,
// different continuation chains won't be able to interfere with each other.
//
// The global "interrupt_cond" can work as a signal about whether the continuation
// is supposed to be interrupted, the reason that the global "interrupt_cond"
// exists is that there may be this scenario:
//
//     Say there's some method PG::func1(), in which the continuations created may
//     or may not be supposed to be interrupted in different situations. If we don't
//     have a global signal, we have to add an extra parameter to every method like
//     PG::func1() to indicate whether the current run should create to-be-interrupted
//     continuations or not.
//
// interruptor::with_interruption() and helpers can be used by users to wrap a future in
// the interruption machinery.

namespace crimson::interruptible {

struct ready_future_marker {};
struct exception_future_marker {};

template <typename InterruptCond>
class interruptible_future_builder;

template <typename InterruptCond>
struct interruptor;

template <typename InterruptCond>
using InterruptCondRef = seastar::lw_shared_ptr<InterruptCond>;

template <typename InterruptCond>
thread_local InterruptCondRef<InterruptCond> interrupt_cond;

template <typename InterruptCond, typename FutureType>
class [[nodiscard]] interruptible_future_detail {};

template <typename FutureType>
struct is_interruptible_future : public std::false_type {};

template <typename InterruptCond, typename FutureType>
struct is_interruptible_future<
  interruptible_future_detail<
    InterruptCond,
    FutureType>>
  : public std::true_type {};

namespace internal {

template <typename InterruptCond, typename Func, typename... Args>
auto call_with_interruption_impl(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func, Args&&... args)
{
  using futurator_t = seastar::futurize<std::invoke_result_t<Func, Args...>>;
  // there might be a case like this:
  // 	with_interruption([] {
  // 		interruptor::do_for_each([] {
  // 			...
  // 			return interruptible_errorated_future();
  // 		}).safe_then_interruptible([] {
  // 			...
  // 		});
  // 	})
  // In this case, as crimson::do_for_each would directly do futurize_invoke
  // for "call_with_interruption", we have make sure this invocation would
  // errorly release ::crimson::interruptible::interrupt_cond<InterruptCond>
  bool set_int_cond = false;

  // If there exists an interrupt condition, which means "Func" may not be
  // permitted to run as a result of the interruption, test it. If it does
  // need to be interrupted, return an interruption; otherwise, restore the
  // global "interrupt_cond" with the interruption condition, and go ahead
  // executing the Func.
  if (!interrupt_cond<InterruptCond> && interrupt_condition) {
    auto [interrupt, fut] = interrupt_condition->template may_interrupt<
      typename futurator_t::type>();
    if (interrupt) {
      return std::move(*fut);
    }
    set_int_cond = true;
    interrupt_cond<InterruptCond> = interrupt_condition;
  }

  auto fut = seastar::futurize_invoke(
      std::forward<Func>(func),
      std::forward<Args>(args)...);
  // Clear the global "interrupt_cond" to prevent it from interfering other
  // continuation chains.
  if (set_int_cond && interrupt_cond<InterruptCond>)
    interrupt_cond<InterruptCond>.release();
  return fut;
}

}

template <typename InterruptCond, typename Func, typename Ret,
	  typename Result = std::invoke_result_t<Func, Ret>,
	  std::enable_if_t<!InterruptCond::template is_interruption_v<Ret> &&
	    seastar::is_future<Ret>::value, int> = 0>
auto call_with_interruption(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func, Ret&& fut)
{
  // if "T" is already an interrupt exception, return it directly;
  // otherwise, upper layer application may encounter errors executing
  // the "Func" body.
  if (fut.failed()) {
    std::exception_ptr eptr = fut.get_exception();
    if (interrupt_condition->is_interruption(eptr)) {
      return seastar::futurize<Result>::make_exception_future(std::move(eptr));
    }
    return internal::call_with_interruption_impl(
	      interrupt_condition,
	      std::forward<Func>(func),
	      seastar::futurize<Ret>::make_exception_future(
		std::move(eptr)));
  }
  return internal::call_with_interruption_impl(
	    interrupt_condition,
	    std::forward<Func>(func),
	    std::move(fut));
}

template <typename InterruptCond, typename Func, typename T,
	  typename Result = std::invoke_result_t<Func, T>,
	  std::enable_if_t<InterruptCond::template is_interruption_v<T>, int> = 0>
auto call_with_interruption(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func, T&& arg)
{
  // if "T" is already an interrupt exception, return it directly;
  // otherwise, upper layer application may encounter errors executing
  // the "Func" body.
  return seastar::futurize<Result>::make_exception_future(
      std::get<0>(std::tuple(std::forward<T>(arg))));
}

template <typename InterruptCond, typename Func, typename T,
	  typename Result = std::invoke_result_t<Func, T>,
	  std::enable_if_t<
	    !InterruptCond::template is_interruption_v<T> &&
	      !seastar::is_future<T>::value, int> = 0>
auto call_with_interruption(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func, T&& arg)
{
  return internal::call_with_interruption_impl(
	    interrupt_condition,
	    std::forward<Func>(func),
	    std::forward<T>(arg));
}

template <typename InterruptCond, typename Func,
	  typename Result = std::invoke_result_t<Func>>
auto call_with_interruption(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func)
{
  return internal::call_with_interruption_impl(
	    interrupt_condition,
	    std::forward<Func>(func));
}

template <typename InterruptCond, typename Func, typename... T,
	  typename Result = std::invoke_result_t<Func, T...>>
Result non_futurized_call_with_interruption(
  InterruptCondRef<InterruptCond> interrupt_condition,
  Func&& func, T&&... args)
{
  bool set_int_cond = false;
  if (!interrupt_cond<InterruptCond> && interrupt_condition) {
    auto [interrupt, fut] = interrupt_condition->template may_interrupt<seastar::future<>>();
    if (interrupt) {
      std::rethrow_exception(fut->get_exception());
    }
    set_int_cond = true;
    interrupt_cond<InterruptCond> = std::move(interrupt_condition);
  }
  try {
    if constexpr (std::is_void_v<Result>) {
      std::invoke(std::forward<Func>(func), std::forward<T>(args)...);

      // Clear the global "interrupt_cond" to prevent it from interfering other
      // continuation chains.
      if (set_int_cond && interrupt_cond<InterruptCond>)
	interrupt_cond<InterruptCond>.release();
      return;
    } else {
      auto&& err = std::invoke(std::forward<Func>(func), std::forward<T>(args)...);
      if (set_int_cond && interrupt_cond<InterruptCond>)
	interrupt_cond<InterruptCond>.release();
      return std::forward<Result>(err);
    }
  } catch (std::exception& e) {
    // Clear the global "interrupt_cond" to prevent it from interfering other
    // continuation chains.
    if (set_int_cond && interrupt_cond<InterruptCond>)
      interrupt_cond<InterruptCond>.release();
    throw e;
  }
}

template <typename InterruptCond, typename Errorator>
struct interruptible_errorator;

template <typename T>
struct parallel_for_each_ret {
  static_assert(seastar::is_future<T>::value);
  using type = seastar::future<>;
};

template <template <typename...> typename ErroratedFuture, typename T>
struct parallel_for_each_ret<
  ErroratedFuture<
    ::crimson::errorated_future_marker<T>>> {
  using type = ErroratedFuture<::crimson::errorated_future_marker<void>>;
};

template <typename InterruptCond, typename FutureType>
class parallel_for_each_state final : private seastar::continuation_base<> {
  using elem_ret_t = std::conditional_t<
    is_interruptible_future<FutureType>::value,
    typename FutureType::core_type,
    FutureType>;
  using future_t = interruptible_future_detail<
    InterruptCond,
    typename parallel_for_each_ret<elem_ret_t>::type>;
  std::vector<future_t> _incomplete;
  seastar::promise<> _result;
  std::exception_ptr _ex;
private:
  void wait_for_one() noexcept {
    while (!_incomplete.empty() && _incomplete.back().available()) {
      if (_incomplete.back().failed()) {
	_ex = _incomplete.back().get_exception();
      }
      _incomplete.pop_back();
    }
    if (!_incomplete.empty()) {
      seastar::internal::set_callback(_incomplete.back(), static_cast<continuation_base<>*>(this));
      _incomplete.pop_back();
      return;
    }
    if (__builtin_expect(bool(_ex), false)) {
      _result.set_exception(std::move(_ex));
    } else {
      _result.set_value();
    }
    delete this;
  }
  virtual void run_and_dispose() noexcept override {
    if (_state.failed()) {
      _ex = std::move(_state).get_exception();
    }
    _state = {};
    wait_for_one();
  }
  task* waiting_task() noexcept override { return _result.waiting_task(); }
public:
  parallel_for_each_state(size_t n) {
    _incomplete.reserve(n);
  }
  void add_future(future_t&& f) {
    _incomplete.push_back(std::move(f));
  }
  future_t get_future() {
    auto ret = _result.get_future();
    wait_for_one();
    return ret;
  }
  static future_t now() {
    return seastar::now();
  }
};

template <typename InterruptCond, typename T>
class [[nodiscard]] interruptible_future_detail<InterruptCond, seastar::future<T>>
  : private seastar::future<T> {
public:
  using core_type = seastar::future<T>;
  template <typename U>
  using interrupt_futurize_t =
    typename interruptor<InterruptCond>::template futurize_t<U>;
  using core_type::get0;
  using core_type::core_type;
  using core_type::get_exception;
  using core_type::ignore_ready_future;

  [[gnu::always_inline]]
  interruptible_future_detail(seastar::future<T>&& base)
    : core_type(std::move(base))
  {}

  using value_type = typename seastar::future<T>::value_type;
  using tuple_type = typename seastar::future<T>::tuple_type;

  [[gnu::always_inline]]
  value_type&& get() {
    if (core_type::available()) {
      return core_type::get();
    } else {
      // destined to wait!
      auto interruption_condition = interrupt_cond<InterruptCond>;
      auto&& value = core_type::get();
      interrupt_cond<InterruptCond> = interruption_condition;
      return std::move(value);
    }
  }

  using core_type::available;
  using core_type::failed;

  template <typename Func,
	    typename Result = interrupt_futurize_t<
		std::invoke_result_t<Func, seastar::future<T>>>>
  [[gnu::always_inline]]
  Result then_wrapped_interruptible(Func&& func) {
    assert(interrupt_cond<InterruptCond>);
    return core_type::then_wrapped(
      [func=std::move(func), interrupt_condition=interrupt_cond<InterruptCond>]
      (auto&& fut) mutable {
      return call_with_interruption(
		std::move(interrupt_condition),
		std::forward<Func>(func),
		std::move(fut));
    });
  }

  template <typename Func>
  [[gnu::always_inline]]
  auto then_interruptible(Func&& func) {
    assert(interrupt_cond<InterruptCond>);
    if constexpr (std::is_void_v<T>) {
      auto fut = core_type::then(
	[func=std::move(func), interrupt_condition=interrupt_cond<InterruptCond>]
	() mutable {
	return call_with_interruption(
		  interrupt_condition,
		  std::move(func));
      });
      return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
    } else {
      auto fut = core_type::then(
	[func=std::move(func), interrupt_condition=interrupt_cond<InterruptCond>]
	(T&& arg) mutable {
	return call_with_interruption(
		  interrupt_condition,
		  std::move(func),
		  std::forward<T>(arg));
      });
      return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
    }
  }

  template <typename Func>
  [[gnu::always_inline]]
  auto then_unpack_interruptible(Func&& func) {
    return then_interruptible([func=std::forward<Func>(func)](T&& tuple) mutable {
      return std::apply(std::forward<Func>(func), std::move(tuple));
    });
  }

  template <typename Func,
	    typename Result =interrupt_futurize_t<
		std::result_of_t<Func(std::exception_ptr)>>>
  [[gnu::always_inline]]
  Result handle_exception_interruptible(Func&& func) {
    assert(interrupt_cond<InterruptCond>);
    return core_type::then_wrapped(
      [func=std::forward<Func>(func),
      interrupt_condition=interrupt_cond<InterruptCond>](auto&& fut) mutable {
      if (!fut.failed()) {
	return seastar::make_ready_future<T>(fut.get());
      } else {
	return call_with_interruption(
		  interrupt_condition,
		  std::move(func),
		  fut.get_exception());
      }
    });
  }

  template <bool may_interrupt = true, typename Func,
	    typename Result = interrupt_futurize_t<
		std::result_of_t<Func()>>>
  [[gnu::always_inline]]
  Result finally_interruptible(Func&& func) {
    if constexpr (may_interrupt) {
      assert(interrupt_cond<InterruptCond>);
      return core_type::then_wrapped(
	[func=std::forward<Func>(func),
	interrupt_condition=interrupt_cond<InterruptCond>](auto&& fut) mutable {
	  return call_with_interruption(
		    interrupt_condition,
		    std::move(func));
      });
    } else {
      return core_type::finally(std::forward<Func>(func));
    }
  }

  template <typename Func,
	    typename Result = interrupt_futurize_t<
		std::result_of_t<Func(
		  typename seastar::function_traits<Func>::template arg<0>::type)>>>
  [[gnu::always_inline]]
  Result handle_exception_type_interruptible(Func&& func) {
    assert(interrupt_cond<InterruptCond>);
    using trait = seastar::function_traits<Func>;
    static_assert(trait::arity == 1, "func can take only one parameter");
    using ex_type = typename trait::template arg<0>::type;
    return core_type::then_wrapped(
      [func=std::forward<Func>(func),
      interrupt_condition=interrupt_cond<InterruptCond>](auto&& fut) mutable 
      -> Result {
      if (!fut.failed()) {
	return seastar::make_ready_future<T>(fut.get());
      } else {
	try {
	  std::rethrow_exception(fut.get_exception());
	} catch (ex_type& ex) {
	  return call_with_interruption(
		    interrupt_condition,
		    std::move(func), ex);
	}
      }
    });
  }


  using my_type = interruptible_future_detail<InterruptCond, seastar::future<T>>;

  template <typename Func>
  [[gnu::always_inline]]
  my_type finally(Func&& func) {
    return core_type::finally(std::forward<Func>(func));
  }
private:
  template <typename Func>
  [[gnu::always_inline]]
  auto handle_interruption(Func&& func) {
    return core_type::then_wrapped(
      [func=std::move(func)](auto&& fut) mutable {
	if (fut.failed()) {
	  std::exception_ptr ex = fut.get_exception();
	  if (InterruptCond::is_interruption(ex)) {
	    return seastar::futurize_invoke(std::move(func), std::move(ex));
	  } else {
	    return seastar::make_exception_future<T>(std::move(ex));
	  }
	} else {
	  return seastar::make_ready_future<T>(fut.get());
	}
      });
  }

  seastar::future<T> to_future() {
    return static_cast<core_type&&>(std::move(*this));
  }
  // this is only supposed to be invoked by seastar functions
  template <typename Func,
	    typename Result = interrupt_futurize_t<
		std::result_of_t<Func(seastar::future<T>)>>>
  [[gnu::always_inline]]
  Result then_wrapped(Func&& func) {
    return core_type::then_wrapped(
      [func=std::move(func), interrupt_condition=interrupt_cond<InterruptCond>]
      (auto&& fut) mutable {
      return call_with_interruption(
		interrupt_condition,
		std::forward<Func>(func),
		std::move(fut));
    });
  }
  friend interruptor<InterruptCond>;
  friend class interruptible_future_builder<InterruptCond>;
  template <typename U>
  friend struct ::seastar::futurize;
  template <typename>
  friend class ::seastar::future;
  template <typename HeldState, typename Future>
  friend class seastar::internal::do_with_state;
  template<typename TX, typename F>
  friend inline auto ::seastar::internal::do_with_impl(TX&& rvalue, F&& f);
  template<typename T1, typename T2, typename T3_or_F, typename... More>
  friend inline auto ::seastar::internal::do_with_impl(T1&& rv1, T2&& rv2, T3_or_F&& rv3, More&&... more);
  template <typename T1, typename T2, typename... More>
  friend auto seastar::internal::do_with_impl(T1&& rv1, T2&& rv2, More&&... more);
  template <typename, typename>
  friend class ::crimson::maybe_handle_error_t;
  template <typename>
  friend class ::seastar::internal::extract_values_from_futures_vector;
  template <typename, typename>
  friend class interruptible_future_detail;
  template <typename ResolvedVectorTransform, typename Future>
  friend inline typename ResolvedVectorTransform::future_type
  seastar::internal::complete_when_all(
      std::vector<Future>&& futures,
      typename std::vector<Future>::iterator pos) noexcept;
  template <typename>
  friend class ::seastar::internal::when_all_state_component;
  template <typename Lock, typename Func>
  friend inline auto seastar::with_lock(Lock& lock, Func&& f);
  template <typename IC, typename FT>
  friend class parallel_for_each_state;
};

template <typename InterruptCond, typename Errorator>
struct interruptible_errorator {
  using base_ertr = Errorator;

  template <typename ValueT = void>
  using future = interruptible_future_detail<InterruptCond,
	typename Errorator::template future<ValueT>>;

  template <class... NewAllowedErrorsT>
  using extend = interruptible_errorator<
    InterruptCond,
    typename Errorator::template extend<NewAllowedErrorsT...>>;

  template <class Ertr>
  using extend_ertr = interruptible_errorator<
    InterruptCond,
    typename Errorator::template extend_ertr<Ertr>>;

  template <typename ValueT = void, typename... A>
  static interruptible_future_detail<
    InterruptCond,
    typename Errorator::template future<ValueT>>
  make_ready_future(A&&... value) {
    return interruptible_future_detail<
      InterruptCond, typename Errorator::template future<ValueT>>(
	Errorator::template make_ready_future<ValueT>(
	  std::forward<A>(value)...));
  }
  static interruptible_future_detail<
    InterruptCond,
    typename Errorator::template future<>> now() {
    return interruptible_future_detail<
      InterruptCond, typename Errorator::template future<>>(
	Errorator::now());
  }

  using pass_further = typename Errorator::pass_further;
};

template <typename InterruptCond,
	  template <typename...> typename ErroratedFuture,
	  typename T>
class [[nodiscard]] interruptible_future_detail<
  InterruptCond,
  ErroratedFuture<::crimson::errorated_future_marker<T>>>
  : private ErroratedFuture<::crimson::errorated_future_marker<T>>
{
public:
  using core_type = ErroratedFuture<crimson::errorated_future_marker<T>>;
  using errorator_type = typename core_type::errorator_type;

  template <typename U>
  using interrupt_futurize_t =
    typename interruptor<InterruptCond>::template futurize_t<U>;

  using core_type::available;
  using core_type::failed;
  using core_type::core_type;
  using core_type::get_exception;

  using value_type = typename core_type::value_type;

  interruptible_future_detail(seastar::future<T>&& fut)
    : core_type(std::move(fut))
  {}

  template <template <typename...> typename ErroratedFuture2,
	    typename... U>
  [[gnu::always_inline]]
  interruptible_future_detail(
    ErroratedFuture2<::crimson::errorated_future_marker<U...>>&& fut)
    : core_type(std::move(fut)) {}

  template <template <typename...> typename ErroratedFuture2,
	    typename... U>
  [[gnu::always_inline]]
  interruptible_future_detail(
    interruptible_future_detail<InterruptCond,
      ErroratedFuture2<::crimson::errorated_future_marker<U...>>>&& fut)
    : core_type(static_cast<typename std::decay_t<decltype(fut)>::core_type&&>(fut)) {
    using src_errorator_t = \
      typename ErroratedFuture2<
	::crimson::errorated_future_marker<U...>>::errorator_type;
    static_assert(core_type::errorator_type::template contains_once_v<
		    src_errorator_t>,
		  "conversion is only possible from less-or-eq errorated future!");
  }

  [[gnu::always_inline]]
  interruptible_future_detail(
    interruptible_future_detail<InterruptCond, seastar::future<T>>&& fut)
    : core_type(static_cast<seastar::future<T>&&>(fut)) {}

  template <class... A>
  [[gnu::always_inline]]
  interruptible_future_detail(ready_future_marker, A&&... a)
    : core_type(::seastar::make_ready_future<typename core_type::value_type>(
		  std::forward<A>(a)...)) {
  }
  [[gnu::always_inline]]
  interruptible_future_detail(exception_future_marker, ::seastar::future_state_base&& state) noexcept
    : core_type(::seastar::futurize<core_type>::make_exception_future(std::move(state))) {
  }
  [[gnu::always_inline]]
  interruptible_future_detail(exception_future_marker, std::exception_ptr&& ep) noexcept
    : core_type(::seastar::futurize<core_type>::make_exception_future(std::move(ep))) {
  }

  template<bool interruptible = true, typename ValueInterruptCondT, typename ErrorVisitorT,
	   std::enable_if_t<!interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc, ErrorVisitorT&& errfunc) {
    auto fut = core_type::safe_then(
	std::forward<ValueInterruptCondT>(valfunc),
	std::forward<ErrorVisitorT>(errfunc));
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template <typename... Args>
  auto si_then(Args&&... args) {
    return safe_then_interruptible(std::forward<Args>(args)...);
  }


  template<bool interruptible = true, typename ValueInterruptCondT, typename ErrorVisitorT,
	   typename U = T, std::enable_if_t<!std::is_void_v<U> && interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc, ErrorVisitorT&& errfunc) {
    assert(interrupt_cond<InterruptCond>);
    auto fut = core_type::safe_then(
      [func=std::move(valfunc), interrupt_condition=interrupt_cond<InterruptCond>]
      (T&& args) mutable {
      return call_with_interruption(
		interrupt_condition,
		std::move(func),
		std::forward<T>(args));
      }, [func=std::move(errfunc),
	  interrupt_condition=interrupt_cond<InterruptCond>]
	  (auto&& err) mutable -> decltype(auto) {
	  constexpr bool return_void = std::is_void_v<
	    std::invoke_result_t<ErrorVisitorT,
	      std::decay_t<decltype(err)>>>;
	  constexpr bool return_err = ::crimson::is_error_v<
	    std::decay_t<std::invoke_result_t<ErrorVisitorT,
	      std::decay_t<decltype(err)>>>>;
	  if constexpr (return_err || return_void) {
	    return non_futurized_call_with_interruption(
		      interrupt_condition,
		      std::move(func),
		      std::move(err));
	  } else {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(func),
		      std::move(err));
	  }
      });
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template<bool interruptible = true, typename ValueInterruptCondT, typename ErrorVisitorT,
	   typename U = T, std::enable_if_t<std::is_void_v<U> && interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc, ErrorVisitorT&& errfunc) {
    assert(interrupt_cond<InterruptCond>);
    auto fut = core_type::safe_then(
      [func=std::move(valfunc), interrupt_condition=interrupt_cond<InterruptCond>]
      () mutable {
      return call_with_interruption(
		interrupt_condition,
		std::move(func));
      }, [func=std::move(errfunc),
	  interrupt_condition=interrupt_cond<InterruptCond>]
	  (auto&& err) mutable -> decltype(auto) {
	  constexpr bool return_void = std::is_void_v<
	    std::invoke_result_t<ErrorVisitorT,
	      std::decay_t<decltype(err)>>>;
	  constexpr bool return_err = ::crimson::is_error_v<
	    std::decay_t<std::invoke_result_t<ErrorVisitorT,
	      std::decay_t<decltype(err)>>>>;
	  if constexpr (return_err || return_void) {
	    return non_futurized_call_with_interruption(
		      interrupt_condition,
		      std::move(func),
		      std::move(err));
	  } else {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(func),
		      std::move(err));
	  }
      });
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template <bool interruptible = true, typename ValueInterruptCondT,
	    typename U = T, std::enable_if_t<std::is_void_v<T> && interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc) {
    assert(interrupt_cond<InterruptCond>);
    auto fut = core_type::safe_then(
      [func=std::move(valfunc),
       interrupt_condition=interrupt_cond<InterruptCond>]
      () mutable {
      return call_with_interruption(
		interrupt_condition,
		std::move(func));
    });
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template <typename ValFuncT, typename ErrorFuncT>
  [[gnu::always_inline]]
  auto safe_then_unpack_interruptible(ValFuncT&& func, ErrorFuncT&& errfunc) {
    return safe_then_interruptible([func=std::forward<ValFuncT>(func)](T&& tuple) mutable {
      return std::apply(std::forward<ValFuncT>(func), std::move(tuple));
    }, std::forward<ErrorFuncT>(errfunc));
  }

  template <typename ValFuncT>
  [[gnu::always_inline]]
  auto safe_then_unpack_interruptible(ValFuncT&& func) {
    return safe_then_interruptible([func=std::forward<ValFuncT>(func)](T&& tuple) mutable {
      return std::apply(std::forward<ValFuncT>(func), std::move(tuple));
    });
  }

  template <bool interruptible = true, typename ValueInterruptCondT,
	    typename U = T, std::enable_if_t<!std::is_void_v<T> && interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc) {
    assert(interrupt_cond<InterruptCond>);
    auto fut = core_type::safe_then(
      [func=std::move(valfunc),
       interrupt_condition=interrupt_cond<InterruptCond>]
      (T&& arg) mutable {
      return call_with_interruption(
		interrupt_condition,
		std::move(func),
		std::forward<T>(arg));
    });
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template <bool interruptible = true, typename ValueInterruptCondT,
	    std::enable_if_t<!interruptible, int> = 0>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc) {
    auto fut = core_type::safe_then(std::forward<ValueInterruptCondT>(valfunc));
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

  template <typename ValueInterruptCondT,
	    typename ErrorVisitorHeadT,
	    typename... ErrorVisitorTailT>
  [[gnu::always_inline]]
  auto safe_then_interruptible(ValueInterruptCondT&& valfunc,
			       ErrorVisitorHeadT&& err_func_head,
			       ErrorVisitorTailT&&... err_func_tail) {
    return safe_then_interruptible(
	std::forward<ValueInterruptCondT>(valfunc),
	::crimson::composer(std::forward<ErrorVisitorHeadT>(err_func_head),
			    std::forward<ErrorVisitorTailT>(err_func_tail)...));
  }

  template <typename ValueInterruptCondT,
	    typename ErrorVisitorHeadT,
	    typename... ErrorVisitorTailT>
  [[gnu::always_inline]]
  auto safe_then_interruptible_tuple(ValueInterruptCondT&& valfunc,
			       ErrorVisitorHeadT&& err_func_head,
			       ErrorVisitorTailT&&... err_func_tail) {
    return safe_then_interruptible(
	std::forward<ValueInterruptCondT>(valfunc),
	::crimson::composer(std::forward<ErrorVisitorHeadT>(err_func_head),
			    std::forward<ErrorVisitorTailT>(err_func_tail)...));
  }

  template <typename ValFuncT,
	    typename ErrorVisitorHeadT,
	    typename... ErrorVisitorTailT>
  [[gnu::always_inline]]
  auto safe_then_unpack_interruptible_tuple(
      ValFuncT&& valfunc,
      ErrorVisitorHeadT&& err_func_head,
      ErrorVisitorTailT&&... err_func_tail) {
    return safe_then_interruptible_tuple(
      [valfunc=std::forward<ValFuncT>(valfunc)](T&& tuple) mutable {
      return std::apply(std::forward<ValFuncT>(valfunc), std::move(tuple));
    },
    ::crimson::composer(std::forward<ErrorVisitorHeadT>(err_func_head),
			std::forward<ErrorVisitorTailT>(err_func_tail)...));
  }

  template <bool interruptible = true, typename ErrorFunc>
  auto handle_error_interruptible(ErrorFunc&& errfunc) {
    if constexpr (interruptible) {
      assert(interrupt_cond<InterruptCond>);
      auto fut = core_type::handle_error(
	[errfunc=std::move(errfunc),
	 interrupt_condition=interrupt_cond<InterruptCond>]
	(auto&& err) mutable -> decltype(auto) {
	  constexpr bool return_void = std::is_void_v<
	    std::invoke_result_t<ErrorFunc,
	      std::decay_t<decltype(err)>>>;
	  constexpr bool return_err = ::crimson::is_error_v<
	    std::decay_t<std::invoke_result_t<ErrorFunc,
	      std::decay_t<decltype(err)>>>>;
	  if constexpr (return_err || return_void) {
	    return non_futurized_call_with_interruption(
		      interrupt_condition,
		      std::move(errfunc),
		      std::move(err));
	  } else {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(errfunc),
		      std::move(err));
	  }
	});
      return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
    } else {
      return core_type::handle_error(std::forward<ErrorFunc>(errfunc));
    }
  }

  template <typename ErrorFuncHead,
	    typename... ErrorFuncTail>
  auto handle_error_interruptible(ErrorFuncHead&& error_func_head,
				  ErrorFuncTail&&... error_func_tail) {
    assert(interrupt_cond<InterruptCond>);
    static_assert(sizeof...(ErrorFuncTail) > 0);
    return this->handle_error_interruptible(
      ::crimson::composer(
	std::forward<ErrorFuncHead>(error_func_head),
	std::forward<ErrorFuncTail>(error_func_tail)...));
  }

  template <typename Func>
  [[gnu::always_inline]]
  auto finally(Func&& func) {
    auto fut = core_type::finally(std::forward<Func>(func));
    return (interrupt_futurize_t<decltype(fut)>)(std::move(fut));
  }

private:
  using core_type::_then;
  template <typename Func>
  [[gnu::always_inline]]
  auto handle_interruption(Func&& func) {
    // see errorator.h safe_then definition
    using func_result_t =
      typename std::invoke_result<Func, std::exception_ptr>::type;
    using func_ertr_t =
      typename core_type::template get_errorator_t<func_result_t>;
    using this_ertr_t = typename core_type::errorator_type;
    using ret_ertr_t = typename this_ertr_t::template extend_ertr<func_ertr_t>;
    using futurator_t = typename ret_ertr_t::template futurize<func_result_t>;
    return core_type::then_wrapped(
      [func=std::move(func),
       interrupt_condition=interrupt_cond<InterruptCond>](auto&& fut) mutable
      -> typename futurator_t::type {
	if (fut.failed()) {
	  std::exception_ptr ex = fut.get_exception();
	  if (InterruptCond::is_interruption(ex)) {
	    return futurator_t::invoke(std::move(func), std::move(ex));
	  } else {
	    return futurator_t::make_exception_future(std::move(ex));
	  }
	} else {
	  return std::move(fut);
	}
      });
  }

  ErroratedFuture<::crimson::errorated_future_marker<T>>
  to_future() {
    return static_cast<core_type&&>(std::move(*this));
  }

  friend class interruptor<InterruptCond>;
  friend class interruptible_future_builder<InterruptCond>;
  template <typename U>
  friend struct ::seastar::futurize;
  template <typename>
  friend class ::seastar::future;
  template<typename TX, typename F>
  friend inline auto ::seastar::internal::do_with_impl(TX&& rvalue, F&& f);
  template<typename T1, typename T2, typename T3_or_F, typename... More>
  friend inline auto ::seastar::internal::do_with_impl(T1&& rv1, T2&& rv2, T3_or_F&& rv3, More&&... more);
  template <typename T1, typename T2, typename... More>
  friend auto seastar::internal::do_with_impl(T1&& rv1, T2&& rv2, More&&... more);
  template <typename HeldState, typename Future>
  friend class seastar::internal::do_with_state;
  template <typename, typename>
  friend class ::crimson::maybe_handle_error_t;
  template <typename, typename>
  friend class interruptible_future_detail;
  template <typename Lock, typename Func>
  friend inline auto seastar::with_lock(Lock& lock, Func&& f);
  template <typename IC, typename FT>
  friend class parallel_for_each_state;
};

template <typename InterruptCond, typename T = void>
using interruptible_future =
  interruptible_future_detail<InterruptCond, seastar::future<T>>;

template <typename InterruptCond, typename Errorator, typename T = void>
using interruptible_errorated_future =
  interruptible_future_detail<
    InterruptCond,
    typename Errorator::template future<T>>;

template <typename InterruptCond>
struct interruptor
{
public:
  template <typename T>
  using future = interruptible_future<InterruptCond, T>;

  template <typename FutureType>
  [[gnu::always_inline]]
  static interruptible_future_detail<InterruptCond, FutureType>
  make_interruptible(FutureType&& fut) {
    return interruptible_future_detail<InterruptCond, FutureType>(std::move(fut));
  }

  [[gnu::always_inline]]
  static interruptible_future_detail<InterruptCond, seastar::future<>> now() {
    return interruptible_future_detail<
	      InterruptCond,
	      seastar::future<>>(seastar::now());
  }

  template <typename ValueT = void, typename... A>
  [[gnu::always_inline]]
  static interruptible_future_detail<InterruptCond, seastar::future<ValueT>>
  make_ready_future(A&&... value) {
    return interruptible_future_detail<InterruptCond, seastar::future<ValueT>>(
	seastar::make_ready_future<ValueT>(std::forward<A>(value)...));
  }

  template <typename T>
  struct futurize {
    using type = interruptible_future_detail<
      InterruptCond, typename seastar::futurize<T>::type>;

    template <typename Func, typename... Args>
    static type apply(Func&& func, std::tuple<Args...>&& args) noexcept {
      return seastar::futurize<T>::apply(std::forward<Func>(func),
					 std::forward<std::tuple<Args...>>(args));
    }

    template <typename Func, typename... Args>
    static type invoke(Func&& func, Args&&... args) noexcept {
      return seastar::futurize<T>::invoke(
	std::forward<Func>(func),
	std::forward<Args>(args)...);
    }
  };

  template <typename FutureType>
  struct futurize<interruptible_future_detail<InterruptCond, FutureType>> {
    using type = interruptible_future_detail<InterruptCond, FutureType>;

    template <typename Func, typename... Args>
    static type apply(Func&& func, std::tuple<Args...>&& args) noexcept {
      return seastar::futurize<FutureType>::apply(
	  std::forward<Func>(func),
	  std::forward<std::tuple<Args...>>(args));
    }

    template <typename Func, typename... Args>
    static type invoke(Func&& func, Args&&... args) noexcept {
      return seastar::futurize<FutureType>::invoke(
	  std::forward<Func>(func),
	  std::forward<Args>(args)...);
    }
  };

  template <typename T>
  using futurize_t = typename futurize<T>::type;

  template <typename Func, typename... Args>
  static auto futurize_apply(Func&& func, std::tuple<Args...>&& args) noexcept {
    using futurator = futurize<std::result_of_t<Func(Args&&...)>>;
    return futurator::apply(std::forward<Func>(func), std::move(args));
  }

  template <typename Container, typename AsyncAction>
  [[gnu::always_inline]]
  static auto do_for_each(Container& c, AsyncAction&& action) {
    return do_for_each(std::begin(c), std::end(c),
	      std::forward<AsyncAction>(action));
  }

  template <typename OpFunc, typename OnInterrupt,
	    typename... Params>
  static inline auto with_interruption_cond(
    OpFunc&& opfunc, OnInterrupt&& efunc, InterruptCond &&cond, Params&&... params) {
    return internal::call_with_interruption_impl(
      seastar::make_lw_shared<InterruptCond>(std::move(cond)),
      std::forward<OpFunc>(opfunc),
      std::forward<Params>(params)...
    ).template handle_interruption(std::move(efunc));
  }

  template <typename OpFunc, typename OnInterrupt,
	    typename... InterruptCondParams>
  static inline auto with_interruption(
    OpFunc&& opfunc, OnInterrupt&& efunc, InterruptCondParams&&... params) {
    return with_interruption_cond(
      std::forward<OpFunc>(opfunc),
      std::forward<OnInterrupt>(efunc),
      InterruptCond(std::forward<InterruptCondParams>(params)...));
  }

  template <typename Error,
	    typename Func,
	    typename... Params>
  static inline auto with_interruption_to_error(
    Func &&f, InterruptCond &&cond, Params&&... params) {
    using func_result_t = std::invoke_result_t<Func, Params...>;
    using func_ertr_t =
      typename seastar::template futurize<
	func_result_t>::core_type::errorator_type;
    using with_trans_ertr =
      typename func_ertr_t::template extend_ertr<errorator<Error>>;

    using value_type = typename func_result_t::value_type;
    using ftype = typename std::conditional_t<
      std::is_same_v<value_type, seastar::internal::monostate>,
      typename with_trans_ertr::template future<>,
      typename with_trans_ertr::template future<value_type>>;

    return with_interruption_cond(
      std::forward<Func>(f),
      [](auto e) -> ftype {
	return Error::make();
      },
      std::forward<InterruptCond>(cond),
      std::forward<Params>(params)...);
  }

  template <typename Func>
  [[gnu::always_inline]]
  static auto wrap_function(Func&& func) {
    return [func=std::forward<Func>(func),
	    interrupt_condition=interrupt_cond<InterruptCond>]() mutable {
	      return call_with_interruption(
		  interrupt_condition,
		  std::forward<Func>(func));
	    };
  }

  template <typename Iterator, typename AsyncAction,
	    typename Result = std::invoke_result_t<AsyncAction, typename Iterator::reference>,
	    std::enable_if_t<is_interruptible_future<Result>::value, int> = 0>
  [[gnu::always_inline]]
  static auto do_for_each(Iterator begin, Iterator end, AsyncAction&& action) {
    if constexpr (seastar::is_future<typename Result::core_type>::value) {
      return make_interruptible(
	  ::seastar::do_for_each(begin, end,
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>]
	    (typename Iterator::reference x) mutable {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action),
		      std::forward<decltype(*begin)>(x)).to_future();
	  })
      );
    } else {
      return make_interruptible(
	  ::crimson::do_for_each(begin, end,
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>]
	    (typename Iterator::reference x) mutable {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action),
		      std::forward<decltype(*begin)>(x)).to_future();
	  })
      );
    }
  }

  template <typename Iterator, typename AsyncAction,
	    typename Result = std::invoke_result_t<AsyncAction, typename Iterator::reference>,
	    std::enable_if_t<!is_interruptible_future<Result>::value, int> = 0>
  [[gnu::always_inline]]
  static auto do_for_each(Iterator begin, Iterator end, AsyncAction&& action) {
    if constexpr (seastar::is_future<Result>::value) {
      return make_interruptible(
	  ::seastar::do_for_each(begin, end,
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>]
	    (typename Iterator::reference x) mutable {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action),
		      std::forward<decltype(*begin)>(x));
	  })
      );
    } else {
      return make_interruptible(
	  ::crimson::do_for_each(begin, end,
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>]
	    (typename Iterator::reference x) mutable {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action),
		      std::forward<decltype(*begin)>(x));
	  })
      );
    }
  }

  template <typename AsyncAction,
	    typename Result = std::invoke_result_t<AsyncAction>,
	    std::enable_if_t<is_interruptible_future<Result>::value, int> = 0>
  [[gnu::always_inline]]
  static auto repeat(AsyncAction&& action) {
    if constexpr (seastar::is_future<typename Result::core_type>::value) {
      return make_interruptible(
	  ::seastar::repeat(
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>] {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action)).to_future();
	  })
      );
    } else {
      return make_interruptible(
	  ::crimson::repeat(
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>] {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action)).to_future();
	  })
      );
    }
  }
  template <typename AsyncAction,
	    typename Result = std::invoke_result_t<AsyncAction>,
	    std::enable_if_t<
	      !is_interruptible_future<Result>::value, int> = 0>
  [[gnu::always_inline]]
  static auto repeat(AsyncAction&& action) {
    if constexpr (seastar::is_future<Result>::value) {
      return make_interruptible(
	  ::seastar::repeat(
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>] {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action));
	  })
      );
    } else {
      return make_interruptible(
	  ::crimson::repeat(
	    [action=std::move(action),
	    interrupt_condition=interrupt_cond<InterruptCond>] {
	    return call_with_interruption(
		      interrupt_condition,
		      std::move(action));
	  })
      );
    }
  }

  template <typename Iterator, typename Func>
  static inline auto parallel_for_each(
    Iterator begin,
    Iterator end,
    Func&& func
  ) noexcept {
    using ResultType = std::invoke_result_t<Func, typename Iterator::reference>;
    parallel_for_each_state<InterruptCond, ResultType>* s = nullptr;
    auto decorated_func =
      [func=std::forward<Func>(func),
      interrupt_condition=interrupt_cond<InterruptCond>]
      (decltype(*Iterator())&& x) mutable {
	return call_with_interruption(
		  interrupt_condition,
		  std::forward<Func>(func),
		  std::forward<decltype(*begin)>(x));
      };
    // Process all elements, giving each future the following treatment:
    //   - available, not failed: do nothing
    //   - available, failed: collect exception in ex
    //   - not available: collect in s (allocating it if needed)
    while (begin != end) {
      auto f = seastar::futurize_invoke(decorated_func, *begin++);
      if (!f.available() || f.failed()) {
	if (!s) {
	  using itraits = std::iterator_traits<Iterator>;
	  auto n = (seastar::internal::iterator_range_estimate_vector_capacity(
		begin, end, typename itraits::iterator_category()) + 1);
	  s = new parallel_for_each_state<InterruptCond, ResultType>(n);
	}
	s->add_future(std::move(f));
      }
    }
    // If any futures were not available, hand off to parallel_for_each_state::start().
    // Otherwise we can return a result immediately.
    if (s) {
      // s->get_future() takes ownership of s (and chains it to one of the futures it contains)
      // so this isn't a leak
      return s->get_future();
    }
    return parallel_for_each_state<InterruptCond, ResultType>::now();
  }

  template <typename Container, typename Func>
  static inline auto parallel_for_each(Container&& container, Func&& func) noexcept {
    return parallel_for_each(
	    std::begin(container),
	    std::end(container),
	    std::forward<Func>(func));
  }

  template <typename Iterator, typename Mapper, typename Initial, typename Reduce>
  static inline interruptible_future<InterruptCond, Initial> map_reduce(
    Iterator begin, Iterator end, Mapper&& mapper, Initial initial, Reduce&& reduce) {
    struct state {
      Initial result;
      Reduce reduce;
    };
    auto s = seastar::make_lw_shared(state{std::move(initial), std::move(reduce)});
    interruptible_future<InterruptCond> ret = seastar::make_ready_future<>();
    while (begin != end) {
        ret = seastar::futurize_invoke(mapper, *begin++).then_wrapped_interruptible(
	    [s = s.get(), ret = std::move(ret)] (auto f) mutable {
            try {
                s->result = s->reduce(std::move(s->result), std::move(f.get0()));
                return std::move(ret);
            } catch (...) {
                return std::move(ret).then_wrapped_interruptible([ex = std::current_exception()] (auto f) {
                    f.ignore_ready_future();
                    return seastar::make_exception_future<>(ex);
                });
            }
        });
    }
    return ret.then_interruptible([s] {
        return seastar::make_ready_future<Initial>(std::move(s->result));
    });
  }
  template <typename Range, typename Mapper, typename Initial, typename Reduce>
  static inline interruptible_future<InterruptCond, Initial> map_reduce(
    Range&& range, Mapper&& mapper, Initial initial, Reduce&& reduce) {
    return map_reduce(std::begin(range), std::end(range), std::forward<Mapper>(mapper),
		      std::move(initial), std::move(reduce));
  }

  template<typename Fut, 
	   std::enable_if_t<
	     seastar::is_future<Fut>::value
	     || is_interruptible_future<Fut>::value, int> = 0>
  static auto futurize_invoke_if_func(Fut&& fut) noexcept {
	return std::forward<Fut>(fut);
  }

  template<typename Func,
	   std::enable_if_t<
	     !seastar::is_future<Func>::value
	     && !is_interruptible_future<Func>::value, int> = 0>
  static auto futurize_invoke_if_func(Func&& func) noexcept {
	return seastar::futurize_invoke(std::forward<Func>(func));
  }

  template <typename... FutOrFuncs>
  static inline auto when_all(FutOrFuncs&&... fut_or_funcs) noexcept {
    return ::seastar::internal::when_all_impl(
	futurize_invoke_if_func(std::forward<FutOrFuncs>(fut_or_funcs))...);
  }

  template <typename... FutOrFuncs>
  static inline auto when_all_succeed(FutOrFuncs&&... fut_or_funcs) noexcept {
    return ::seastar::internal::when_all_succeed_impl(
	futurize_invoke_if_func(std::forward<FutOrFuncs>(fut_or_funcs))...);
  }

  template <typename Func,
	    typename Result = futurize_t<std::invoke_result_t<Func>>>
  static inline Result async(Func&& func) {
    return seastar::async([func=std::forward<Func>(func),
			   interrupt_condition=interrupt_cond<InterruptCond>]() mutable {
      return non_futurized_call_with_interruption(
	  interrupt_condition, std::forward<Func>(func));
    });
  }

  static void yield() {
    assert(interrupt_cond<InterruptCond>);
    auto interruption_condition = interrupt_cond<InterruptCond>;
    interrupt_cond<InterruptCond>.release();
    seastar::thread::yield();
    interrupt_cond<InterruptCond> = interruption_condition;
  }

  static void maybe_yield() {
    assert(interrupt_cond<InterruptCond>);
    if (seastar::thread::should_yield()) {
      auto interruption_condition = interrupt_cond<InterruptCond>;
      interrupt_cond<InterruptCond>.release();
      seastar::thread::yield();
      interrupt_cond<InterruptCond> = interruption_condition;
    }
  }
};

} // namespace crimson::interruptible

namespace seastar {

template <typename InterruptCond, typename... T>
struct futurize<::crimson::interruptible::interruptible_future_detail<
  InterruptCond, seastar::future<T...>>> {
  using type = ::crimson::interruptible::interruptible_future_detail<
    InterruptCond, seastar::future<T...>>;

  using value_type = typename type::value_type;
  using tuple_type = typename type::tuple_type;

  static type from_tuple(tuple_type&& value) {
    return type(ready_future_marker(), std::move(value));
  }
  static type from_tuple(const tuple_type& value) {
    return type(ready_future_marker(), value);
  }
  static type from_tuple(value_type&& value) {
    return type(ready_future_marker(), std::move(value));
  }
  static type from_tuple(const value_type& value) {
    return type(ready_future_marker(), value);
  }

  template <typename Func, typename... FuncArgs>
  [[gnu::always_inline]]
  static inline type invoke(Func&& func, FuncArgs&&... args) noexcept {
    try {
      return func(std::forward<FuncArgs>(args)...);
    } catch (...) {
      return make_exception_future(std::current_exception());
    }
  }

  template <typename Func>
  [[gnu::always_inline]]
  static type invoke(Func&& func, seastar::internal::monostate) noexcept {
    try {
      return ::seastar::futurize_invoke(std::forward<Func>(func));
    } catch (...) {
      return make_exception_future(std::current_exception());
    }
  }

  template <typename Arg>
  static inline type make_exception_future(Arg&& arg) noexcept {
    return seastar::make_exception_future<T...>(std::forward<Arg>(arg));
  }

  static inline type make_exception_future(future_state_base&& state) noexcept {
    return seastar::internal::make_exception_future<T...>(std::move(state));
  }

  template<typename PromiseT, typename Func>
  static void satisfy_with_result_of(PromiseT&& pr, Func&& func) {
    func().forward_to(std::move(pr));
  }
};

template <typename InterruptCond,
	  template <typename...> typename ErroratedFuture,
	  typename... T>
struct futurize<
  ::crimson::interruptible::interruptible_future_detail<
    InterruptCond,
    ErroratedFuture<::crimson::errorated_future_marker<T...>>
  >
> {
  using type = ::crimson::interruptible::interruptible_future_detail<
    InterruptCond,
    ErroratedFuture<::crimson::errorated_future_marker<T...>>>;
  using core_type = ErroratedFuture<
      ::crimson::errorated_future_marker<T...>>;
  using errorator_type =
    ::crimson::interruptible::interruptible_errorator<
      InterruptCond,
      typename ErroratedFuture<
	::crimson::errorated_future_marker<T...>>::errorator_type>;

  template<typename Func, typename... FuncArgs>
  static inline type invoke(Func&& func, FuncArgs&&... args) noexcept {
    try {
        return func(std::forward<FuncArgs>(args)...);
    } catch (...) {
        return make_exception_future(std::current_exception());
    }
  }

  template <typename Func>
  [[gnu::always_inline]]
  static type invoke(Func&& func, seastar::internal::monostate) noexcept {
    try {
      return ::seastar::futurize_invoke(std::forward<Func>(func));
    } catch (...) {
      return make_exception_future(std::current_exception());
    }
  }

  template <typename Arg>
  static inline type make_exception_future(Arg&& arg) noexcept {
    return core_type::errorator_type::template make_exception_future2<T...>(
	std::forward<Arg>(arg));
  }

  template<typename PromiseT, typename Func>
  static void satisfy_with_result_of(PromiseT&& pr, Func&& func) {
    func().forward_to(std::move(pr));
  }

};

template <typename InterruptCond, typename FutureType>
struct continuation_base_from_future<
  ::crimson::interruptible::interruptible_future_detail<InterruptCond, FutureType>> {
  using type = typename seastar::continuation_base_from_future<FutureType>::type;
};

} // namespace seastar
