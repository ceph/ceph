// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <system_error>

#include <seastar/core/future-util.hh>

#include "include/ceph_assert.h"

namespace crimson {

template<typename Iterator, typename AsyncAction>
inline auto do_for_each(Iterator begin, Iterator end, AsyncAction action) {
  using futurator = \
    ::seastar::futurize<std::result_of_t<AsyncAction(decltype(*begin))>>;

  if (begin == end) {
    return futurator::type::errorator_type::template make_ready_future<>();
  }
  while (true) {
    auto f = futurator::invoke(action, *begin);
    ++begin;
    if (begin == end) {
      return f;
    }
    if (!f.available() || seastar::need_preempt()) {
      return std::move(f)._then(
        [ action = std::move(action),
          begin = std::move(begin),
          end = std::move(end)
        ] () mutable {
          return ::crimson::do_for_each(std::move(begin),
                                        std::move(end),
                                        std::move(action));
      });
    }
    if (f.failed()) {
      return f;
    }
  }
}
template<typename Container, typename AsyncAction>
inline auto do_for_each(Container& c, AsyncAction action) {
  return ::crimson::do_for_each(std::begin(c), std::end(c), std::move(action));
}

template<typename AsyncAction>
inline auto do_until(AsyncAction action) {
  using futurator = \
    ::seastar::futurize<std::result_of_t<AsyncAction()>>;

  while (true) {
    auto f = futurator::invoke(action);
    if (!seastar::need_preempt() && f.available() && std::get<0>(f.get())) {
      return futurator::type::errorator_type::template make_ready_future<>();
    }
    if (!f.available() || seastar::need_preempt()) {
      return std::move(f)._then(
        [ action = std::move(action)] (auto &&done) mutable {
	  if (done) {
	    return futurator::type::errorator_type::template make_ready_future<>();
	  }
          return ::crimson::do_until(
	    std::move(action));
	});
    }
    if (f.failed()) {
      return futurator::type::errorator_type::template make_exception_future2<>(
	f.get_exception()
      );
    }
  }
}

// define the interface between error types and errorator
template <class ConcreteErrorT>
class error_t {
  static constexpr const std::type_info& get_exception_ptr_type_info() {
    return ConcreteErrorT::exception_ptr_type_info();
  }

  std::exception_ptr to_exception_ptr() const {
    const auto* concrete_error = static_cast<const ConcreteErrorT*>(this);
    return concrete_error->to_exception_ptr();
  }

  decltype(auto) static from_exception_ptr(std::exception_ptr ep) {
    return ConcreteErrorT::from_exception_ptr(std::move(ep));
  }

  template <class... AllowedErrorsT>
  friend struct errorator;

  template <class ErrorVisitorT, class FuturatorT>
  friend class maybe_handle_error_t;

public:
  template <class Func>
  static decltype(auto) handle(Func&& func) {
    return ConcreteErrorT::handle(std::forward<Func>(func));
  }
};

// unthrowable_wrapper ensures compilation failure when somebody
// would like to `throw make_error<...>)()` instead of returning.
// returning allows for the compile-time verification of future's
// AllowedErrorsV and also avoid the burden of throwing.
template <class ErrorT, ErrorT ErrorV>
struct unthrowable_wrapper : error_t<unthrowable_wrapper<ErrorT, ErrorV>> {
  unthrowable_wrapper(const unthrowable_wrapper&) = delete;
  [[nodiscard]] static const auto& make() {
    static constexpr unthrowable_wrapper instance{};
    return instance;
  }

  template<class Func>
  static auto handle(Func&& func) {
    return [
      func = std::forward<Func>(func)
    ] (const unthrowable_wrapper&) mutable -> decltype(auto) {
      if constexpr (std::is_invocable_v<Func, ErrorT>) {
        return std::invoke(std::forward<Func>(func), ErrorV);
      } else {
        return std::invoke(std::forward<Func>(func));
      }
    };
  }

  struct pass_further {
    decltype(auto) operator()(const unthrowable_wrapper& e) {
      return e;
    }
  };

  struct discard {
    decltype(auto) operator()(const unthrowable_wrapper&) {
    }
  };


private:
  // can be used only to initialize the `instance` member
  explicit unthrowable_wrapper() = default;

  // implement the errorable interface
  struct throwable_carrier{};
  static std::exception_ptr carrier_instance;

  static constexpr const std::type_info& exception_ptr_type_info() {
    return typeid(throwable_carrier);
  }
  auto to_exception_ptr() const {
    // error codes don't need to instantiate `std::exception_ptr` each
    // time as the code is actually a part of the type itself.
    // `std::make_exception_ptr()` on modern enough GCCs is quite cheap
    // (see the Gleb Natapov's patch eradicating throw/catch there),
    // but using one instance per type boils down the overhead to just
    // ref-counting.
    return carrier_instance;
  }
  static const auto& from_exception_ptr(std::exception_ptr) {
    return make();
  }

  friend class error_t<unthrowable_wrapper<ErrorT, ErrorV>>;
};

template <class ErrorT, ErrorT ErrorV>
std::exception_ptr unthrowable_wrapper<ErrorT, ErrorV>::carrier_instance = \
  std::make_exception_ptr<
    unthrowable_wrapper<ErrorT, ErrorV>::throwable_carrier>({});


template <class ErrorT>
struct stateful_error_t : error_t<stateful_error_t<ErrorT>> {
  template <class... Args>
  explicit stateful_error_t(Args&&... args)
    : ep(std::make_exception_ptr<ErrorT>(std::forward<Args>(args)...)) {
  }

  template<class Func>
  static auto handle(Func&& func) {
    static_assert(std::is_invocable_v<Func, ErrorT>);
    return [
      func = std::forward<Func>(func)
    ] (stateful_error_t<ErrorT>&& e) mutable -> decltype(auto) {
      try {
        std::rethrow_exception(e.ep);
      } catch (const ErrorT& obj) {
        return std::invoke(std::forward<Func>(func), obj);
      }
      ceph_abort_msg("exception type mismatch – impossible!");
    };
  }

private:
  std::exception_ptr ep;

  explicit stateful_error_t(std::exception_ptr ep) : ep(std::move(ep)) {}

  static constexpr const std::type_info& exception_ptr_type_info() {
    return typeid(ErrorT);
  }
  auto to_exception_ptr() const {
    return ep;
  }
  static stateful_error_t<ErrorT> from_exception_ptr(std::exception_ptr ep) {
    return stateful_error_t<ErrorT>(std::move(ep));
  }

  friend class error_t<stateful_error_t<ErrorT>>;
};

namespace _impl {
  template <class T> struct always_false : std::false_type {};
};

template <class ErrorVisitorT, class FuturatorT>
class maybe_handle_error_t {
  const std::type_info& type_info;
  typename FuturatorT::type result;
  ErrorVisitorT errfunc;

public:
  maybe_handle_error_t(ErrorVisitorT&& errfunc, std::exception_ptr ep)
    : type_info(*ep.__cxa_exception_type()),
      result(FuturatorT::make_exception_future(std::move(ep))),
      errfunc(std::forward<ErrorVisitorT>(errfunc)) {
  }

  template <class ErrorT>
  void handle() {
    static_assert(std::is_invocable<ErrorVisitorT, ErrorT>::value,
                  "provided Error Visitor is not exhaustive");
    // In C++ throwing an exception isn't the sole way to signal
    // error with it. This approach nicely fits cold, infrequent cases
    // but when applied to a hot one, it will likely hurt performance.
    //
    // Alternative approach is to create `std::exception_ptr` on our
    // own and place it in the future via `make_exception_future()`.
    // When it comes to handling, the pointer can be interrogated for
    // pointee's type with `__cxa_exception_type()` instead of costly
    // re-throwing (via `std::rethrow_exception()`) and matching with
    // `catch`. The limitation here is lack of support for hierarchies
    // of exceptions. The code below checks for exact match only while
    // `catch` would allow to match against a base class as well.
    // However, this shouldn't be a big issue for `errorator` as Error
    // Visitors are already checked for exhaustiveness at compile-time.
    //
    // NOTE: `__cxa_exception_type()` is an extension of the language.
    // It should be available both in GCC and Clang but a fallback
    // (based on `std::rethrow_exception()` and `catch`) can be made
    // to handle other platforms if necessary.
    if (type_info == ErrorT::error_t::get_exception_ptr_type_info()) {
      // set `state::invalid` in internals of `seastar::future` to not
      // call `report_failed_future()` during `operator=()`.
      [[maybe_unused]] auto&& ep = std::move(result).get_exception();

      using return_t = std::invoke_result_t<ErrorVisitorT, ErrorT>;
      if constexpr (std::is_assignable_v<decltype(result), return_t>) {
        result = std::invoke(std::forward<ErrorVisitorT>(errfunc),
                             ErrorT::error_t::from_exception_ptr(std::move(ep)));
      } else if constexpr (std::is_same_v<return_t, void>) {
        // void denotes explicit discarding
        // execute for the sake a side effects. Typically this boils down
        // to throwing an exception by the handler.
        std::invoke(std::forward<ErrorVisitorT>(errfunc),
                    ErrorT::error_t::from_exception_ptr(std::move(ep)));
      } else {
        static_assert(_impl::always_false<return_t>::value,
                      "return of Error Visitor is not assignable to future");
        // do nothing with `ep`.
      }
    }
  }

  auto get_result() && {
    return std::move(result);
  }
};

template <class FuncHead, class... FuncTail>
static constexpr auto composer(FuncHead&& head, FuncTail&&... tail) {
  return [
    head = std::forward<FuncHead>(head),
    // perfect forwarding in lambda's closure isn't availble in C++17
    // using tuple as workaround; see: https://stackoverflow.com/a/49902823
    tail = std::make_tuple(std::forward<FuncTail>(tail)...)
  ] (auto&&... args) mutable -> decltype(auto) {
    if constexpr (std::is_invocable_v<FuncHead, decltype(args)...>) {
      return std::invoke(std::forward<FuncHead>(head),
                         std::forward<decltype(args)>(args)...);
    } else if constexpr (sizeof...(FuncTail) > 0) {
      using next_composer_t = decltype(composer<FuncTail...>);
      auto&& next = std::apply<next_composer_t>(composer<FuncTail...>,
                                                std::move(tail));
      return std::invoke(std::move(next),
                         std::forward<decltype(args)>(args)...);
    } else {
      static_assert(
	std::is_invocable_v<FuncHead, decltype(args)...> ||
	(sizeof...(FuncTail) > 0),
      "composition is not exhaustive");
    }
  };
}

template <class... ValuesT>
struct errorated_future_marker{};

template <class... AllowedErrors>
struct errorator {
  template <class T>
  static inline constexpr bool is_error_v = std::is_base_of_v<error_t<T>, T>;

  static_assert((... && is_error_v<AllowedErrors>),
                "errorator expects presence of ::is_error in all error types");

  template <class ErrorT>
  struct contains_once {
    static constexpr bool value =
      (0 + ... + std::is_same_v<ErrorT, AllowedErrors>) == 1;
  };
  template <class... Errors>
  struct contains_once<errorator<Errors...>> {
    static constexpr bool value = (... && contains_once<Errors>::value);
  };
  template <class T>
  static constexpr bool contains_once_v = contains_once<T>::value;

  static_assert((... && contains_once_v<AllowedErrors>),
                "no error type in errorator can be duplicated");

  struct ready_future_marker{};
  struct exception_future_marker{};

private:
  // see the comment for `using future = _future` below.
  template <class>
  class _future {};
  template <class... ValuesT>
  class _future<::crimson::errorated_future_marker<ValuesT...>>
    : private seastar::future<ValuesT...> {
    using base_t = seastar::future<ValuesT...>;
    // we need the friendship for the sake of `get_exception() &&` when
    // `safe_then()` is going to return an errorated future as a result of
    // chaining. In contrast to `seastar::future`, errorator<T...>::future`
    // has this member private.
    template <class ErrorVisitor, class Futurator>
    friend class maybe_handle_error_t;

    template <class, class = std::void_t<>>
    struct get_errorator {
      // generic template for non-errorated things (plain types and
      // vanilla seastar::future as well).
      using type = errorator<>;
    };
    template <class FutureT>
    struct get_errorator<FutureT,
                         std::void_t<typename FutureT::errorator_type>> {
      using type = typename FutureT::errorator_type;
    };
    template <class T>
    using get_errorator_t = typename get_errorator<T>::type;

    template <class ValueFuncErroratorT, class... ErrorVisitorRetsT>
    struct make_errorator {
      // NOP. The generic template.
    };
    template <class... ValueFuncAllowedErrors,
              class    ErrorVisitorRetsHeadT,
              class... ErrorVisitorRetsTailT>
    struct make_errorator<errorator<ValueFuncAllowedErrors...>,
                          ErrorVisitorRetsHeadT,
                          ErrorVisitorRetsTailT...> {
    private:
      using step_errorator = errorator<ValueFuncAllowedErrors...>;
      // add ErrorVisitorRetsHeadT only if 1) it's an error type and
      // 2) isn't already included in the errorator's error set.
      // It's enough to negate contains_once_v as any errorator<...>
      // type is already guaranteed to be free of duplications.
      using next_errorator = std::conditional_t<
        is_error_v<ErrorVisitorRetsHeadT> &&
          !step_errorator::template contains_once_v<ErrorVisitorRetsHeadT>,
        typename step_errorator::template extend<ErrorVisitorRetsHeadT>,
        step_errorator>;

    public:
      using type = typename make_errorator<next_errorator,
                                           ErrorVisitorRetsTailT...>::type;
    };
    // finish the recursion
    template <class... ValueFuncAllowedErrors>
    struct make_errorator<errorator<ValueFuncAllowedErrors...>> {
      using type = ::crimson::errorator<ValueFuncAllowedErrors...>;
    };
    template <class... Args>
    using make_errorator_t = typename make_errorator<Args...>::type;

    using base_t::base_t;

    template <class Futurator, class Future, class ErrorVisitor>
    [[gnu::noinline]]
    static auto _safe_then_handle_errors(Future&& future,
                                         ErrorVisitor&& errfunc) {
      maybe_handle_error_t<ErrorVisitor, Futurator> maybe_handle_error(
        std::forward<ErrorVisitor>(errfunc),
        std::move(future).get_exception()
      );
      (maybe_handle_error.template handle<AllowedErrors>() , ...);
      return std::move(maybe_handle_error).get_result();
    }

  public:
    using errorator_type = ::crimson::errorator<AllowedErrors...>;
    using promise_type = seastar::promise<ValuesT...>;

    using base_t::available;
    using base_t::failed;

    [[gnu::always_inline]]
    _future(base_t&& base)
      : base_t(std::move(base)) {
    }

    template <class... A>
    [[gnu::always_inline]]
    _future(ready_future_marker, A&&... a)
      : base_t(::seastar::make_ready_future<ValuesT...>(std::forward<A>(a)...)) {
    }
    [[gnu::always_inline]]
    _future(exception_future_marker, ::seastar::future_state_base&& state) noexcept
      : base_t(::seastar::futurize<base_t>::make_exception_future(std::move(state))) {
    }
    [[gnu::always_inline]]
    _future(exception_future_marker, std::exception_ptr&& ep) noexcept
      : base_t(::seastar::futurize<base_t>::make_exception_future(std::move(ep))) {
    }

    template <template <class...> class ErroratedFuture,
              class = std::void_t<
                typename ErroratedFuture<
                  ::crimson::errorated_future_marker<ValuesT...>>::errorator_type>>
    operator ErroratedFuture<errorated_future_marker<ValuesT...>> () && {
      using dest_errorator_t = \
        typename ErroratedFuture<
          ::crimson::errorated_future_marker<ValuesT...>>::errorator_type;
      static_assert(dest_errorator_t::template contains_once_v<errorator_type>,
                    "conversion is possible to more-or-eq errorated future!");
      return static_cast<base_t&&>(*this);
    }

    // initialize future as failed without throwing. `make_exception_future()`
    // internally uses `std::make_exception_ptr()`. cppreference.com shouldn't
    // be misinterpreted when it says:
    //
    //   "This is done as if executing the following code:
    //     try {
    //         throw e;
    //     } catch(...) {
    //         return std::current_exception();
    //     }",
    //
    // the "as if" is absolutely crucial because modern GCCs employ optimized
    // path for it. See:
    //   * https://gcc.gnu.org/git/?p=gcc.git;a=commit;h=cce8e59224e18858749a2324bce583bcfd160d6c,
    //   * https://gcc.gnu.org/ml/gcc-patches/2016-08/msg00373.html.
    //
    // This behavior, combined with `__cxa_exception_type()` for inspecting
    // exception's type, allows for throw/catch-free handling of stateless
    // exceptions (which is fine for error codes). Stateful jumbos would be
    // actually a bit harder as `_M_get()` is private, and thus rethrowing is
    // necessary to get to the state inside. However, it's not unthinkable to
    // see another extension bringing operator*() to the exception pointer...
    //
    // TODO: we don't really need to `make_exception_ptr` each time. It still
    // allocates memory underneath while can be replaced with single instance
    // per type created on start-up.
    template <class ErrorT,
              class DecayedT = std::decay_t<ErrorT>,
              bool IsError = is_error_v<DecayedT>,
              class = std::enable_if_t<IsError>>
    _future(ErrorT&& e)
      : base_t(
          seastar::make_exception_future<ValuesT...>(
            errorator_type::make_exception_ptr(e))) {
      static_assert(errorator_type::contains_once_v<DecayedT>,
                    "ErrorT is not enlisted in errorator");
    }

    template <class ValueFuncT, class ErrorVisitorT>
    auto safe_then(ValueFuncT&& valfunc, ErrorVisitorT&& errfunc) {
      static_assert((... && std::is_invocable_v<ErrorVisitorT,
                                                AllowedErrors>),
                    "provided Error Visitor is not exhaustive");

      using value_func_result_t = std::invoke_result_t<ValueFuncT, ValuesT&&...>;
      // recognize whether there can be any error coming from the Value
      // Function.
      using value_func_errorator_t = get_errorator_t<value_func_result_t>;
      // mutate the Value Function's errorator to harvest errors coming
      // from the Error Visitor. Yes, it's perfectly fine to fail error
      // handling at one step and delegate even broader set of issues
      // to next continuation.
      using return_errorator_t = make_errorator_t<
        value_func_errorator_t,
        std::decay_t<std::invoke_result_t<ErrorVisitorT, AllowedErrors>>...>;
      // OK, now we know about all errors next continuation must take
      // care about. If Visitor handled everything and the Value Func
      // doesn't return any, we'll finish with errorator<>::future
      // which is just vanilla seastar::future – that's it, next cont
      // finally could use `.then()`!
      using futurator_t = \
        typename return_errorator_t::template futurize<value_func_result_t>;
      // `seastar::futurize`, used internally by `then_wrapped()`, would
      // wrap any non-`seastar::future` type coming from Value Func into
      // `seastar::future`. As we really don't want to end with things
      // like `seastar::future<errorator::future<...>>`, we need either:
      //   * convert the errorated future into plain in the lambda below
      //     and back here or
      //   * specialize the `seastar::futurize<T>` to get proper kind of
      //     future directly from `::then_wrapped()`.
      // As C++17 doesn't guarantee copy elision when non-same types are
      // involved while examination of assemblies from GCC 8.1 confirmed
      // extra copying, switch to the second approach has been made.
      return this->then_wrapped(
        [ valfunc = std::forward<ValueFuncT>(valfunc),
          errfunc = std::forward<ErrorVisitorT>(errfunc)
        ] (auto&& future) mutable noexcept {
          if (__builtin_expect(future.failed(), false)) {
            return _safe_then_handle_errors<futurator_t>(
              std::move(future), std::forward<ErrorVisitorT>(errfunc));
          } else {
            // NOTE: using `seastar::future::get()` here is a bit bloaty
            // as the method rechecks availability of future's value and,
            // if it's unavailable, does the `::do_wait()` path (yes, it
            // targets `seastar::thread`). Actually this is dead code as
            // `then_wrapped()` executes the lambda only when the future
            // is available (which means: failed or ready). However, GCC
            // hasn't optimized it out:
            //
            //          if (__builtin_expect(future.failed(), false)) {
            //    ea25:       48 83 bd c8 fe ff ff    cmpq   $0x2,-0x138(%rbp)
            //    ea2c:       02
            //    ea2d:       0f 87 f0 05 00 00       ja     f023 <ceph::osd::
            // ...
            //    /// If get() is called in a \ref seastar::thread context,
            //    /// then it need not be available; instead, the thread will
            //    /// be paused until the future becomes available.
            //    [[gnu::always_inline]]
            //    std::tuple<T...> get() {
            //        if (!_state.available()) {
            //    ea3a:       0f 85 1b 05 00 00       jne    ef5b <ceph::osd::
            //    }
            // ...
            //
            // I don't perceive this as huge issue. Though, it cannot be
            // claimed errorator has 0 overhead on hot path. The perfect
            // solution here would be mark the `::get_available_state()`
            // as `protected` and use dedicated `get_value()` exactly as
            // `::then()` already does.
            return futurator_t::apply(std::forward<ValueFuncT>(valfunc),
                                      std::move(future).get());
          }
        });
    }

    /**
     * unsafe_thread_get
     *
     * Only valid within a seastar_thread.  Ignores errorator protections
     * and throws any contained exceptions.
     *
     * Should really only be used within test code
     * (see test/crimson/gtest_seastar.h).
     */
    auto &&unsafe_get() {
      return seastar::future<ValuesT...>::get();
    }
    auto unsafe_get0() {
      return seastar::future<ValuesT...>::get0();
    }


    template <class FuncT>
    auto finally(FuncT &&func) {
      using func_result_t = std::invoke_result_t<FuncT>;
      using func_errorator_t = get_errorator_t<func_result_t>;
      using return_errorator_t = func_errorator_t;
      using futurator_t =
        typename return_errorator_t::template futurize<func_result_t>;

      return this->then_wrapped(
	[ func = std::forward<FuncT>(func)
	] (auto&& future) mutable noexcept {
	  return futurator_t::invoke(std::forward<FuncT>(func)).safe_then(
	    [future = std::forward<decltype(future)>(future)]() mutable {
	      return std::move(future);
	    });
	});
    }

    // taking ErrorFuncOne and ErrorFuncTwo separately from ErrorFuncTail
    // to avoid SFINAE
    template <class ValueFunc,
              class ErrorFuncHead,
              class... ErrorFuncTail>
    auto safe_then(ValueFunc&& value_func,
                   ErrorFuncHead&& error_func_head,
                   ErrorFuncTail&&... error_func_tail) {
      static_assert(sizeof...(ErrorFuncTail) > 0);
      return safe_then(
        std::forward<ValueFunc>(value_func),
        composer(std::forward<ErrorFuncHead>(error_func_head),
                 std::forward<ErrorFuncTail>(error_func_tail)...));
    }

    template <class ValueFunc>
    auto safe_then(ValueFunc&& value_func) {
      return safe_then(std::forward<ValueFunc>(value_func),
                       errorator_type::pass_further{});
    }

    template <class Func>
    void then(Func&&) = delete;

    template <class ErrorVisitorT>
    auto handle_error(ErrorVisitorT&& errfunc) {
      static_assert((... && std::is_invocable_v<ErrorVisitorT,
                                                AllowedErrors>),
                    "provided Error Visitor is not exhaustive");
      using return_errorator_t = make_errorator_t<
        errorator<>,
        std::decay_t<std::invoke_result_t<ErrorVisitorT, AllowedErrors>>...>;
      using futurator_t = \
        typename return_errorator_t::template futurize<::seastar::future<ValuesT...>>;
      return this->then_wrapped(
        [ errfunc = std::forward<ErrorVisitorT>(errfunc)
        ] (auto&& future) mutable noexcept {
          if (__builtin_expect(future.failed(), false)) {
            return _safe_then_handle_errors<futurator_t>(
              std::move(future), std::forward<ErrorVisitorT>(errfunc));
          } else {
            return typename futurator_t::type{ std::move(future) };
          }
        });
    }
    template <class ErrorFuncHead,
              class... ErrorFuncTail>
    auto handle_error(ErrorFuncHead&& error_func_head,
                      ErrorFuncTail&&... error_func_tail) {
      static_assert(sizeof...(ErrorFuncTail) > 0);
      return this->handle_error(
        composer(std::forward<ErrorFuncHead>(error_func_head),
                 std::forward<ErrorFuncTail>(error_func_tail)...));
    }

  private:
    // for ::crimson::do_for_each
    template <class Func>
    auto _then(Func&& func) {
      return base_t::then(std::forward<Func>(func));
    }
    template<typename Iterator, typename AsyncAction>
    friend inline auto ::crimson::do_for_each(Iterator begin,
                                              Iterator end,
                                              AsyncAction action);

    template<typename AsyncAction>
    friend inline auto ::crimson::do_until(AsyncAction action);

    template <class...>
    friend class ::seastar::future;

    // let seastar::do_with_impl to up-cast us to seastar::future.
    template<typename T, typename F>
    friend inline auto ::seastar::internal::do_with_impl(T&& rvalue, F&& f);
    template<typename T1, typename T2, typename T3_or_F, typename... More>
    friend inline auto ::seastar::internal::do_with_impl(T1&& rv1, T2&& rv2, T3_or_F&& rv3, More&&... more);
  };

  class Enabler {};

  template <typename T>
  using EnableIf = typename std::enable_if<contains_once_v<std::decay_t<T>>, Enabler>::type;

  template <typename ErrorFunc>
  struct all_same_way_t {
    ErrorFunc func;
    all_same_way_t(ErrorFunc &&error_func)
      : func(std::forward<ErrorFunc>(error_func)) {}

    template <typename ErrorT, EnableIf<ErrorT>...>
    decltype(auto) operator()(ErrorT&& e) {
      using decayed_t = std::decay_t<decltype(e)>;
      auto&& handler =
        decayed_t::error_t::handle(std::forward<ErrorFunc>(func));
      static_assert(std::is_invocable_v<decltype(handler), ErrorT>);
      return std::invoke(std::move(handler), std::forward<ErrorT>(e));
    }
  };

public:
  // HACK: `errorated_future_marker` and `_future` is just a hack to
  // specialize `seastar::futurize` for category of class templates:
  // `future<...>` from distinct errorators. Such tricks are usually
  // performed basing on SFINAE and `std::void_t` to check existence
  // of a trait/member (`future<...>::errorator_type` in our case).
  // Unfortunately, this technique can't be applied as the `futurize`
  // lacks the optional parameter. The problem looks awfully similar
  // to following SO item:  https://stackoverflow.com/a/38860413.
  template <class... ValuesT>
  using future = _future<::crimson::errorated_future_marker<ValuesT...>>;

  // the visitor that forwards handling of all errors to next continuation
  struct pass_further {
    template <class ErrorT, EnableIf<ErrorT>...>
    decltype(auto) operator()(ErrorT&& e) {
      static_assert(contains_once_v<std::decay_t<ErrorT>>,
                    "passing further disallowed ErrorT");
      return std::forward<ErrorT>(e);
    }
  };

  struct discard_all {
    template <class ErrorT, EnableIf<ErrorT>...>
    void operator()(ErrorT&&) {
      static_assert(contains_once_v<std::decay_t<ErrorT>>,
                    "discarding disallowed ErrorT");
    }
  };

  // assert_all{ "TODO" };
  class assert_all {
    const char* const msg = nullptr;
  public:
    template <std::size_t N>
    assert_all(const char (&msg)[N])
      : msg(msg) {
    }
    assert_all() = default;

    template <class ErrorT, EnableIf<ErrorT>...>
    void operator()(ErrorT&&) {
      static_assert(contains_once_v<std::decay_t<ErrorT>>,
                    "discarding disallowed ErrorT");
      if (msg) {
        ceph_abort_msg(msg);
      } else {
        ceph_abort();
      }
    }
  };

  template <class ErrorFunc>
  static decltype(auto) all_same_way(ErrorFunc&& error_func) {
    return all_same_way_t<ErrorFunc>{std::forward<ErrorFunc>(error_func)};
  };

  // get a new errorator by extending current one with new error
  template <class... NewAllowedErrorsT>
  using extend = errorator<AllowedErrors..., NewAllowedErrorsT...>;

  // get a new errorator by summing and deduplicating error set of
  // the errorator `unify<>` is applied on with another errorator
  // provided as template parameter.
  template <class OtherErroratorT>
  struct unify {
    // 1st: generic NOP template
  };
  template <class    OtherAllowedErrorsHead,
            class... OtherAllowedErrorsTail>
  struct unify<errorator<OtherAllowedErrorsHead,
                         OtherAllowedErrorsTail...>> {
  private:
    // 2nd: specialization for errorators with non-empty error set.
    //
    // split error set of other errorator, passed as template param,
    // into head and tail. Mix error set of this errorator with head
    // of the other one only if it isn't already present in the set.
    using step_errorator = std::conditional_t<
      contains_once_v<OtherAllowedErrorsHead> == false,
      errorator<AllowedErrors..., OtherAllowedErrorsHead>,
      errorator<AllowedErrors...>>;
    using rest_errorator = errorator<OtherAllowedErrorsTail...>;

  public:
    using type = typename step_errorator::template unify<rest_errorator>::type;
  };
  template <class... EmptyPack>
  struct unify<errorator<EmptyPack...>> {
    // 3rd: recursion finisher
    static_assert(sizeof...(EmptyPack) == 0);
    using type = errorator<AllowedErrors...>;
  };

  template <typename... T, typename... A>
  static future<T...> make_ready_future(A&&... value) {
    return future<T...>(ready_future_marker(), std::forward<A>(value)...);
  }

  template <typename... T>
  static
  future<T...> make_exception_future2(std::exception_ptr&& ex) noexcept {
    return future<T...>(exception_future_marker(), std::move(ex));
  }
  template <typename... T>
  static
  future<T...> make_exception_future2(seastar::future_state_base&& state) noexcept {
    return future<T...>(exception_future_marker(), std::move(state));
  }
  template <typename... T, typename Exception>
  static
  future<T...> make_exception_future2(Exception&& ex) noexcept {
    return make_exception_future2<T...>(std::make_exception_ptr(std::forward<Exception>(ex)));
  }

  static auto now() {
    return make_ready_future<>();
  }

private:
  template <class T, class = std::void_t<T>>
  class futurize {
    using vanilla_futurize = seastar::futurize<T>;

    template <class...>
    struct tuple2future {};
    template <class... Args>
    struct tuple2future <std::tuple<Args...>> {
      using type = future<Args...>;
    };

  public:
    using type =
      typename tuple2future<typename vanilla_futurize::value_type>::type;

    template <class Func, class... Args>
    static type apply(Func&& func, std::tuple<Args...>&& args) {
      return vanilla_futurize::apply(std::forward<Func>(func),
                                     std::forward<std::tuple<Args...>>(args));
    }

    template <typename Arg>
    static type make_exception_future(Arg&& arg) {
      return vanilla_futurize::make_exception_future(std::forward<Arg>(arg));
    }
  };
  template <template <class...> class ErroratedFutureT,
            class... ValuesT>
  class futurize<ErroratedFutureT<::crimson::errorated_future_marker<ValuesT...>>,
                 std::void_t<
                   typename ErroratedFutureT<
                     ::crimson::errorated_future_marker<ValuesT...>>::errorator_type>> {
  public:
    using type = ::crimson::errorator<AllowedErrors...>::future<ValuesT...>;

    template <class Func, class... Args>
    static type apply(Func&& func, std::tuple<Args...>&& args) {
      try {
        return ::seastar::futurize_apply(std::forward<Func>(func),
					 std::forward<std::tuple<Args...>>(args));
      } catch (...) {
        return make_exception_future(std::current_exception());
      }
    }

    template <typename Arg>
    static type make_exception_future(Arg&& arg) {
      return ::crimson::errorator<AllowedErrors...>::make_exception_future2<ValuesT...>(std::forward<Arg>(arg));
    }
  };

  template <class ErrorT>
  static std::exception_ptr make_exception_ptr(ErrorT&& e) {
    // calling via interface class due to encapsulation and friend relations.
    return e.error_t<std::decay_t<ErrorT>>::to_exception_ptr();
  }

  // needed because of:
  //  * return_errorator_t::template futurize<...> in `safe_then()`,
  //  * conversion to `std::exception_ptr` in `future::future(ErrorT&&)`.
  // the friendship with all errorators is an idea from Kefu to fix build
  // issues on GCC 9. This version likely fixes some access violation bug
  // we were exploiting before.
  template <class...>
  friend class errorator;
}; // class errorator, generic template

// no errors? errorator<>::future is plain seastar::future then!
template <>
class errorator<> {
public:
  template <class... ValuesT>
  using future = ::seastar::future<ValuesT...>;

  template <class T>
  using futurize = ::seastar::futurize<T>;

  // get a new errorator by extending current one with new error
  template <class... NewAllowedErrors>
  using extend = errorator<NewAllowedErrors...>;

  // errorator with empty error set never contains any error
  template <class T>
  static constexpr bool contains_once_v = false;
}; // class errorator, <> specialization


template <class    ErroratorOne,
          class    ErroratorTwo,
          class... FurtherErrators>
struct compound_errorator {
private:
  // generic template. Empty `FurtherErrators` are handled by
  // the specialization below.
  static_assert(sizeof...(FurtherErrators) > 0);
  using step =
    typename compound_errorator<ErroratorOne, ErroratorTwo>::type;

public:
  using type =
    typename compound_errorator<step, FurtherErrators...>::type;
};
template <class ErroratorOne,
          class ErroratorTwo>
struct compound_errorator<ErroratorOne, ErroratorTwo>  {
  // specialization for empty `FurtherErrators` arg pack
  using type =
    typename ErroratorOne::template unify<ErroratorTwo>::type;
};
template <class... Args>
using compound_errorator_t = typename compound_errorator<Args...>::type;

// this is conjunction of two nasty features: C++14's variable template
// and inline global variable of C++17. The latter is crucial to ensure
// the variable will get the same address across all translation units.
template <std::errc ErrorV>
inline std::error_code ec = std::make_error_code(ErrorV);

template <std::errc ErrorV>
using ct_error_code = unthrowable_wrapper<const std::error_code&, ec<ErrorV>>;

namespace ct_error {
  using enoent = ct_error_code<std::errc::no_such_file_or_directory>;
  using enodata = ct_error_code<std::errc::no_message_available>;
  using invarg =  ct_error_code<std::errc::invalid_argument>;
  using input_output_error = ct_error_code<std::errc::io_error>;
  using object_corrupted = ct_error_code<std::errc::illegal_byte_sequence>;
  using permission_denied = ct_error_code<std::errc::permission_denied>;
  using operation_not_supported =
    ct_error_code<std::errc::operation_not_supported>;
  using not_connected = ct_error_code<std::errc::not_connected>;
  using timed_out = ct_error_code<std::errc::timed_out>;
  using erange =
    ct_error_code<std::errc::result_out_of_range>;
  using ebadf =
    ct_error_code<std::errc::bad_file_descriptor>;
  using enospc =
    ct_error_code<std::errc::no_space_on_device>;
  using value_too_large = ct_error_code<std::errc::value_too_large>;
  using eagain =
    ct_error_code<std::errc::resource_unavailable_try_again>;

  struct pass_further_all {
    template <class ErrorT>
    decltype(auto) operator()(ErrorT&& e) {
      return std::forward<ErrorT>(e);
    }
  };

  struct discard_all {
    template <class ErrorT>
    void operator()(ErrorT&&) {
    }
  };

  class assert_all {
    const char* const msg = nullptr;
  public:
    template <std::size_t N>
    assert_all(const char (&msg)[N])
      : msg(msg) {
    }
    assert_all() = default;

    template <class ErrorT>
    void operator()(ErrorT&&) {
      if (msg) {
        ceph_abort(msg);
      } else {
        ceph_abort();
      }
    }
  };

  template <class ErrorFunc>
  static decltype(auto) all_same_way(ErrorFunc&& error_func) {
    return [
      error_func = std::forward<ErrorFunc>(error_func)
    ] (auto&& e) mutable -> decltype(auto) {
      using decayed_t = std::decay_t<decltype(e)>;
      auto&& handler =
        decayed_t::error_t::handle(std::forward<ErrorFunc>(error_func));
      return std::invoke(std::move(handler), std::forward<decltype(e)>(e));
    };
  };
}

using stateful_errc = stateful_error_t<std::errc>;
using stateful_errint = stateful_error_t<int>;
using stateful_ec = stateful_error_t<std::error_code>;

} // namespace crimson


// open the `seastar` namespace to specialize `futurize`. This is not
// pretty for sure. I just hope it's not worse than e.g. specializing
// `hash` in the `std` namespace. The justification is copy avoidance
// in `future<...>::safe_then()`. See the comments there for details.
namespace seastar {

template <template <class...> class Container,
          class... Values>
struct futurize<Container<::crimson::errorated_future_marker<Values...>>> {
  using errorator_type = typename Container<
    ::crimson::errorated_future_marker<Values...>>::errorator_type;

  using type = typename errorator_type::template future<Values...>;
  using value_type = std::tuple<Values...>;

  template<typename Func, typename... FuncArgs>
  [[gnu::always_inline]]
  static inline type apply(Func&& func, std::tuple<FuncArgs...>&& args) noexcept {
    try {
      return ::seastar::apply(std::forward<Func>(func),
                              std::forward<std::tuple<FuncArgs...>>(args));
    } catch (...) {
      return make_exception_future(std::current_exception());
    }
  }

  template<typename Func, typename... FuncArgs>
  static inline type invoke(Func&& func, FuncArgs&&... args) noexcept {
    try {
        return func(std::forward<FuncArgs>(args)...);
    } catch (...) {
        return make_exception_future(std::current_exception());
    }
  }

  template <typename Arg>
  [[gnu::always_inline]]
  static type make_exception_future(Arg&& arg) {
    return errorator_type::template make_exception_future2<Values...>(std::forward<Arg>(arg));
  }
};

namespace internal {
template <template <class...> class Container,
          class... Values>
struct continuation_base_from_future<Container<::crimson::errorated_future_marker<Values...>>> {
  using type = continuation_base<Values...>;
};
}

} // namespace seastar
