// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>

namespace crimson {

namespace _impl {
  enum class ct_error {
    enoent,
    invarg,
    enodata,
    input_output_error,
    object_corrupted
  };
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
  static constexpr unthrowable_wrapper instance{};
  [[nodiscard]] static const auto& make() {
    return instance;
  }

  template<class Func>
  static auto handle(Func&& func) {
    return [
      func = std::forward<Func>(func)
    ] (const unthrowable_wrapper&) mutable -> decltype(auto) {
      return std::invoke(std::forward<Func>(func));
    };
  }

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
    return instance;
  }

  friend class error_t<unthrowable_wrapper<ErrorT, ErrorV>>;
};

template <class ErrorT, ErrorT ErrorV>
std::exception_ptr unthrowable_wrapper<ErrorT, ErrorV>::carrier_instance = \
  std::make_exception_ptr<
    unthrowable_wrapper<ErrorT, ErrorV>::throwable_carrier>({});

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
        _impl::always_false<FuncHead>::value, "composition is not exhaustive");
    }
  };
}

template <class... AllowedErrors>
struct errorator {
  template <class T>
  static inline constexpr bool is_error_v = std::is_base_of_v<error_t<T>, T>;

  static_assert((... && is_error_v<AllowedErrors>),
                "errorator expects presence of ::is_error in all error types");

  template <class... ValuesT>
  class future : private seastar::future<ValuesT...> {
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
      using type = std::conditional_t<
        is_error_v<ErrorVisitorRetsHeadT>,
        typename make_errorator<errorator<ValueFuncAllowedErrors...,
                                          ErrorVisitorRetsHeadT>,
                                ErrorVisitorRetsTailT...>::type,
        typename make_errorator<errorator<ValueFuncAllowedErrors...>,
                                ErrorVisitorRetsTailT...>::type>;
    };
    // finish the recursion
    template <class... ValueFuncAllowedErrors>
    struct make_errorator<errorator<ValueFuncAllowedErrors...>> {
      using type = ::crimson::errorator<ValueFuncAllowedErrors...>;
    };
    template <class... Args>
    using make_errorator_t = typename make_errorator<Args...>::type;

    using base_t::base_t;

  public:
    using errorator_type = ::crimson::errorator<AllowedErrors...>;

    [[gnu::always_inline]]
    future(base_t&& base)
      : base_t(std::move(base)) {
    }

    template <template <class...> class ErroratedFuture,
              class = std::void_t<
                typename ErroratedFuture<ValuesT...>::errorator_type>>
    operator ErroratedFuture<ValuesT...> () && {
      using dest_errorator_t = \
        typename ErroratedFuture<ValuesT...>::errorator_type;
      using this_errorator_t = errorator<AllowedErrors...>;

      static_assert(!dest_errorator_t::template is_less_errorated_v<this_errorator_t>,
                    "conversion is possible to more-or-eq errorated future!");
      return std::move(*this).as_plain_future();
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
    future(ErrorT&& e)
      : base_t(
          seastar::make_exception_future<ValuesT...>(
            errorator_type::make_exception_ptr(e))) {
      // this is `fold expression` of C++17.
      static_assert((... || (std::is_same_v<DecayedT, AllowedErrors>)),
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
      // `seastar::futurize`, used internally by `then_wrapped()`, wraps
      // any non-`seastar::future` in `seastar::future`. We really don't
      // want to end with e.g. `seastar::future<errorator::future<...>>`,
      // so the lambda converts errorated future into plain one and here
      // we're converting it back. As there is absolutely no difference
      // between these types from the memory layout's POV (casting could
      // be used instead), I count on compiler's ability to elide copies.
      return typename futurator_t::type{ this->then_wrapped(
        [ valfunc = std::forward<ValueFuncT>(valfunc),
          errfunc = std::forward<ErrorVisitorT>(errfunc)
        ] (auto future) mutable {
          if (__builtin_expect(future.failed(), false)) {
            maybe_handle_error_t<ErrorVisitorT, futurator_t> maybe_handle_error(
              std::forward<ErrorVisitorT>(errfunc),
              std::move(future).get_exception()
            );
            (maybe_handle_error.template handle<AllowedErrors>() , ...);
            return plainify(std::move(maybe_handle_error).get_result());
          } else {
            return plainify(futurator_t::apply(std::forward<ValueFuncT>(valfunc),
                                               std::move(future).get()));
          }
        })};
    }

    // taking ErrorFuncOne and ErrorFuncTwo separately from ErrorFuncTail
    // to avoid SFINAE
    template <class ValueFunc,
              class ErrorFuncOne,
              class ErrorFuncTwo,
              class... ErrorFuncTail>
    auto safe_then(ValueFunc&& value_func,
                   ErrorFuncOne&& error_func_one,
                   ErrorFuncTwo&& error_func_two,
                   ErrorFuncTail&&... error_func_tail) {
      return safe_then(
        std::forward<ValueFunc>(value_func),
        composer(std::forward<ErrorFuncOne>(error_func_one),
                 std::forward<ErrorFuncTwo>(error_func_two),
                 std::forward<ErrorFuncTail>(error_func_tail)...));
    }

    template <class Func>
    void then(Func&&) = delete;


  private:
    // for the sake of `plainify()` let any errorator convert errorated
    // future into plain one.
    template <class...>
    friend class errorator;

    base_t&& as_plain_future() && {
      return std::move(*this);
    }
  };

  // the visitor that forwards handling of all errors to next continuation
  struct pass_further {
    template <class ErrorT>
    decltype(auto) operator()(ErrorT&& e) {
      using decayed_t = std::decay_t<ErrorT>;
      static_assert((... || std::is_same_v<AllowedErrors, decayed_t>),
                    "passing further disallowed ErrorT");
      return std::forward<ErrorT>(e);
    }
  };

  struct discard_all {
    template <class ErrorT>
    decltype(auto) operator()(ErrorT&&) {
      static_assert((... || std::is_same_v<AllowedErrors,
                                           std::decay_t<ErrorT>>),
                    "discarding disallowed ErrorT");
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

  // get a new errorator by extending current one with new error
  template <class... NewAllowedErrorsT>
  using extend = errorator<AllowedErrors..., NewAllowedErrorsT...>;

  // comparing errorators
  template <class ErrorT>
  struct is_carried {
    static constexpr bool value = \
      ((std::is_same_v<AllowedErrors, ErrorT>) || ...);
  };
  template <class ErrorT>
  static constexpr bool is_carried_v = is_carried<ErrorT>::value;

  template <class>
  struct is_less_errorated {
    // NOP.
  };
  template <class... OtherAllowedErrors>
  struct is_less_errorated<errorator<OtherAllowedErrors...>> {
    static constexpr bool value = \
      ((!is_carried_v<OtherAllowedErrors>) || ...);
  };
  template <class OtherErrorator>
  static constexpr bool is_less_errorated_v = \
    is_less_errorated<OtherErrorator>::value;

private:
  template <class... Args>
  static decltype(auto) plainify(seastar::future<Args...>&& fut) {
    return std::forward<seastar::future<Args...>>(fut);
  }
  template <class Arg>
  static decltype(auto) plainify(Arg&& arg) {
    return std::forward<Arg>(arg).as_plain_future();
  }

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
  class futurize<ErroratedFutureT<ValuesT...>,
                 std::void_t<
                   typename ErroratedFutureT<ValuesT...>::errorator_type>> {
  public:
    using type = ::crimson::errorator<AllowedErrors...>::future<ValuesT...>;

    template <class Func, class... Args>
    static type apply(Func&& func, std::tuple<Args...>&& args) {
      try {
        return ::seastar::apply(std::forward<Func>(func),
                                std::forward<std::tuple<Args...>>(args));
      } catch (...) {
        return make_exception_future(std::current_exception());
      }
    }

    template <typename Arg>
    static type make_exception_future(Arg&& arg) {
      return ::seastar::make_exception_future<ValuesT...>(std::forward<Arg>(arg));
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
  template <class... ValueT>
  friend class future;
}; // class errorator, generic template

// no errors? errorator<>::future is plain seastar::future then!
template <>
class errorator<> {
public:
  template <class... ValuesT>
  using future = ::seastar::future<ValuesT...>;

  template <class T>
  using futurize = ::seastar::futurize<T>;
}; // class errorator, <> specialization

namespace ct_error {
  using enoent = unthrowable_wrapper<_impl::ct_error, _impl::ct_error::enoent>;
  using enodata = unthrowable_wrapper<_impl::ct_error, _impl::ct_error::enodata>;
  using invarg = unthrowable_wrapper<_impl::ct_error, _impl::ct_error::invarg>;
  using input_output_error = unthrowable_wrapper<_impl::ct_error, _impl::ct_error::input_output_error>;
  using object_corrupted = unthrowable_wrapper<_impl::ct_error, _impl::ct_error::object_corrupted>;
}

} // namespace crimson
