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

// unthrowable_wrapper ensures compilation failure when somebody
// would like to `throw make_error<...>)()` instead of returning.
// returning allows for the compile-time verification of future's
// AllowedErrorsV and also avoid the burden of throwing.
template <_impl::ct_error ErrorV>
struct unthrowable_wrapper {
  using wrapped_type = decltype(ErrorV);

  unthrowable_wrapper(const unthrowable_wrapper&) = delete;
  static constexpr unthrowable_wrapper instance{};
  template <class T> friend const T& make_error();

  // comparison operator for the static_assert in future's ctor.
  template <_impl::ct_error OtherErrorV>
  constexpr bool operator==(const unthrowable_wrapper<OtherErrorV>&) const {
    return OtherErrorV == ErrorV;
  }

private:
  // can be used only to initialize the `instance` member
  explicit unthrowable_wrapper() = default;
};

template <class T> [[nodiscard]] const T& make_error() {
  return T::instance;
}

// TODO: let `exception` use other type than `ct_error`.
template <_impl::ct_error V>
class exception {
  exception() = default;
public:
  exception(const exception&) = default;
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

  template <_impl::ct_error ErrorV>
  void operator()(const unthrowable_wrapper<ErrorV>& e) {
    static_assert(std::is_invocable<ErrorVisitorT, decltype(e)>::value,
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
    if (type_info == typeid(exception<ErrorV>)) {
      // set `state::invalid` in internals of `seastar::future` to not
      // call `report_failed_future()` during `operator=()`.
      std::move(result).get_exception();

      constexpr bool explicitly_discarded = std::is_invocable_r<
        struct ignore_marker_t&&, ErrorVisitorT, decltype(e)>::value;
      if constexpr (!explicitly_discarded) {
        result = std::forward<ErrorVisitorT>(errfunc)(e);
      }
    }
  }

  auto get_result() && {
    return std::move(result);
  }
};

template <class... WrappedAllowedErrorsT>
struct errorator {
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
    struct is_error {
      static constexpr bool value = false;
    };
    template <class T>
    struct is_error<T, std::void_t<typename T::wrapped_type>> {
      // specialization for _impl::ct_error. it could be written in much
      // simpler form – without void_t and is_same_v.
      static constexpr bool value = \
        std::is_same_v<typename T::wrapped_type, _impl::ct_error>;
    };
    template <class T>
    static inline constexpr bool is_error_v = is_error<T>::value;

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
    template <class... ValueFuncWrappedAllowedErrorsT,
              class    ErrorVisitorRetsHeadT,
              class... ErrorVisitorRetsTailT>
    struct make_errorator<errorator<ValueFuncWrappedAllowedErrorsT...>,
                          ErrorVisitorRetsHeadT,
                          ErrorVisitorRetsTailT...> {
      using type = std::conditional_t<
        is_error_v<ErrorVisitorRetsHeadT>,
        typename make_errorator<errorator<ValueFuncWrappedAllowedErrorsT...,
                                          ErrorVisitorRetsHeadT>,
                                ErrorVisitorRetsTailT...>::type,
        typename make_errorator<errorator<ValueFuncWrappedAllowedErrorsT...>,
                                ErrorVisitorRetsTailT...>::type>;
    };
    // finish the recursion
    template <class... ValueFuncWrappedAllowedErrorsT>
    struct make_errorator<errorator<ValueFuncWrappedAllowedErrorsT...>> {
      using type = ::ceph::errorator<ValueFuncWrappedAllowedErrorsT...>;
    };
    template <class... Args>
    using make_errorator_t = typename make_errorator<Args...>::type;

    using base_t::base_t;

  public:
    using errorator_type = ceph::errorator<WrappedAllowedErrorsT...>;

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
      using this_errorator_t = errorator<WrappedAllowedErrorsT...>;

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
    template <_impl::ct_error ErrorV>
    future(const unthrowable_wrapper<ErrorV>& e)
      : base_t(seastar::make_exception_future<ValuesT...>(exception<ErrorV>{})) {
      // this is `fold expression` of C++17
      static_assert((... || (e == WrappedAllowedErrorsT::instance)),
                    "disallowed ct_error");
    }

    template <class ValueFuncT, class ErrorVisitorT>
    auto safe_then(ValueFuncT&& valfunc, ErrorVisitorT&& errfunc) {
      static_assert((... && std::is_invocable_v<
                      ErrorVisitorT,
                      decltype(WrappedAllowedErrorsT::instance)>),
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
        std::decay_t<std::invoke_result_t<
          ErrorVisitorT, decltype(WrappedAllowedErrorsT::instance)>>...>;
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
            (maybe_handle_error(WrappedAllowedErrorsT::instance) , ...);
            return plainify(std::move(maybe_handle_error).get_result());
          } else {
            return plainify(futurator_t::apply(std::forward<ValueFuncT>(valfunc),
                                               std::move(future).get()));
          }
        })};
    }

    template <class Func>
    void then(Func&&) = delete;


  private:
    // for the sake of `plainify()` let any errorator convert errorated
    // future into plain one.
    template <class... AnyWrappedAllowedErrorsT>
    friend class errorator;

    base_t&& as_plain_future() && {
      return std::move(*this);
    }
  };

  // the visitor that forwards handling of all errors to next continuation
  struct pass_further {
    template <_impl::ct_error ErrorV>
    const auto& operator()(const unthrowable_wrapper<ErrorV>& e) {
      static_assert((... || (e == WrappedAllowedErrorsT::instance)),
                    "passing further disallowed ct_error");
      return ::crimson::make_error<std::decay_t<decltype(e)>>();
    }
  };

  struct discard_all {
    template <_impl::ct_error ErrorV>
    auto operator()(const unthrowable_wrapper<ErrorV>& e) {
      static_assert((... || (e == WrappedAllowedErrorsT::instance)),
                    "discarding disallowed ct_error");
      return ignore_marker_t{};
    }
  };

  // get a new errorator by extending current one with new error
  template <class... NewWrappedAllowedErrorsT>
  using extend = errorator<WrappedAllowedErrorsT...,
                           NewWrappedAllowedErrorsT...>;

  // comparing errorators
  template <class ErrorT>
  struct is_carried {
    static constexpr bool value = \
      ((WrappedAllowedErrorsT::instance == ErrorT::instance) || ...);
  };
  template <class ErrorT>
  static constexpr bool is_carried_v = is_carried<ErrorT>::value;

  template <class>
  struct is_less_errorated {
    // NOP.
  };
  template <class... OtherWrappedAllowedErrorsT>
  struct is_less_errorated<errorator<OtherWrappedAllowedErrorsT...>> {
    static constexpr bool value = \
      ((!is_carried_v<OtherWrappedAllowedErrorsT>) || ...);
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
    using type = ::ceph::errorator<WrappedAllowedErrorsT...>::future<ValuesT...>;

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

  // needed because of return_errorator_t::template futurize<...>
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
  using enoent = unthrowable_wrapper<_impl::ct_error::enoent>;
  using enodata = unthrowable_wrapper<_impl::ct_error::enodata>;
  using invarg = unthrowable_wrapper<_impl::ct_error::invarg>;
  using input_output_error = unthrowable_wrapper<_impl::ct_error::input_output_error>;
  using object_corrupted = unthrowable_wrapper<_impl::ct_error::object_corrupted>;
}

} // namespace crimson
