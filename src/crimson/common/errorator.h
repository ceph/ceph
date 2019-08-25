// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>

namespace crimson {

namespace _impl {
  enum class ct_error {
    enoent,
    invarg,
    enodata
  };
}

// unthrowable_wrapper ensures compilation failure when somebody
// would like to `throw make_error<...>)()` instead of returning.
// returning allows for the compile-time verification of future's
// AllowedErrorsV and also avoid the burden of throwing.
template <_impl::ct_error ErrorV> struct unthrowable_wrapper {
  unthrowable_wrapper(const unthrowable_wrapper&) = delete;
  static constexpr unthrowable_wrapper instance{};
  template <class T> friend const T& make_error();

  // comparison operator for the static_assert in future's ctor.
  template <_impl::ct_error OtherErrorV>
  constexpr bool operator==(const unthrowable_wrapper<OtherErrorV>&) const {
    return OtherErrorV == ErrorV;
  }

private:
  // can be used only for initialize the `instance` member
  explicit unthrowable_wrapper() = default;
};

template <class T> [[nodiscard]] const T& make_error() {
  return T::instance;
}

template <class... WrappedAllowedErrorsT>
struct errorator {
  template <class... ValuesT>
  class future : private seastar::future<ValuesT...> {
    using base_t = seastar::future<ValuesT...>;

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
        // to handle other platforms is necessary.
        if (type_info == typeid(exception<ErrorV>)) {
          // set `state::invalid` in internals of `seastar::future` to not
          // `report_failed_future()` during `operator=()`.
          std::move(result).get_exception();
          result = std::forward<ErrorVisitorT>(errfunc)(e);
        }
      }

      auto get_result() && {
        return std::move(result);
      }
    };

    using base_t::base_t;

    [[gnu::always_inline]]
    future(base_t&& base)
      : base_t(std::move(base)) {
    }

  public:
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
      return this->then_wrapped(
        [ valfunc = std::forward<ValueFuncT>(valfunc),
          errfunc = std::forward<ErrorVisitorT>(errfunc)
        ] (auto future) mutable {
          using futurator_t = \
            seastar::futurize<std::result_of_t<ValueFuncT(ValuesT&&...)>>;
          if (__builtin_expect(future.failed(), false)) {
            maybe_handle_error_t<ErrorVisitorT, futurator_t> maybe_handle_error(
              std::forward<ErrorVisitorT>(errfunc),
              std::move(future).get_exception()
            );
            (maybe_handle_error(WrappedAllowedErrorsT::instance) , ...);
            return std::move(maybe_handle_error).get_result();
          } else {
            return futurator_t::apply(std::forward<ValueFuncT>(valfunc),
                                      std::move(future).get());
          }
        });
    }

    template <class Func>
    void then(Func&&) = delete;

    friend errorator<WrappedAllowedErrorsT...>;
  };

  template <class... ValuesT>
  static future<ValuesT...> its_error_free(seastar::future<ValuesT...>&& plain_future) {
    return future<ValuesT...>(std::move(plain_future));
  }
}; // class errorator

namespace ct_error {
  using enoent = unthrowable_wrapper<_impl::ct_error::enoent>;
  using enodata = unthrowable_wrapper<_impl::ct_error::enodata>;
  using invarg = unthrowable_wrapper<_impl::ct_error::invarg>;
}

} // namespace crimson
