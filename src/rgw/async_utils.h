// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cassert>
#include <chrono>
#include <deque>
#include <exception>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <boost/asio/execution/executor.hpp>

#include <boost/asio/experimental/cancellation_condition.hpp>
#include <boost/asio/experimental/parallel_group.hpp>

#include <boost/asio/append.hpp>
#include <boost/asio/associated_immediate_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

#include <boost/asio/experimental/promise.hpp>
#include <boost/asio/experimental/use_promise.hpp>

#include "include/function2.hpp"
#include "include/scope_guard.h"

#include "common/async/blocked_completion.h"
#include "common/async/concepts.h"
#include "common/async/redirect_error.h"
#include "common/async/yield_context.h"

#include "common/ceph_time.h"
#include "common/dout.h"
#include "common/dout_fmt.h"
#include "common/error_code.h"

#include "rgw/rgw_asio_thread.h"

/// \file rgw/async_utils
///
/// \brief Utilities for asynchrony


namespace rgw {
/// \defgroup CoroBridge Coroutine bridging
///
/// Bridging adaptors between different flavors of coroutines.
/// @{

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template <
    boost::asio::execution::executor Executor,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
inline int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    Executor executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
    std::string* what ///< Where to store the result of `what()` on exception
    ) noexcept
{
  std::exception_ptr e;
  maybe_warn_about_blocking(dpp);
  boost::asio::co_spawn(executor, std::move(coro), ceph::async::use_blocked[e]);
  return ceph::from_exception(e, what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template <
    ceph::async::execution_context ExecutionContext,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
inline int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    ExecutionContext& execution_context, ///< Execution context on which to run
    ///  the coroutine
    boost::asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
    std::string* what ///< Where to store the result of `what()` on exception
    ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro), what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template <
    boost::asio::execution::executor Executor,
    typename T,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    Executor executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
    T& val, ///< Where to store the returned value
    std::string* what ///< Where to store the result of `what()`.
    ) noexcept
{
  std::exception_ptr e;
  maybe_warn_about_blocking(dpp);
  val = boost::asio::co_spawn(
      executor, std::move(coro), ceph::async::use_blocked[e]);
  return ceph::from_exception(e, what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template <
    ceph::async::execution_context ExecutionContext,
    typename T,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    ExecutionContext&
        execution_context, ///< Execution context on which to run the coroutine
    boost::asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
    T& val, ///< Where to store the returned value
    std::string* what ///< Where to store the result of `what()`.
    ) noexcept
{
  return run_coro(
      dpp, execution_context.get_executor(), std::move(coro), val, what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return multiple
/// values as a tuple.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template <
    boost::asio::execution::executor Executor,
    boost::asio::execution::executor AwaitableExecutor,
    typename... Ts>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    Executor executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<std::tuple<Ts...>, AwaitableExecutor>
        coro, ///< The coroutine itself
    std::tuple<Ts&...>&& vals, ///< Supply with std::tie
    std::string* what ///< Where to store the result of `what()`.
    ) noexcept
{
  std::exception_ptr e;
  maybe_warn_about_blocking(dpp);
  vals = boost::asio::co_spawn(
      executor, std::move(coro), ceph::async::use_blocked[e]);
  return ceph::from_exception(e, what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template <
    ceph::async::execution_context ExecutionContext,
    typename... Ts,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, //< In case we're blocking
    ExecutionContext&
        execution_context, ///< Execution context on which to run the coroutine
    boost::asio::awaitable<std::tuple<Ts...>, AwaitableExecutor>
        coro, ///< The coroutine itself
    std::tuple<Ts&...>&& vals, ///< Supply with std::tie
    std::string* what ///< Where to store the result of `what()`.
    ) noexcept
{
  return run_coro(
      dpp, execution_context.get_executor(), std::move(coro), std::move(vals),
      what);
}

/// Call a C++ coroutine from a stackful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string.
template <
    boost::asio::execution::executor Executor,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    const Executor& executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  std::exception_ptr e;
  if (y) {
    auto& yield = y.get_yield_context();
    boost::asio::co_spawn(
        yield.get_executor(), std::move(coro),
        ceph::async::redirect_error(yield, e));
  } else {
    maybe_warn_about_blocking(dpp);
    boost::asio::co_spawn(
        executor, std::move(coro), ceph::async::use_blocked[e]);
  }
  std::string what;
  auto r = ceph::from_exception(e, &what);
  if (e) [[unlikely]] {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, what);
  }
  return r;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template <
    ceph::async::execution_context ExecutionContext,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
inline int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    ExecutionContext& execution_context, ///< Execution context on which to run
    ///  the coroutine
    boost::asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  return run_coro(
      dpp, execution_context.get_executor(), std::move(coro), name, y,
      log_level);
}

/// Call a C++ coroutine from a stackful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string. This overload
/// supports coroutines that return something other than void.
template <
    boost::asio::execution::executor Executor,
    typename T,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    Executor executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
    T& val, ///< Where to store the returned value
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  std::exception_ptr e;
  if (y) {
    auto& yield = y.get_yield_context();
    val = boost::asio::co_spawn(
        yield.get_executor(), std::move(coro),
        ceph::async::redirect_error(yield, e));
  } else {
    maybe_warn_about_blocking(dpp);
    val = boost::asio::co_spawn(
        executor, std::move(coro), ceph::async::use_blocked[e]);
  }
  std::string what;
  auto r = ceph::from_exception(e, &what);
  if (e) [[unlikely]] {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, what);
  }
  return r;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
template <
    ceph::async::execution_context ExecutionContext,
    typename T,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    ExecutionContext&
        execution_context, ///< Execution context on which to run the coroutine
    boost::asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
    T& val, ///< Where to store the returned value
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  return run_coro(
      dpp, execution_context.get_executor(), std::move(coro), val, name, y,
      log_level);
}

/// Call a C++ coroutine from a stackful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string. This overload
/// supports coroutines that return multiple values with a tuple.
template <
    boost::asio::execution::executor Executor,
    boost::asio::execution::executor AwaitableExecutor,
    typename... Ts>
  requires std::is_convertible_v<Executor, AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    Executor executor, ///< Executor on which to run the coroutine
    boost::asio::awaitable<std::tuple<Ts...>, AwaitableExecutor>
        coro, ///< The coroutine itself
    std::tuple<Ts&...>&& vals, ///< Supply with std::tie
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  std::exception_ptr e;
  if (y) {
    auto& yield = y.get_yield_context();
    vals = boost::asio::co_spawn(
        yield.get_executor(), std::move(coro),
        ceph::async::redirect_error(yield, e));
  } else {
    maybe_warn_about_blocking(dpp);
    vals = boost::asio::co_spawn(
        executor, std::move(coro), ceph::async::use_blocked[e]);
  }
  std::string what;
  auto r = ceph::from_exception(e, &what);
  if (e) [[unlikely]] {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, what);
  }
  return r;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template <
    ceph::async::execution_context ExecutionContext,
    typename... Ts,
    boost::asio::execution::executor AwaitableExecutor>
  requires std::is_convertible_v<
      typename ExecutionContext::executor_type,
      AwaitableExecutor>
int
run_coro(
    const DoutPrefixProvider* dpp, /// For logging
    ExecutionContext&
        execution_context, ///< Execution context on which to run the coroutine
    boost::asio::awaitable<std::tuple<Ts...>, AwaitableExecutor>
        coro, ///< The coroutine itself
    std::tuple<Ts&...>&& vals, ///< Supply with std::tie
    std::string_view name, ///< Name, for logging errors
    optional_yield y, /// Stackful coroutine context…hopefully
    int log_level = 5 /// What level to log at
    ) noexcept
{
  return run_coro(
      dpp, execution_context.get_executor(), std::move(coro), std::move(vals),
      name, y, log_level);
}

/// @}

namespace detail {
/// CompletionToken wrapping an optional_yield and a DoutPrefixProvider*
///
/// \note I thought briefly of whether I could justify asserting on
/// blocking in an asio thread. Unconditionally, since I didn't have a
/// CephContext* either.
///
/// \note Obviously, I could not.
///
/// \note ;(
struct oy_completer {
  const DoutPrefixProvider* dpp;
  optional_yield y;
};
} // namespace detail

/// Adapt `optional_yield` as a completion token for Asio operations
///
/// Rather than `use_blocked`, use this as a completion token when
/// calling to neorados or similar functions expecting one.
///
/// \warning Errors from calls made using this adaptor will throw.
///
/// For example, with neorados:
///
/// \code{.cpp}
/// try {
///   rados.execute(o, ioc, std::move(op), oyc(dpp, y));
/// } catch (sys::system_error& e) {
///   // …
/// }
///
/// \endcode
///
/// \param[in] dpp Logging for `maybe_warn_about_blocking()`
/// \param[in] y The yield_context, maybe. Maybe not.
///
/// \note I was originally going to write an `async_result`
/// specialization for optional_yield.
inline detail::oy_completer
oyc(const DoutPrefixProvider* dpp, optional_yield y)
{
  return {dpp, y};
}

/// Adapt `optional_yield` as a completion token for Asio operations,
/// with error diversion
///
/// Rather than `use_blocked`, use this as a completion token when
/// calling to neorados or similar functions expecting one.
///
/// \note This version diverts the error rather than throwing. The
/// type of the error (the `disposition` in Asio parlance) depends on
/// the operation. Basic neorados operations use
/// `boost::system::error_code`. Coroutines use `std::exception_ptr`.
///
/// An example for neorados,
/// \code{.cpp}
///
/// boost::system::error_code ec;
/// rados.execute(o, ioc, std::move(op), oyc(dpp, y, ec));
///
/// if (ec) {
///   …
/// }
/// \endcode
///
/// \param[in] dpp Logging for `maybe_warn_about_blocking()`
/// \param[in] y The yield_context, maybe. Maybe not.
/// \param[in] disposition A disposition to redirect
///
/// \note I was going to add an overload to optional_yield for
/// `operator []` to give ergonomic error redirection. Then I
/// remembered to add maybe warn about blocking and discovered it
/// required a DoutPrefixProvider*.
template<ceph::async::disposition Disposition>
inline auto
oyc(const DoutPrefixProvider* dpp, optional_yield y, Disposition& disposition)
{
  return ceph::async::redirect_error(oyc(dpp, y), disposition);
}

/// Helper type for visitors, from <https://en.cppreference.com/w/cpp/utility/variant/visit.html>
template <class... Ts>
struct overloads : Ts... {
  using Ts::operator()...;
};

/// Promise used when shutting down the SAL
using shutdown_promise =
    ::boost::asio::experimental::promise<void(std::exception_ptr)>;

/// Vector to accumulate labeled shutdown promises
using shutdown_vector = std::vector<std::pair<std::string, shutdown_promise>>;

/// \defgroup TaskHandles Handles for long-running coroutines
///
/// Handles for long-running tasks and background loops that can be
/// cancelled and joined.
/// @{

/// Type tag to construct handle with coroutine running
struct running_t {};

/// Create a handle with the coroutine running
inline constexpr running_t running{};

/// Current state of a task handle
enum class task_state {
  /// Handle is not running but can be run
  ready,
  /// Handle is running and may be canceled or joined
  running,
  /// Handle is dead and cannot be used
  dead
};

namespace detail {
template <
    typename CoroType,
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class metatask_base {
public:
  using coro_type = CoroType;
  using executor_type = Strand;
  using value_type = void;
  using promise_type =
      boost::asio::experimental::promise<void(std::exception_ptr)>;

  /// Return the executor on which the coroutine will run
  ///
  /// \returns The executor for running coroutines, a strand in this
  /// case
  executor_type
  get_executor() const
  {
    return strand;
  }

  /// \brief Return the state of the task
  ///
  /// \returns A value of type `task_state`
  task_state
  state() const
  {
    return std::visit(
        overloads{
            [](const coro_type&) { return task_state::ready; },
            [](const promise_type&) { return task_state::running; },
            [](const std::monostate&) { return task_state::dead; }},
        runnable);
  }

  /// Cancels the currently running coroutine
  ///
  /// \note Cancellation is idempotent with the same cancellation
  /// level
  ///
  /// \pre The task must be running. If `run` has not been called, or
  /// `join` or `shutdown` have been called, it is a contract
  /// violation.
  void
  cancel(
      boost::asio::cancellation_type cancellation_level =
          boost::asio::cancellation_type::all)
  {
    assert(state() == task_state::running);
    std::get<promise_type>(runnable).cancel(cancellation_level);
  }

  /// Joins the currently running coroutine
  ///
  /// \param[in] token The Asio completion token
  ///
  /// \pre The task must be running. If `run` has not been called, or
  /// `join` or `shutdown` have been called, it is a contract
  /// violation.
  ///
  /// \returns Value appropriate to the completion token.
  template<boost::asio::completion_token_for<void(std::exception_ptr)> CompletionToken>
  auto
  join(CompletionToken&& token)
  {
    assert(state() == task_state::running);
    scope_guard _{[this] { runnable.template emplace<std::monostate>(); }};
    // Extending the promise's lifetime through the execution of the handler.
    auto p = std::make_shared<promise_type>(std::get<promise_type>(std::move(runnable)));
    return (*p)(boost::asio::consign(std::forward<CompletionToken>(token), p));
  }

  /// Extracts the completion of the running coroutine for shutdown
  ///
  /// \note This function is intended to hook into the SAL shutdown
  /// flow that accumulates multiple promises to wait on.
  ///
  /// \pre The task must be running. If `run` has not been called, or
  /// `join` or `shutdown` have been called, it is a contract
  /// violation.
  void
  shutdown(std::string label, shutdown_vector& to_wait)
  {
    assert(state() == task_state::running);
    scope_guard _{[this] { runnable.template emplace<std::monostate>(); }};
    to_wait.emplace_back(std::move(label), std::get<promise_type>(std::move(runnable)));
  }

  metatask_base(const metatask_base&) = delete;
  metatask_base& operator =(const metatask_base&) = delete;
  metatask_base(metatask_base&&) = delete;
  metatask_base& operator =(metatask_base&&) = delete;

protected:
  template <typename... Args>
  metatask_base(Strand strand, Args&&... args) :
    strand(strand), runnable(std::forward<Args>(args)...)
  {}

  Strand strand;
  std::variant<promise_type, coro_type, std::monostate> runnable;
};

template <
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class task_base : public metatask_base<boost::asio::awaitable<void>, Strand> {
  using Base = metatask_base<boost::asio::awaitable<void>, Strand>;
protected:
  using Base::runnable;
  using Base::strand;
public:
  using typename Base::coro_type;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// Starts the coroutine running
  ///
  /// \pre The task may not be running and may not have run before. If
  /// `run()` has been called before or the `running` version of the
  /// constructor was called, it is a contract violation.
  void
  run()
  {
    using boost::asio::experimental::use_promise;
    assert(state() == task_state::ready);
    runnable.template emplace<promise_type>(boost::asio::co_spawn(
        strand, std::get<coro_type>(std::move(runnable)),
        boost::asio::bind_executor(strand, use_promise)));
    assert(state() == task_state::running);
  }

  /// \brief Construct a task_base from an awaitable, ready
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The awaitable for the task
  task_base(Strand strand, coro_type&& coro) :
    Base(strand, std::in_place_type<coro_type>, std::move(coro))
  {}

  /// \brief Construct a task_base from a promise, running
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] promise The promise for the currently running task
  task_base(Strand strand, promise_type&& promise) :
    Base(strand, std::in_place_type<promise_type>, std::move(promise))
  {}

  /// \brief Construct an empty task_base, dead
  ///
  /// \param[in] strand The strand on which to run
  task_base(Strand strand) :
    Base(strand, std::monostate{})
  {}
};

template <
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class ytask_base : public metatask_base<fu2::unique_function<void(boost::asio::yield_context)>, Strand> {
  using Base = metatask_base<fu2::unique_function<void(boost::asio::yield_context)>, Strand>;
protected:
  using Base::runnable;
  using Base::strand;
public:
  using typename Base::coro_type;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// Starts the coroutine running
  ///
  /// \pre The task may not be running and may not have run before. If
  /// `run()` has been called before or the `start_running` version of
  /// the constructor was called, it is a contract violation.
  void
  run()
  {
    using boost::asio::experimental::use_promise;
    assert(state() == task_state::ready);
    runnable.template emplace<promise_type>(boost::asio::spawn(
        boost::asio::any_io_executor{strand},
        std::get<coro_type>(std::move(runnable)),
        boost::asio::bind_executor(strand, use_promise)));
    assert(state() == task_state::running);
  }

  /// \brief Construct a ytask_base from a function, ready
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The function for the task
  ytask_base(Strand strand, coro_type&& coro) :
    Base(strand, std::in_place_type<coro_type>, std::move(coro))
  {}

  /// \brief Construct a ytask_base from a promise, running
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] promise The promise for the running task
  ytask_base(Strand strand, promise_type&& promise) :
    Base(strand, std::in_place_type<promise_type>, std::move(promise))
  {}

  /// \brief Construct an empty ytask_base, dead
  ///
  /// \param[in] strand The strand on which to run
  ytask_base(Strand strand) :
    Base(strand, std::monostate{}) {}
};
} // namespace detail

/// \brief Handle for long-running tasks
///
/// Example:
/// ```
/// rgw::task task{boost::asio::make_strand(co_await boost::asio::this_coro::executor),
///                some_coroutine(), rgw::running};
/// do_some();
/// other_things();
/// co_await task.join(asio::use_awaitable); // Wait for completion.
/// ```
///
/// \warning This class makes no attempt at serialization. Use a
/// strand or mutex if you need that.
template <
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class task final : public detail::task_base<Strand> {
  using Base = detail::task_base<Strand>;
public:
  using typename Base::coro_type;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::run;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// \brief Construct a task from an awaitable, ready to be run
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The awaitable for the task
  task(Strand strand, coro_type coro) :
    Base(strand, std::move(coro))
  {
    assert(state() == task_state::ready);
  }

  /// \brief Construct a task from a coroutine function, ready to be run
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the task
  template <std::invocable<> CoroFun>
    requires(std::is_same_v<std::invoke_result_t<CoroFun>, coro_type>)
  task(Strand strand, CoroFun&& coro_fun) :
    Base(strand, std::invoke(std::forward<CoroFun>(coro_fun)))
  {
    assert(state() == task_state::ready);
  }

  /// \brief Construct a running task from an awaitable
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The awaitable for the task
  task(Strand strand, coro_type coro, running_t /**< Type tag */) :
    Base(strand,
        boost::asio::co_spawn(
            strand,
            std::move(coro),
            boost::asio::bind_executor(
                strand,
                boost::asio::experimental::use_promise)))
  {
    assert(state() == task_state::running);
  }

  /// \brief Construct a running task from a coroutine function
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the task
  template <std::invocable<> CoroFun>
    requires(std::is_same_v<std::invoke_result_t<CoroFun>, coro_type>)
  task(Strand strand, CoroFun&& coro_fun, running_t /**< Type tag */) :
    Base(strand,
        boost::asio::co_spawn(
            strand,
            std::invoke(std::forward<CoroFun>(coro_fun)),
            boost::asio::bind_executor(
                strand,
                boost::asio::experimental::use_promise)))
  {
    assert(state() == task_state::running);
  }
};

/// \brief Handle for periodic maintenance tasks
///
/// The supplied function returns an optional containing its intended
/// delay. If the optional is empty, the loop will exit.
///
/// Example:
///
/// ```
/// asio::awaitable<ceph::timespan>
/// periodic()
/// {
///   co_await maintenance();
///   co_return 3s;
/// }
/// ```
///
/// ```
/// rgw::run_loop loop{boost::asio::make_strand(co_await boost::asio::this_coro::executor),
///                    &periodic, rgw::running};
/// co_await make_work();
/// loop.wake();
/// other_things();
/// loop.cancel();
/// co_await loop.join(asio::use_awaitable); // Wait for completion.
/// ```
///
/// \warning This class makes no attempt at serialization. Use a
/// strand or mutex if you need that.
template <
    typename Rep = ceph::rep,
    typename Ratio = std::nano,
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class run_loop final : public detail::task_base<Strand> {
  using Base = detail::task_base<Strand>;
  using Base::runnable;
  using Base::strand;
  using timer_t = boost::asio::steady_timer;
public:
  using duration = std::chrono::duration<Rep, Ratio>;
  using coro_type = boost::asio::awaitable<duration>;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::run;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// \brief Construct a run_loop from a coroutine function, ready to be run
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the loop. It must return
  ///                     its intended delay before being re-run
  template <std::invocable<> CoroFun>
    requires(std::is_same_v<std::invoke_result_t<CoroFun>, coro_type>)
  run_loop(Strand strand, CoroFun&& coro_fun) :
    Base(strand)
  {
    runnable.template emplace<typename Base::coro_type>(loop(std::forward<CoroFun>(coro_fun), timer_));
    assert(state() == task_state::ready);
  }

  /// \brief Construct a running run_loop from a coroutine function
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the loop. It must return
  ///                     its intended delay before being re-run
  template <std::invocable<> CoroFun>
    requires(std::is_same_v<std::invoke_result_t<CoroFun>, coro_type>)
  run_loop(Strand strand, CoroFun&& coro_fun, running_t /**< Type tag */) :
    Base(strand)
  {
    runnable.template emplace<promise_type>(boost::asio::co_spawn(
        strand, loop(std::forward<CoroFun>(coro_fun), timer_),
        boost::asio::bind_executor(
            strand, boost::asio::experimental::use_promise)));
    assert(state() == task_state::running);
  }

  /// \brief Wake a waiting task
  ///
  /// \note This wakes the task only when sleeping in the pause between runs.
  void
  wake()
  {
    assert(state() == task_state::running);
    boost::asio::dispatch(strand, [timer = this->timer_] {
      timer->cancel();
    });
  }

private:

  template <std::invocable<> CoroFun>
    requires(std::is_same_v<std::invoke_result_t<CoroFun>, coro_type>)
  static boost::asio::awaitable<void>
  loop(CoroFun coro_fun, std::shared_ptr<timer_t> timer)
  {
    try {
      // GCC 14 has a bug related to parenthesized `co_await` expressions.
      for (auto state = co_await boost::asio::this_coro::cancellation_state;
           state.cancelled() == boost::asio::cancellation_type::none;
           state = co_await boost::asio::this_coro::cancellation_state) {
        auto delay = co_await coro_fun();
        timer->expires_after(delay);
        try {
          co_await timer->async_wait(boost::asio::use_awaitable);
        } catch (boost::system::system_error& e) {
          // If the coroutine has been canceled we'll exit when we hit
          // the loop condition. Otherwise, we've been awoken.
          if (e.code() != boost::asio::error::operation_aborted) {
            throw;
          }
        }
      }
    } catch (boost::system::system_error& e) {
      // Since cancellation is an expected way to exit, it is not
      // exceptional.
      if (e.code() != boost::asio::error::operation_aborted) {
        throw;
      }
    }
  }

  // This is a shared pointer so the coroutine handle itself can own a
  // reference and promise cancellation through the `run_loop`
  // destructor will propagate to the timer properly.
  std::shared_ptr<timer_t> timer_ = std::make_shared<timer_t>(get_executor());
};

/// \brief Handle for long-running tasks, stackful version
///
/// Example:
/// ```
/// rgw::ytask task{boost::asio::make_strand(y.get_executor()),
///                 &some_coroutine, rgw::running};
/// do_some();
/// other_things();
/// join(y); // Wait for completion.
/// ```
///
/// \warning This class makes no attempt at serialization. Use a
/// strand or mutex if you need that.
template <
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class ytask final : public detail::ytask_base<Strand> {
  using Base = detail::ytask_base<Strand>;
public:
  using typename Base::coro_type;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::run;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// \brief Construct a ytask from a stackful coroutine function, ready to be run
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The stackful coroutine function for the task
  template <std::invocable<boost::asio::yield_context> Coro>
    requires(std::is_void_v<
                std::invoke_result_t<Coro, boost::asio::yield_context>>)
  ytask(Strand strand, Coro&& coro) :
    Base(strand, std::forward<Coro>(coro))
  {
    assert(state() == task_state::ready);
  }

  /// \brief Construct a running ytask from a stackful coroutine function
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro The stackful coroutine function for the task
  template <std::invocable<boost::asio::yield_context> Coro>
    requires(
        std::is_void_v<std::invoke_result_t<Coro, boost::asio::yield_context>>)
  ytask(Strand strand, Coro&& coro, running_t) :
    Base(
        strand,
        boost::asio::spawn(
            boost::asio::any_io_executor{strand},
            std::forward<Coro>(coro),
            boost::asio::bind_executor(
                strand,
                boost::asio::experimental::use_promise)))
  {
    assert(state() == task_state::running);
  }
};

/// \brief Handle for periodic maintenance tasks, stackful version
///
/// Example:
///
/// ```
/// ceph::timespan
/// periodic(asio::yield_context y)
/// {
///   maintenance(y);
///   return 3s;
/// }
/// ```
///
/// ```
/// rgw::yrun_loop loop{asio::make_strand(y.get_executor()),
///                     &periodic, rgw::running};
/// make_work(y);
/// loop.wake();
/// other_things();
/// loop.cancel();
/// loop.join(y); // Wait for completion.
/// ```
///
/// \warning This class makes no attempt at serialization. Use a
/// strand or mutex if you need that.
template <
    typename Rep = ceph::rep,
    typename Ratio = std::nano,
    ceph::async::strand Strand = boost::asio::strand<boost::asio::any_io_executor>>
class yrun_loop final : public detail::ytask_base<Strand> {
  using Base = detail::ytask_base<Strand>;
  using Base::runnable;
  using Base::strand;
  using timer_t = boost::asio::steady_timer;
public:
  using duration = std::chrono::duration<Rep, Ratio>;
  using typename Base::executor_type;
  using typename Base::value_type;
  using typename Base::promise_type;

  using Base::get_executor;
  using Base::state;
  using Base::run;
  using Base::cancel;
  using Base::join;
  using Base::shutdown;

  /// \brief Construct a yrun_loop from a stackful coroutine, ready to
  /// be run
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the loop. It must return
  ///                     its intended delay before being re-run
  template <std::invocable<boost::asio::yield_context> CoroFun>
    requires(std::is_convertible_v<
             std::invoke_result_t<CoroFun, boost::asio::yield_context>,
             duration>)
  yrun_loop(Strand strand, CoroFun&& coro_fun) :
    Base(strand)
  {
    runnable.template emplace<typename Base::coro_type>(
        [timer = timer_, coro_fun = std::forward<CoroFun>(coro_fun)](
            boost::asio::yield_context y) mutable {
          loop(std::forward<CoroFun>(coro_fun), std::move(timer), y);
        });
    assert(state() == task_state::ready);
  }

  /// \brief Construct a running yrun_loop from a coroutine function
  ///
  /// \param[in] strand The strand on which to run
  /// \param[in] coro_fun The coroutine function for the loop. It must return
  ///                     its intended delay before being re-run
  template <std::invocable<boost::asio::yield_context> CoroFun>
    requires(std::is_convertible_v<
             std::invoke_result_t<CoroFun, boost::asio::yield_context>,
             duration>)
  yrun_loop(Strand strand, CoroFun&& coro_fun, running_t /**< Type tag */) :
    Base(strand)
  {
    runnable.template emplace<promise_type>(boost::asio::spawn(
        boost::asio::any_io_executor(strand),
        [timer = timer_, coro_fun = std::forward<CoroFun>(coro_fun)](
            boost::asio::yield_context y) mutable {
          loop(std::forward<CoroFun>(coro_fun), std::move(timer), y);
        },
        boost::asio::bind_executor(
            strand, boost::asio::experimental::use_promise)));
    assert(state() == task_state::running);
  }

  /// \brief Wake a waiting task
  ///
  /// \note This wakes the task only when sleeping in the pause between runs.
  void
  wake()
  {
    assert(state() == task_state::running);
    boost::asio::dispatch(strand, [timer = timer_] {
      timer->cancel();
    });
  }

private:
  template <std::invocable<boost::asio::yield_context> CoroFun>
    requires(std::is_convertible_v<
             std::invoke_result_t<CoroFun, boost::asio::yield_context>,
             duration>)
  static void
  loop(
      CoroFun coro_fun,
      std::shared_ptr<timer_t> timer,
      boost::asio::yield_context y)
  {
    try {
      while (y.get_cancellation_state().cancelled() ==
             boost::asio::cancellation_type::none) {
        auto delay = coro_fun(y);
        timer->expires_after(delay);
        try {
          timer->async_wait(y);
        } catch (boost::system::system_error& e) {
          // If the coroutine has been canceled we'll exit when we hit
          // the while condition. Otherwise, we've been awoken.
          if (e.code() != boost::asio::error::operation_aborted) {
            throw;
          }
        }
      }
    } catch (boost::system::system_error& e) {
      // Since cancellation is the only way we exit, it is not
      // exceptional.
      if (e.code() != boost::asio::error::operation_aborted) {
        throw;
      }
    }
  }

  // This is a shared pointer so the coroutine handle itself can own a
  // reference and promise cancellation through the `run_loop`
  // destructor will propagate to the timer properly.
  std::shared_ptr<timer_t> timer_ = std::make_shared<timer_t>(get_executor());
};
/// @}

/// \brief Convenience to wait within a C++20 coroutine
///
/// @param[in] delay Time to wait
template <typename Rep, typename Period>
boost::asio::awaitable<void>
wait_for(std::chrono::duration<Rep, Period> delay)
{
  boost::asio::steady_timer timer{
      co_await boost::asio::this_coro::executor, delay};
  co_await timer.async_wait(boost::asio::use_awaitable);
}

/// \brief Convenience to wait within a stackful coroutine
///
/// @param[in] delay Time to wait
/// @param[in] y Yield context
template <typename Rep, typename Period>
void
wait_for(std::chrono::duration<Rep, Period> delay, boost::asio::yield_context y)
{
  boost::asio::steady_timer timer{y.get_executor(), delay};
  timer.async_wait(y);
}

/// \brief Return a function object binding a member function to an
/// object
///
/// \param[in] obj The object to bind
/// \param[in] pm The member function to be bound
template <typename M, typename T, typename U>
  requires(std::is_base_of_v<U, T>)
auto
membind(T& obj, M U::* pm)
{
  return std::bind_front(std::mem_fn(pm), std::ref(obj));
}

template <
    boost::asio::execution::executor Executor,
    boost::asio::completion_token_for<void(std::exception_ptr)> CompletionToken>
auto
await_shutdowns(
    const DoutPrefixProvider* dpp,
    Executor executor,
    shutdown_vector&& to_wait,
    CompletionToken&& token)
{
  namespace asio = boost::asio;
  using asio::experimental::make_parallel_group;
  using asio::experimental::wait_for_all;
  auto e = asio::get_associated_executor(token, executor);
  auto consigned = asio::consign(
      std::forward<CompletionToken>(token), boost::asio::make_work_guard(e));
  return asio::async_initiate<decltype(consigned), void(std::exception_ptr)>(
      [e, dpp, to_wait = std::move(to_wait)](auto handler) mutable {
        if (to_wait.empty()) [[unlikely]] {
          auto i = asio::get_associated_immediate_executor(handler, e);
          asio::dispatch(
              i, asio::append(std::move(handler), std::exception_ptr{}));
          return;
        }
        std::vector<std::string> labels;
        // Due to reallocation, std::vector won't use a move
        // constructor that isn't noexcept, and `executor_binder`'s
        // move constructor isn't, seemingly as an oversight.
        //
        // This worked fine with views because `to_vector` knew a size
        // in advance and so there was no potential reallocation
        // codepath. Heck Jammy.
        std::deque<decltype(boost::asio::bind_executor(
            e, std::declval<shutdown_promise>()))>
            promises;
        for (auto&& [label, promise] : to_wait) {
          labels.emplace_back(std::move(label));
          // `make_parallel_group` requires `get_executor()` which promises
          // do not have.
          promises.emplace_back(asio::bind_executor(e, std::move(promise)));
        }
        // This way we can keep it alive and not have all the promises
        // get canceled.
        auto pg = std::make_shared<decltype(make_parallel_group(std::move(promises)))>(
            make_parallel_group(std::move(promises)));
        pg->async_wait(
            wait_for_all{},
            [e, dpp, pg, labels = std::move(labels),
             handler = std::move(handler)](
                std::vector<std::size_t> completion_order,
                std::vector<std::exception_ptr> exceptions) mutable {
              auto what = [](std::exception_ptr e) {
                if (e)
                  try {
                    std::rethrow_exception(e);
                  } catch (const std::exception& e) {
                    return std::string(e.what());
                  } catch (...) {
                    return std::string(
                        "Exception not descended from std::exception.");
                  }
                return std::string("No error.");
              };
              auto is_cancellation = [](std::exception_ptr e) {
                if (e)
                  try {
                    std::rethrow_exception(e);
                  } catch (const boost::system::system_error& e) {
                    return (e.code() == asio::error::operation_aborted);
                  } catch (...) {
                  }
                return false;
              };
              std::exception_ptr returned_error;
              for (auto& idx : completion_order) {
                auto label = std::move(labels[idx]);
                auto eptr = std::move(exceptions[idx]);
                if (eptr && !is_cancellation(eptr)) {
                  ldpp_dout(dpp, 1)
                      << "Shutting down " << label << " failed with exception "
                      << what(eptr) << dendl;
                  if (!returned_error) {
                    returned_error = std::move(eptr);
                  }
                }
              }
              asio::post(
                  e,
                  asio::append(std::move(handler), std::move(returned_error)));
            });
      },
      consigned);
}
} // namespace rgw

namespace boost::asio {
template <typename Signature>
struct async_result<rgw::detail::oy_completer, Signature> {
  template <typename Initiation, typename RawCompletionToken, typename... Args>
  static auto
  initiate(Initiation&& initiation, RawCompletionToken&& token, Args&&... args)
  {
    if (token.y) {
      return async_result<yield_context, Signature>::initiate(
          std::forward<Initiation>(initiation), token.y.get_yield_context(),
          std::forward<Args>(args)...);
    } else {
      maybe_warn_about_blocking(token.dpp);
      return async_result<ceph::async::use_blocked_t, Signature>::initiate(
          std::forward<Initiation>(initiation), ceph::async::use_blocked,
          std::forward<Args>(args)...);
    }
  }
};
} // namespace boost::asio
