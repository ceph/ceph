// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <exception>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/asio/co_spawn.hpp>

#include "common/async/blocked_completion.h"
#include "common/async/concepts.h"
#include "common/async/yield_context.h"

#include "common/dout.h"
#include "common/dout_fmt.h"
#include "common/error_code.h"

#include "rgw/rgw_asio_thread.h"

/// \file rgw/async_utils
///
/// \brief Utilities for asynchrony

namespace rgw {

namespace asio = boost::asio;
namespace async = ceph::async;

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template<asio::execution::executor Executor,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<Executor, AwaitableExecutor>
inline int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  Executor executor, ///< Executor on which to run the coroutine
  asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
  std::string* what ///< Where to store the result of `what()` on exception
  ) noexcept
{
  try {
    maybe_warn_about_blocking(dpp);
    asio::co_spawn(executor, std::move(coro), async::use_blocked);
  } catch (const std::exception&) {
    return ceph::from_exception(std::current_exception(), what);
  }
  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template<async::execution_context ExecutionContext,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
inline int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  ExecutionContext& execution_context, ///< Execution context on which to run
                                      ///  the coroutine
  asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
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
template<asio::execution::executor Executor,
	 typename T,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<Executor, AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  Executor executor, ///< Executor on which to run the coroutine
  asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
  T& val, ///< Where to store the returned value
  std::string* what ///< Where to store the result of `what()`.
  ) noexcept
{
  try {
    val = asio::co_spawn(executor, std::move(coro), async::use_blocked);
    maybe_warn_about_blocking(dpp);
  } catch (const std::exception& e) {
    return ceph::from_exception(std::current_exception(), what);
  }
  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template<async::execution_context ExecutionContext,
	 typename T,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  ExecutionContext& execution_context, ///< Execution context on which to run the coroutine
  asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
  T& val, ///< Where to store the returned value
  std::string* what ///< Where to store the result of `what()`.
  ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro), val, what);
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return multiple
/// values as a tuple.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template<asio::execution::executor Executor,
	 asio::execution::executor AwaitableExecutor,
	 typename ...Ts>
requires std::is_convertible_v<Executor, AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  Executor executor, ///< Executor on which to run the coroutine
  asio::awaitable<std::tuple<Ts...>, AwaitableExecutor> coro, ///< The coroutine itself
  std::tuple<Ts&...>&& vals, ///< Supply with std::tie
  std::string* what ///< Where to store the result of `what()`.
  ) noexcept
{
  try {
    maybe_warn_about_blocking(dpp);
    vals = asio::co_spawn(executor, std::move(coro), async::use_blocked);
  } catch (const std::exception& e) {
    return ceph::from_exception(std::current_exception(), what);
  }
  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template<async::execution_context ExecutionContext,
	 typename... Ts,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, //< In case we're blocking
  ExecutionContext& execution_context, ///< Execution context on which to run the coroutine
  asio::awaitable<std::tuple<Ts...>, AwaitableExecutor> coro, ///< The coroutine itself
  std::tuple<Ts&...>&& vals, ///< Supply with std::tie
  std::string* what ///< Where to store the result of `what()`.
  ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro),
		  std::move(vals), what);
}

/// Call a C++ coroutine from a stacful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string.
template<asio::execution::executor Executor,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<Executor, AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  const Executor& executor, ///< Executor on which to run the coroutine
  asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  try {
    if (y) {
      auto& yield = y.get_yield_context();
      asio::co_spawn(yield.get_executor(), std::move(coro), yield);
    } else {
      maybe_warn_about_blocking(dpp);
      asio::co_spawn(executor, std::move(coro), async::use_blocked);
    }
  } catch (const std::exception& e) {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, e.what());
    return ceph::from_exception(std::current_exception());
  }
  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin
template<async::execution_context ExecutionContext,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
inline int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  ExecutionContext& execution_context, ///< Execution context on which to run
                                      ///  the coroutine
  asio::awaitable<void, AwaitableExecutor> coro, ///< The coroutine itself
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro), name, y,
		  log_level);
}


/// Call a C++ coroutine from a stackful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string. This overload
/// supports coroutines that return something other than void.
template<asio::execution::executor Executor,
	 typename T,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<Executor, AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  Executor executor, ///< Executor on which to run the coroutine
  asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
  T& val, ///< Where to store the returned value
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  try {
    if (y) {
      auto& yield = y.get_yield_context();
      val = asio::co_spawn(yield.get_executor(), std::move(coro), yield);
    } else {
      maybe_warn_about_blocking(dpp);
      val = asio::co_spawn(executor, std::move(coro), async::use_blocked);
    }
  } catch (const std::exception& e) {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, e.what());
    return ceph::from_exception(std::current_exception());
  }

  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
template<async::execution_context ExecutionContext,
	 typename T,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  ExecutionContext& execution_context, ///< Execution context on which to run the coroutine
  asio::awaitable<T, AwaitableExecutor> coro, ///< The coroutine itself
  T& val, ///< Where to store the returned value
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro), val, name,
		  y, log_level);
}

/// Call a C++ coroutine from a stackful coroutine if we can, but block
/// if we get `null_yield.` handling exceptions by returning negative
/// error codes and passing out the `what()` string. This overload
/// supports coroutines that return multiple values with a tuple.
template<asio::execution::executor Executor,
	 asio::execution::executor AwaitableExecutor,
	 typename ...Ts>
requires std::is_convertible_v<Executor, AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  Executor executor, ///< Executor on which to run the coroutine
  asio::awaitable<std::tuple<Ts...>, AwaitableExecutor> coro, ///< The coroutine itself
  std::tuple<Ts&...>&& vals, ///< Supply with std::tie
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  try {
    if (y) {
      auto& yield = y.get_yield_context();
      vals = asio::co_spawn(yield.get_executor(), std::move(coro), yield);
    } else {
      maybe_warn_about_blocking(dpp);
      vals = asio::co_spawn(executor, std::move(coro), async::use_blocked);
    }
  } catch (const std::exception& e) {
    ldpp_dout_fmt(dpp, log_level, "{}: failed: {}", name, e.what());
    return ceph::from_exception(std::current_exception());
  }
  return 0;
}

/// Call a coroutine and block until it completes, handling exceptions
/// by returning negative error codes and passing out the `what()`
/// string. This overload supports coroutines that return something
/// other than void.
///
/// Intended for use in interactive front-ends, e.g. radosgw-admin.
template<async::execution_context ExecutionContext,
	 typename... Ts,
	 asio::execution::executor AwaitableExecutor>
requires std::is_convertible_v<typename ExecutionContext::executor_type,
			       AwaitableExecutor>
int run_coro(
  const DoutPrefixProvider* dpp, /// For logging
  ExecutionContext& execution_context, ///< Execution context on which to run the coroutine
  asio::awaitable<std::tuple<Ts...>, AwaitableExecutor> coro, ///< The coroutine itself
  std::tuple<Ts&...>&& vals, ///< Supply with std::tie
  std::string_view name, ///< Name, for logging errors
  optional_yield y, /// Stackful coroutine context…hopefully
  int log_level = 5 /// What level to log at
  ) noexcept
{
  return run_coro(dpp, execution_context.get_executor(), std::move(coro),
		  std::move(vals), name, y, log_level);
}
}
