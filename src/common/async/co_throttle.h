// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <limits>
#include <boost/intrusive_ptr.hpp>
#include "common/async/cancel_on_error.h"
#include "common/async/detail/co_throttle_impl.h"

namespace ceph::async {

/// A coroutine throttle that allows a parent coroutine to spawn and manage
/// multiple child coroutines, while enforcing an upper bound on concurrency.
///
/// Child coroutines can be of type awaitable<void> or awaitable<error_code>.
/// Error codes returned by children are reported to the parent on its next call
/// to spawn() or wait(). The cancel_on_error option controls whether these
/// errors trigger the cancellation of other children.
///
/// All child coroutines are canceled by cancel() or co_throttle destruction.
/// This allows the parent coroutine to share memory with its child coroutines
/// without fear of dangling references.
///
/// This class is not thread-safe, so a strand executor should be used in
/// multi-threaded contexts.
///
/// Example:
/// @code
/// awaitable<void> child(task& t);
///
/// awaitable<void> parent(std::span<task> tasks)
/// {
///   // process all tasks, up to 10 at a time
///   auto ex = co_await boost::asio::this_coro::executor;
///   auto throttle = co_throttle{ex, 10};
///
///   for (auto& t : tasks) {
///     co_await throttle.spawn(child(t));
///   }
///   co_await throttle.wait();
/// }
/// @endcode
template <boost::asio::execution::executor Executor>
class co_throttle {
 public:
  using executor_type = Executor;
  executor_type get_executor() const { return impl->get_executor(); }

  using size_type = uint16_t;
  static constexpr size_type max_limit = std::numeric_limits<size_type>::max();

  co_throttle(const executor_type& ex, size_type limit,
              cancel_on_error on_error = cancel_on_error::none)
    : impl(new impl_type(ex, limit, on_error))
  {
  }

  ~co_throttle()
  {
    cancel();
  }

  co_throttle(const co_throttle&) = delete;
  co_throttle& operator=(const co_throttle&) = delete;

  template <typename T>
  using awaitable = boost::asio::awaitable<T, executor_type>;

  /// Try to spawn the given coroutine. If this would exceed the concurrency
  /// limit, wait for another coroutine to complete first. This default
  /// limit can be overridden with the optional `smaller_limit` argument.
  ///
  /// If any spawned coroutines of type awaitable<error_code> return a non-zero
  /// error, the first such error is reported by the next call to spawn() or
  /// wait(). When spawn() reports these errors, the given coroutine given will
  /// only be spawned in the case of cancel_on_error::none. New coroutines can
  /// be spawned by later calls to spawn() regardless of cancel_on_error.
  ///
  /// If a spawned coroutine exits by an uncaught exception, that exception is
  /// rethrown by the next call to spawn() or wait().
  auto spawn(awaitable<boost::system::error_code> cr,
             size_type smaller_limit = max_limit)
      -> awaitable<boost::system::error_code>
  {
    return impl->spawn(std::move(cr), smaller_limit);
  }

  /// \overload
  auto spawn(awaitable<void> cr, size_type smaller_limit = max_limit)
      -> awaitable<boost::system::error_code>
  {
    return impl->spawn(std::move(cr), smaller_limit);
  }

  /// Wait for all associated coroutines to complete. If any of these coroutines
  /// return a non-zero error_code, the first of those errors is returned.
  awaitable<boost::system::error_code> wait()
  {
    return impl->wait();
  }

  /// Cancel all associated coroutines. Callers waiting on spawn() or wait()
  /// will fail with boost::asio::error::operation_aborted.
  void cancel()
  {
    impl->cancel();
  }

 private:
  using impl_type = detail::co_throttle_impl<Executor, size_type>;
  boost::intrusive_ptr<impl_type> impl;
};

} // namespace ceph::async
