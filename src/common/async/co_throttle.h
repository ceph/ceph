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
/// Child coroutines must be of type awaitable<void>. Exceptions thrown by
/// children are rethrown to the parent on its next call to spawn() or wait().
/// The cancel_on_error option controls whether these exceptions errors trigger
/// the cancellation of other children.
///
/// All child coroutines are canceled by cancel() or co_throttle destruction.
/// This allows the parent coroutine to share memory with its child coroutines
/// without fear of dangling references.
///
/// This class is not thread-safe, so a strand executor should be used in
/// multi-threaded contexts.
///
/// Example:
/// \code
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
/// \endcode
template <boost::asio::execution::executor Executor>
class co_throttle {
  using impl_type = detail::co_throttle_impl<Executor>;
  boost::intrusive_ptr<impl_type> impl;

 public:
  using executor_type = Executor;
  executor_type get_executor() const noexcept { return impl->get_executor(); }

  using size_type = typename impl_type::size_type;
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

  /// Try to spawn the given coroutine \ref cr. If this would exceed the
  /// concurrency limit, wait for another coroutine to complete first. This
  /// default limit can be overridden with the optional \ref smaller_limit
  /// argument.
  ///
  /// If any spawned coroutines exit with an exception, the first exception is
  /// rethrown by the next call to spawn() or wait(). If spawn() has an
  /// exception to rethrow, it will spawn \cr first only in the case of
  /// cancel_on_error::none. New coroutines can be spawned by later calls to
  /// spawn() regardless of cancel_on_error.
  auto spawn(boost::asio::awaitable<void, executor_type> cr,
             size_type smaller_limit = max_limit)
      -> boost::asio::awaitable<void, executor_type>
  {
    return impl->spawn(std::move(cr), smaller_limit);
  }

  /// Wait for all associated coroutines to complete. If any of these coroutines
  /// exit with an exception, the first of those exceptions is rethrown.
  auto wait()
      -> boost::asio::awaitable<void, executor_type>
  {
    return impl->wait();
  }

  /// Cancel all associated coroutines.
  void cancel()
  {
    impl->cancel();
  }
};

} // namespace ceph::async
