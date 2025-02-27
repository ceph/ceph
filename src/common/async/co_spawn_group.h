// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/execution/executor.hpp>
#include "cancel_on_error.h"
#include "detail/co_spawn_group.h"

namespace ceph::async {

/// \brief Tracks a group of coroutines to await all of their completions.
///
/// The wait() function can be used to await the completion of all children.
/// If any child coroutines exit with an exception, the first such exception
/// is rethrown by wait(). The cancel_on_error option controls whether these
/// exceptions trigger the cancellation of other children.
///
/// All child coroutines are canceled by cancel() or co_spawn_group destruction.
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
///   // process all tasks in parallel
///   auto ex = co_await boost::asio::this_coro::executor;
///   auto group = co_spawn_group{ex, tasks.size()};
///
///   for (auto& t : tasks) {
///     group.spawn(child(t));
///   }
///   co_await group.wait();
/// }
/// \endcode
template <boost::asio::execution::executor Executor>
class co_spawn_group {
  using impl_type = detail::co_spawn_group_impl<Executor>;
  boost::intrusive_ptr<impl_type> impl;

 public:
  co_spawn_group(Executor ex, size_t limit,
                 cancel_on_error on_error = cancel_on_error::none)
    : impl(new impl_type(ex, limit, on_error))
  {
  }

  ~co_spawn_group()
  {
    impl->cancel();
  }

  using executor_type = Executor;
  executor_type get_executor() const
  {
    return impl->get_executor();
  }

  /// Spawn the given coroutine \ref cr on the group's executor. Throws a
  /// std::length_error exception if the number of outstanding coroutines
  /// would exceed the group's limit.
  void spawn(boost::asio::awaitable<void, executor_type> cr)
  {
    impl->spawn(std::move(cr));
  }

  /// Wait for all outstanding coroutines before returning. If any of the
  /// spawned coroutines exit with an exception, the first exception is
  /// rethrown.
  ///
  /// After wait() completes, whether by exception or co_return, the spawn
  /// group can be reused to spawn and await additional coroutines.
  boost::asio::awaitable<void, executor_type> wait()
  {
    return impl->wait();
  }

  /// Cancel all outstanding coroutines.
  void cancel()
  {
    impl->cancel();
  }
};

} // namespace ceph::async
