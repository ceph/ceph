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
#include "detail/spawn_group.h"

namespace ceph::async {

/// \brief Tracks a group of coroutines to await all of their completions.
///
/// This class functions as a CompletionToken for calls to co_spawn(), attaching
/// a handler that notifies the group upon the child coroutine's completion.
///
/// The wait() function can be used to await the completion of all children.
/// If any child coroutines exit with an exception, the first such exception
/// is rethrown by wait(). The cancel_on_error option controls whether these
/// exceptions trigger the cancellation of other children.
///
/// All child coroutines are canceled by cancel() or spawn_group destruction.
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
///   auto group = spawn_group{ex, tasks.size()};
///
///   for (auto& t : tasks) {
///     boost::asio::co_spawn(ex, child(t), group);
///   }
///   co_await group.wait();
/// }
/// \endcode
template <boost::asio::execution::executor Executor>
class spawn_group {
  using impl_type = detail::spawn_group_impl<Executor>;
  boost::intrusive_ptr<impl_type> impl;

 public:
  spawn_group(Executor ex, size_t limit,
              cancel_on_error on_error = cancel_on_error::none)
    : impl(new impl_type(ex, limit, on_error))
  {
  }

  ~spawn_group()
  {
    impl->cancel();
  }

  using executor_type = Executor;
  executor_type get_executor() const
  {
    return impl->get_executor();
  }

  /// Return a cancellable co_spawn() completion handler with signature
  /// void(std::exception_ptr). Throws a std::length_error exception if the
  /// number of outstanding completion handlers would exceed the group's limit.
  ///
  /// As a convenience, you can avoid calling this function by using the
  /// spawn_group itself as a CompletionToken for co_spawn().
  auto completion()
  {
    return impl->completion();
  }

  /// Wait for all outstanding completion handlers before returning. If any
  /// of the spawned coroutines exit with an exception, the first exception
  /// is rethrown.
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

namespace boost::asio {

// Allow spawn_group to be used as a CompletionToken.
template <typename Executor, typename Signature>
struct async_result<ceph::async::spawn_group<Executor>, Signature>
{
  using completion_handler_type =
      ceph::async::detail::spawn_group_handler<Executor>;
  async_result(completion_handler_type&) {}

  using return_type = void;
  return_type get() {}

  template <typename Initiation, typename... Args>
  static return_type initiate(Initiation&& init,
                              ceph::async::spawn_group<Executor>& group,
                              Args&& ...args)
  {
    return std::move(init)(group.completion(), std::forward<Args>(args)...);
  }
};

} // namespace boost::asio
