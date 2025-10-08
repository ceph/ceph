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

#include "detail/spawn_throttle_impl.h"

#include <boost/intrusive_ptr.hpp>
#include "cancel_on_error.h"

namespace ceph::async {

/// A coroutine throttle that allows a parent coroutine to spawn and manage
/// multiple child coroutines, while enforcing an upper bound on concurrency.
///
/// Child coroutines take boost::asio::yield_context as the only argument.
/// Exceptions thrown by children are reported to the caller on its next call
/// to spawn() or wait(). The cancel_on_error option controls whether these
/// exceptions trigger the cancellation of other children.
///
/// All child coroutines are canceled by cancel() or spawn_throttle destruction.
/// This allows a parent function to share memory with its child coroutines
/// without fear of dangling references.
///
/// This class is not thread-safe. Member functions should be called from the
/// parent thread of execution only.
///
/// Example:
/// @code
/// void child(boost::asio::yield_context yield);
///
/// void parent(size_t count, boost::asio::yield_context yield)
/// {
///   // spawn all children, up to 10 at a time
///   auto throttle = ceph::async::spawn_throttle{yield, 10};
///
///   for (size_t i = 0; i < count; i++) {
///     throttle.spawn(child);
///   }
///   throttle.wait();
/// }
/// @endcode
class spawn_throttle {
  using impl_type = detail::spawn_throttle_impl;
  boost::intrusive_ptr<impl_type> impl;

 public:
  spawn_throttle(boost::asio::yield_context yield, size_t limit,
                 cancel_on_error on_error = cancel_on_error::none)
    : impl(new detail::spawn_throttle_impl(yield, limit, on_error))
  {}

  spawn_throttle(spawn_throttle&&) = default;
  spawn_throttle& operator=(spawn_throttle&&) = default;
  // disable copy for unique ownership
  spawn_throttle(const spawn_throttle&) = delete;
  spawn_throttle& operator=(const spawn_throttle&) = delete;

  /// Cancel outstanding coroutines on destruction.
  ~spawn_throttle()
  {
    if (impl) {
      impl->cancel();
    }
  }

  using executor_type = impl_type::executor_type;
  executor_type get_executor()
  {
    return impl->get_executor();
  }

  /// Spawn a cancellable coroutine to call the given function, passing its
  /// boost::asio::yield_context as the only argument.
  ///
  /// Before spawning, this function may block until a throttle unit becomes
  /// available. If one or more previously-spawned coroutines exit with an
  /// exception, the first such exception is rethrown here.
  template <typename F>
  void spawn(F&& f)
  {
    boost::asio::spawn(get_executor(), std::forward<F>(f), impl->get());
  }

  /// /overload
  template <typename StackAllocator, typename F>
  void spawn(std::allocator_arg_t arg, StackAllocator&& alloc, F&& f)
  {
    boost::asio::spawn(get_executor(), arg, std::forward<StackAllocator>(alloc),
                       std::forward<F>(f), impl->get());
  }

  /// Wait for all outstanding completions before returning. If any
  /// of the spawned coroutines exits with an exception, the first exception
  /// is rethrown.
  ///
  /// After wait() completes, whether successfully or by exception, the yield
  /// throttle can be reused to spawn and await additional coroutines.
  void wait()
  {
    impl->wait_for(0);
  }

  /// Cancel all outstanding coroutines.
  void cancel()
  {
    impl->cancel();
  }
};

} // namespace ceph::async
