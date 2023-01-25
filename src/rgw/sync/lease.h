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

#include "include/scope_guard.h"
#include "common/ceph_time.h"
#include "detail/lease_state.h"
#include "common.h"

namespace rgw::sync {

/// \brief Client interface for a specific timed distributed exclusive lock.
class LockClient {
 public:
  virtual ~LockClient() {}

  /// Acquire a timed lock for the given duration, or throw on error.
  virtual awaitable<void> acquire(ceph::timespan duration) = 0;
  /// Renew an acquired lock for the given duration, or throw on error.
  virtual awaitable<void> renew(ceph::timespan duration) = 0;
  /// Release an acquired lock, or throw on error.
  virtual awaitable<void> release() = 0;
};


/// \brief Call a coroutine under the protection of a continuous lease.
///
/// Acquires exclusive access to a timed lock, then spawns the given coroutine
/// \ref cr. The lock is renewed at intervals of \ref duration / 2. The
/// coroutine is canceled if the lock is lost before its completion.
///
/// Exceptions thrown by release() are ignored, but exceptions from acquire()
/// and renew() propagate back to the caller. If renew() is delayed long
/// enough for the lock to expire, a boost::system::system_error exception is
/// thrown with an error code matching boost::system::errc::timed_out.
///
/// Otherwise, the result of \ref cr is returned to the caller, whether by
/// exception or return value.
///
/// \relates LockClient
///
/// \param lock A client that can send lock requests
/// \param duration Duration of the lock
/// \param cr The coroutine to call under lease
///
/// \tparam T The return type of the coroutine \ref cr
template <typename T>
auto with_lease(LockClient& lock,
                ceph::timespan duration,
                awaitable<T> cr)
    -> awaitable<T>
{
  auto ex = co_await asio::this_coro::executor;

  // acquire the lock. exceptions propagate directly to the caller
  co_await lock.acquire(duration);
  auto expires_at = detail::lease_clock::now() + duration;

  // allocate the lease state with scoped cancellation so that with_lease()'s
  // cancellation triggers cancellation of the spawned coroutine
  auto state = detail::make_lease_completion_state<T>(ex);
  const auto state_guard = make_scope_guard([&state] { state->cancel(); });

  // spawn the coroutine with a waitable/cancelable completion handler
  asio::co_spawn(ex, std::move(cr), state->completion_handler());

  // lock renewal loop
  auto& timer = state->get_timer();
  const ceph::timespan interval = duration / 2;

  while (!state->aborted() && !state->completed()) {
    // sleep until the next lock interval
    timer.expires_after(interval);
    try {
      co_await timer.async_wait(use_awaitable);
    } catch (const std::exception&) {
      break; // timer canceled by cr's completion, or caller canceled
    }

    // arm a timeout for the renew request
    timer.expires_at(expires_at);
    timer.async_wait([state] (error_code ec) {
          if (!ec) {
            state->abort(std::make_exception_ptr(
                boost::system::system_error(
                    ETIMEDOUT, boost::system::system_category())));
          }
        });

    try {
      co_await lock.renew(duration);
      expires_at = detail::lease_clock::now() + duration;
    } catch (const std::exception&) {
      state->abort(std::current_exception());
      expires_at = detail::lease_clock::zero(); // don't release below
      break;
    }
  }
  timer.cancel();

  // if cr was canceled, await its completion before releasing the lock
  if (!state->completed()) {
    co_await state->wait();
  }

  // release the lock if it hasn't expired
  if (detail::lease_clock::now() < expires_at) try {
    co_await lock.release();
  } catch (const std::exception&) {} // ignore errors

  // return the spawned coroutine's result
  co_return state->get();
}

} // namespace rgw::sync
