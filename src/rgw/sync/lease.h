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

#include <exception>
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


/// Exception thrown by with_lease() whenever the wrapped coroutine is canceled
/// due to a renewal failure.
class lease_aborted : public std::runtime_error {
  std::exception_ptr eptr;
 public:
  lease_aborted(std::exception_ptr eptr)
    : runtime_error("lease aborted"), eptr(eptr)
  {}

  /// Return the original exception that triggered the cancellation.
  std::exception_ptr original_exception() const { return eptr; };
};


/// \brief Call a coroutine under the protection of a continuous lease.
///
/// Acquires exclusive access to a timed lock, then spawns the given coroutine
/// \ref cr. The lock is renewed at intervals of \ref duration / 2, and released
/// after the coroutine's completion.
///
/// Exceptions from acquire() propagate directly to the caller. Exceptions from
/// release() are ignored in favor of the result of \ref cr.
///
/// If renew() is delayed long enough for the timed lock to expire, a
/// boost::system::system_error exception is thrown with an error code matching
/// boost::system::errc::timed_out. That exception, along with any exception
/// from renew() itself, gets wrapped in a lease_aborted exception when
/// reported to the caller. Any failure to renew the lock will also result in
/// the cancellation of \ref cr.
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
            auto eptr = std::make_exception_ptr(
                boost::system::system_error(
                    ETIMEDOUT, boost::system::system_category()));
            state->abort(std::make_exception_ptr(lease_aborted{eptr}));
          }
        });

    try {
      co_await lock.renew(duration);
      expires_at = detail::lease_clock::now() + duration;
    } catch (const std::exception&) {
      state->abort(std::make_exception_ptr(
              lease_aborted{std::current_exception()}));
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
