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
#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system.hpp>
#include "include/scope_guard.h"
#include "common/ceph_time.h"
#include "detail/lease_state.h"

namespace ceph::async {

/// \brief Client interface for a specific timed exclusive lock.
class LockClient {
 protected:
  using Signature = void(boost::system::error_code);
  using Handler = boost::asio::any_completion_handler<Signature>;

  virtual void acquire(ceph::timespan duration, Handler handler) = 0;
  virtual void renew(ceph::timespan duration, Handler handler) = 0;
  virtual void release(Handler handler) = 0;

 public:
  virtual ~LockClient() {}

  /// Acquire a timed lock for the given duration.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_acquire(ceph::timespan duration, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, duration] (auto h) { acquire(duration, std::move(h)); }, token);
  }
  /// Renew an acquired lock for the given duration.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_renew(ceph::timespan duration, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, duration] (auto h) { renew(duration, std::move(h)); }, token);
  }
  /// Release an acquired lock.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_release(CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this] (auto h) { release(std::move(h)); }, token);
  }
};


/// Exception thrown by with_lease() whenever the wrapped coroutine is
/// canceled due to a renewal failure.
class lease_aborted : public std::runtime_error {
  boost::system::error_code ec;
 public:
  lease_aborted(boost::system::error_code ec)
    : runtime_error("lease aborted"), ec(ec)
  {}

  /// Return the original error that triggered the cancellation.
  boost::system::error_code code() const { return ec; }
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
/// If renew() is delayed long enough for the timed lock to expire, that results
/// in an error code matching boost::system::errc::timed_out. That error, along
/// with any error from renew() itself, gets wrapped in a lease_aborted
/// exception when reported to the caller. Any failure to renew the lock will
/// also result in the cancellation of \ref cr.
///
/// Cancellation of the parent coroutine also cancels the child coroutine
/// without releasing the lock.
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
                boost::asio::awaitable<T> cr)
    -> boost::asio::awaitable<T>
{
  auto ex = co_await boost::asio::this_coro::executor;

  // acquire the lock. exceptions propagate directly to the caller
  co_await lock.async_acquire(duration, boost::asio::use_awaitable);
  auto expires_at = detail::lease_clock::now() + duration;

  // the LockClient may not support cancellation, so check after it returns
  if (auto cs = co_await boost::asio::this_coro::cancellation_state;
      cs.cancelled() != boost::asio::cancellation_type::none) {
    throw boost::system::system_error(boost::asio::error::operation_aborted);
  }

  // allocate the lease state with scoped cancellation so that with_lease()'s
  // cancellation triggers cancellation of the spawned coroutine
  auto state = detail::make_lease_completion_state<T>(ex);
  const auto state_guard = make_scope_guard([&state] { state->cancel(); });

  // spawn the coroutine with a waitable/cancelable completion handler
  boost::asio::co_spawn(ex, std::move(cr), state->completion_handler());

  // lock renewal loop
  auto& timer = state->get_timer();
  const ceph::timespan interval = duration / 2;
  boost::system::error_code ec;

  while (!state->completed()) {
    // sleep until the next lock interval
    timer.expires_after(interval);
    co_await timer.async_wait(boost::asio::redirect_error(
            boost::asio::use_awaitable, ec));
    if (ec) {
      if (auto cs = co_await boost::asio::this_coro::cancellation_state;
          cs.cancelled() != boost::asio::cancellation_type::none) {
        // if cancellation was requested by the caller, let the timer's
        // operation_aborted exception propagate. state_guard will cancel the
        // wrapped coroutine immediately on unwind
        throw boost::system::system_error(ec);
      }
      break; // timer canceled by cr's completion
    }

    // arm a timeout for the renew request
    timer.expires_at(expires_at);
    timer.async_wait([state] (boost::system::error_code ec) {
          if (!ec) {
            state->abort(std::make_exception_ptr(lease_aborted{
                make_error_code(std::errc::timed_out)}));
          }
        });

    co_await lock.async_renew(duration, boost::asio::redirect_error(
            boost::asio::use_awaitable, ec));
    timer.cancel();
    if (auto cs = co_await boost::asio::this_coro::cancellation_state;
        cs.cancelled() != boost::asio::cancellation_type::none) {
      ec = boost::asio::error::operation_aborted;
    }
    if (ec) {
      state->rethrow(); // may already have a timeout error to return
      throw lease_aborted{ec};
    }
    expires_at = detail::lease_clock::now() + duration;
  }

  // release the lock, ignoring errors
  co_await lock.async_release(boost::asio::redirect_error(
          boost::asio::use_awaitable, ec));

  // return the spawned coroutine's result
  co_return state->get();
}

namespace detail {

template <typename T>
void renew(LockClient& lock,
           lease_clock::time_point expires_at,
           ceph::timespan duration,
           boost::asio::yield_context yield,
           boost::intrusive_ptr<lease_completion_state<T>>& state);

} // namespace detail

/// \overload
///
/// \param lock A client that can send lock requests
/// \param duration Duration of the lock
/// \param yield The calling coroutine's yield context
/// \param cr The coroutine to call under lease, taking a newly-spawned
///           yield context as its only argument.
template <std::invocable<boost::asio::yield_context> Func>
auto with_lease(LockClient& lock,
                ceph::timespan duration,
                boost::asio::yield_context yield,
                Func&& cr)
    -> std::invoke_result_t<Func, boost::asio::yield_context>
{
  auto ex = yield.get_executor();

  // acquire the lock. exceptions propagate directly to the caller
  lock.async_acquire(duration, yield);
  auto expires_at = detail::lease_clock::now() + duration;

  // the LockClient may not support cancellation, so check after it returns
  if (yield.cancelled() != boost::asio::cancellation_type::none) {
    throw boost::system::system_error(boost::asio::error::operation_aborted);
  }

  // allocate the lease state with scoped cancellation so that with_lease()'s
  // cancellation triggers cancellation of the spawned coroutine
  using T = std::invoke_result_t<Func, boost::asio::yield_context>;
  auto state = detail::make_lease_completion_state<T>(ex);
  const auto state_guard = make_scope_guard([&state] { state->cancel(); });

  // spawn the coroutine with a waitable/cancelable completion handler
  boost::asio::spawn(ex, std::forward<Func>(cr), state->completion_handler());

  detail::renew(lock, expires_at, duration, yield, state);

  // release the lock, ignoring errors
  boost::system::error_code ec;
  lock.async_release(yield[ec]);

  // return the spawned coroutine's result
  return state->get();
}

namespace detail {

// the renewal loop only depends on the coroutine's return type T, so doesn't
// need to be reinstantiated for every Func
template <typename T>
void renew(LockClient& lock,
           lease_clock::time_point expires_at,
           ceph::timespan duration,
           boost::asio::yield_context yield,
           boost::intrusive_ptr<lease_completion_state<T>>& state)
{
  // lock renewal loop
  auto& timer = state->get_timer();
  const ceph::timespan interval = duration / 2;
  boost::system::error_code ec;

  while (!state->completed()) {
    // sleep until the next lock interval
    timer.expires_after(interval);
    timer.async_wait(yield[ec]);
    if (ec) {
      if (yield.cancelled() != boost::asio::cancellation_type::none) {
        // if cancellation was requested by the caller, let the timer's
        // operation_aborted exception propagate. state_guard will cancel the
        // wrapped coroutine immediately on unwind
        throw boost::system::system_error(ec);
      }
      break; // timer canceled by cr's completion
    }

    // arm a timeout for the renew request
    timer.expires_at(expires_at);
    timer.async_wait([state] (boost::system::error_code ec) {
          if (!ec) {
            state->abort(std::make_exception_ptr(lease_aborted{
                make_error_code(std::errc::timed_out)}));
          }
        });

    lock.async_renew(duration, yield[ec]);
    timer.cancel();
    if (yield.cancelled() != boost::asio::cancellation_type::none) {
      ec = boost::asio::error::operation_aborted;
    }
    if (ec) {
      state->rethrow(); // may already have a timeout error to return
      throw lease_aborted{ec};
    }
    expires_at = lease_clock::now() + duration;
  }
}

} // namespace detail

} // namespace ceph::async
