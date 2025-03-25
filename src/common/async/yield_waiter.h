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

#include <exception>
#include <mutex>
#include <optional>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/spawn.hpp>

namespace ceph::async {

namespace detail {

// handler wrapper that reacquires a lock immediately before completion
template <typename Handler, typename BasicLockable>
struct lock_handler {
  Handler handler;
  BasicLockable* lock = nullptr;

  template <typename ...Args>
  void operator()(Args&& ...args) {
    if (lock) {
      lock->lock();
    }
    std::move(handler)(std::forward<Args>(args)...);
  }
};

// deduction guide required by windows?
template <typename Handler, typename BasicLockable>
lock_handler(Handler&&, BasicLockable*) -> lock_handler<Handler, BasicLockable>;

} // namespace detail

/// Captures a yield_context handler for deferred completion or cancellation.
template <typename Ret>
class yield_waiter {
 public:
  /// Function signature for the completion handler.
  using Signature = void(boost::system::error_code, Ret);

  yield_waiter() = default;

  // copy and move are disabled because the cancellation handler captures 'this'
  yield_waiter(const yield_waiter&) = delete;
  yield_waiter& operator=(const yield_waiter&) = delete;

  /// Returns true if there's a handler awaiting completion.
  operator bool() const { return state.has_value(); }

  /// Returns false if there's a handler awaiting completion.
  bool empty() const { return !state.has_value(); }

  /// Returns the captured completion handler's executor.
  /// Precondition: !empty()
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const
  {
    return boost::asio::get_associated_executor(
        state->handler, state->work.get_executor());
  }

  /// Suspends the given yield_context until the captured handler is invoked
  /// via complete() or cancel().
  template <typename CompletionToken>
  auto async_wait(CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this] (handler_type h) {
          auto slot = get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          constexpr std::unique_lock<std::mutex>* lock = nullptr;
          state.emplace(std::move(h), lock);
        }, token);
  }

  /// Suspends the given yield_context until the captured handler is invoked
  /// via complete() or cancel(). The given lock is released immediately before
  /// it suspends and reacquired immediately after it resumes.
  template <typename CompletionToken>
  auto async_wait(std::unique_lock<std::mutex>& lock, CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, &lock] (handler_type h) {
          auto slot = get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          state.emplace(std::move(h), &lock);
          lock.unlock(); // unlock before suspend
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(boost::system::error_code ec, Ret value)
  {
    auto s = std::move(*state);
    state.reset();
    boost::asio::dispatch(
        boost::asio::append(
            detail::lock_handler{std::move(s.handler), s.lock},
            ec, std::move(value)));
  }

  /// Destroy the completion handler.
  void shutdown()
  {
    state.reset();
  }

 private:
  using handler_type = typename boost::asio::async_result<
      boost::asio::yield_context, Signature>::handler_type;
  using work_guard = boost::asio::executor_work_guard<
      boost::asio::any_io_executor>;

  struct handler_state {
    handler_type handler;
    work_guard work;
    std::unique_lock<std::mutex>* lock = nullptr;

    handler_state(handler_type&& h, std::unique_lock<std::mutex>* lock)
      : handler(std::move(h)), work(make_work_guard(handler)), lock(lock)
    {}
  };
  std::optional<handler_state> state;

  struct op_cancellation {
    yield_waiter* self;
    op_cancellation(yield_waiter* self) : self(self) {}
    void operator()(boost::asio::cancellation_type type) {
      if (type != boost::asio::cancellation_type::none) {
        self->cancel();
      }
    }
  };

  // Cancel the coroutine with an operation_aborted error.
  void cancel()
  {
    if (state) {
      complete(make_error_code(boost::asio::error::operation_aborted), Ret{});
    }
  }
};

// specialization for Ret=void
template <>
class yield_waiter<void> {
 public:
  /// Function signature for the completion handler.
  using Signature = void(boost::system::error_code);

  yield_waiter() = default;

  // copy and move are disabled because the cancellation handler captures 'this'
  yield_waiter(const yield_waiter&) = delete;
  yield_waiter& operator=(const yield_waiter&) = delete;

  /// Returns true if there's a handler awaiting completion.
  operator bool() const { return state.has_value(); }

  /// Returns false if there's a handler awaiting completion.
  bool empty() const { return !state.has_value(); }

  /// Returns the captured completion handler's executor.
  /// Precondition: !empty()
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const
  {
    return boost::asio::get_associated_executor(
        state->handler, state->work.get_executor());
  }

  /// Suspends the given yield_context until the captured handler is invoked
  /// via complete() or cancel().
  template <typename CompletionToken>
  auto async_wait(CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this] (handler_type h) {
          auto slot = get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          constexpr std::unique_lock<std::mutex>* lock = nullptr;
          state.emplace(std::move(h), lock);
        }, token);
  }

  /// Suspends the given yield_context until the captured handler is invoked
  /// via complete() or cancel(). The given lock is released immediately before
  /// it suspends and reacquired immediately after it resumes.
  template <typename CompletionToken>
  auto async_wait(std::unique_lock<std::mutex>& lock, CompletionToken&& token)
  {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, &lock] (handler_type h) {
          auto slot = get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          state.emplace(std::move(h), &lock);
          lock.unlock(); // unlock before suspend
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(boost::system::error_code ec)
  {
    auto s = std::move(*state);
    state.reset();
    boost::asio::dispatch(
        boost::asio::append(
            detail::lock_handler{std::move(s.handler), s.lock}, ec));
  }

  /// Destroy the completion handler.
  void shutdown()
  {
    state.reset();
  }

 private:
  using handler_type = typename boost::asio::async_result<
      boost::asio::yield_context, Signature>::handler_type;
  using work_guard = boost::asio::executor_work_guard<
      boost::asio::any_io_executor>;

  struct handler_state {
    handler_type handler;
    work_guard work;
    std::unique_lock<std::mutex>* lock = nullptr;

    handler_state(handler_type&& h, std::unique_lock<std::mutex>* lock)
      : handler(std::move(h)), work(make_work_guard(handler)), lock(lock)
    {}
  };
  std::optional<handler_state> state;

  struct op_cancellation {
    yield_waiter* self;
    op_cancellation(yield_waiter* self) : self(self) {}
    void operator()(boost::asio::cancellation_type type) {
      if (type != boost::asio::cancellation_type::none) {
        self->cancel();
      }
    }
  };

  // Cancel the coroutine with an operation_aborted error.
  void cancel()
  {
    if (state) {
      complete(make_error_code(boost::asio::error::operation_aborted));
    }
  }
};

} // namespace ceph::async

namespace boost::asio {

// forward the handler's associated executor, allocator, cancellation slot, etc
template <template <typename, typename> class Associator,
          typename Handler, typename BasicLockable, typename DefaultCandidate>
struct associator<Associator,
    ceph::async::detail::lock_handler<Handler, BasicLockable>, DefaultCandidate>
  : Associator<Handler, DefaultCandidate>
{
  static auto get(const ceph::async::detail::lock_handler<Handler, BasicLockable>& h) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler);
  }
  static auto get(const ceph::async::detail::lock_handler<Handler, BasicLockable>& h,
                  const DefaultCandidate& c) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler, c);
  }
};

} // namespace boost::asio
