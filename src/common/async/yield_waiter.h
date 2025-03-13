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
#include <optional>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/spawn.hpp>

namespace ceph::async {

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
          state.emplace(std::move(h));
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(boost::system::error_code ec, Ret value)
  {
    auto s = std::move(*state);
    state.reset();
    auto h = boost::asio::append(std::move(s.handler), ec, std::move(value));
    boost::asio::dispatch(std::move(h));
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

    explicit handler_state(handler_type&& h)
      : handler(std::move(h)),
        work(make_work_guard(handler))
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
          state.emplace(std::move(h));
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(boost::system::error_code ec)
  {
    auto s = std::move(*state);
    state.reset();
    boost::asio::dispatch(boost::asio::append(std::move(s.handler), ec));
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

    explicit handler_state(handler_type&& h)
      : handler(std::move(h)),
        work(make_work_guard(handler))
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
