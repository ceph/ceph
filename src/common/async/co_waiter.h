// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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
#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/asio/use_awaitable.hpp>
#include "include/ceph_assert.h"

namespace ceph::async {

/// Captures an awaitable handler for deferred completion or cancellation.
template <typename Ret, boost::asio::execution::executor Executor>
class co_waiter {
  using signature = void(std::exception_ptr, Ret);
  using token_type = boost::asio::use_awaitable_t<Executor>;
  using handler_type = typename boost::asio::async_result<
      token_type, signature>::handler_type;
  std::optional<handler_type> handler;

  struct op_cancellation {
    co_waiter* self;
    op_cancellation(co_waiter* self) : self(self) {}
    void operator()(boost::asio::cancellation_type_t type) {
      if (type != boost::asio::cancellation_type::none) {
        self->cancel();
      }
    }
  };
 public:
  co_waiter() = default;

  // copy and move are disabled because the cancellation handler captures 'this'
  co_waiter(const co_waiter&) = delete;
  co_waiter& operator=(const co_waiter&) = delete;

  /// Returns true if there's a handler awaiting completion.
  bool waiting() const { return handler.has_value(); }

  /// Returns an awaitable that blocks until complete() or cancel().
  boost::asio::awaitable<Ret, Executor> get()
  {
    ceph_assert(!handler);
    token_type token;
    return boost::asio::async_initiate<token_type, signature>(
        [this] (handler_type h) {
          auto slot = boost::asio::get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          handler.emplace(std::move(h));
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(std::exception_ptr eptr, Ret value)
  {
    ceph_assert(handler);
    auto h = boost::asio::append(std::move(*handler), eptr, std::move(value));
    handler.reset();
    boost::asio::dispatch(std::move(h));
  }

  /// Cancel the coroutine with an operation_aborted exception.
  void cancel()
  {
    if (handler) {
      auto eptr = std::make_exception_ptr(
          boost::system::system_error(
              boost::asio::error::operation_aborted));
      complete(eptr, Ret{});
    }
  }

  /// Destroy the completion handler.
  void shutdown()
  {
    handler.reset();
  }
};

// specialization for Ret=void
template <boost::asio::execution::executor Executor>
class co_waiter<void, Executor> {
  using signature = void(std::exception_ptr);
  using token_type = boost::asio::use_awaitable_t<Executor>;
  using handler_type = typename boost::asio::async_result<
      token_type, signature>::handler_type;
  std::optional<handler_type> handler;

  struct op_cancellation {
    co_waiter* self;
    op_cancellation(co_waiter* self) : self(self) {}
    void operator()(boost::asio::cancellation_type_t type) {
      if (type != boost::asio::cancellation_type::none) {
        self->cancel();
      }
    }
  };
 public:
  co_waiter() = default;

  // copy and move are disabled because the cancellation handler captures 'this'
  co_waiter(const co_waiter&) = delete;
  co_waiter& operator=(const co_waiter&) = delete;

  /// Returns true if there's a handler awaiting completion.
  bool waiting() const { return handler.has_value(); }

  /// Returns an awaitable that blocks until complete() or cancel().
  boost::asio::awaitable<void, Executor> get()
  {
    ceph_assert(!handler);
    token_type token;
    return boost::asio::async_initiate<token_type, signature>(
        [this] (handler_type h) {
          auto slot = boost::asio::get_associated_cancellation_slot(h);
          if (slot.is_connected()) {
            slot.template emplace<op_cancellation>(this);
          }
          handler.emplace(std::move(h));
        }, token);
  }

  /// Schedule the completion handler with the given arguments.
  void complete(std::exception_ptr eptr)
  {
    ceph_assert(handler);
    auto h = boost::asio::append(std::move(*handler), eptr);
    handler.reset();
    boost::asio::dispatch(std::move(h));
  }

  /// Cancel the coroutine with an operation_aborted exception.
  void cancel()
  {
    if (handler) {
      auto eptr = std::make_exception_ptr(
          boost::system::system_error(
              boost::asio::error::operation_aborted));
      complete(eptr);
    }
  }

  /// Destroy the completion handler.
  void shutdown()
  {
    handler.reset();
  }
};

} // namespace ceph::async
