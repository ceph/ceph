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

#include <memory>
#include <optional>
#include <boost/asio/append.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/async/cancel_on_error.h"
#include "common/async/detail/service.h"
#include "include/ceph_assert.h"

namespace ceph::async::detail {

// Coroutine throttle implementation. This is reference-counted so the
// co_spawn() completion handlers can extend the implementation's lifetime.
// This is required for per-op cancellation because the cancellation_signals
// must outlive their coroutine frames.
template <boost::asio::execution::executor Executor, typename SizeType>
class co_throttle_impl :
    public boost::intrusive_ref_counter<co_throttle_impl<Executor, SizeType>,
        boost::thread_unsafe_counter>,
    public service_list_base_hook
{
 public:
  using size_type = SizeType;

  using executor_type = Executor;
  executor_type get_executor() const { return ex; }

  co_throttle_impl(const executor_type& ex, size_type limit,
                   cancel_on_error on_error)
    : svc(boost::asio::use_service<service<co_throttle_impl>>(
            boost::asio::query(ex, boost::asio::execution::context))),
      ex(ex), limit(limit), on_error(on_error),
      children(new child[limit])
  {
    // register for service_shutdown() notifications
    svc.add(*this);

    // initialize the free list
    for (size_type i = 0; i < limit; i++) {
      free.push_back(children[i]);
    }
  }
  ~co_throttle_impl()
  {
    svc.remove(*this);
  }

  template <typename T>
  using awaitable = boost::asio::awaitable<T, executor_type>;

  template <typename T> // where T=void or error_code
  auto spawn(awaitable<T> cr, size_type smaller_limit)
      -> awaitable<boost::system::error_code>
  {
    if (unreported_exception) {
      std::rethrow_exception(std::exchange(unreported_exception, nullptr));
    }
    if (unreported_error && on_error != cancel_on_error::none) {
      co_return std::exchange(unreported_error, {});
    }

    const size_type current_limit = std::min(smaller_limit, limit);
    if (count >= current_limit) {
      auto ec = co_await wait_for(current_limit - 1);
      if (ec) {
        unreported_error.clear();
        co_return ec;
      }
      if (unreported_error && on_error != cancel_on_error::none) {
        co_return std::exchange(unreported_error, {});
      }
    }

    ++count;

    // move a free child to the outstanding list
    ceph_assert(!free.empty());
    child& c = free.front();
    free.pop_front();
    outstanding.push_back(c);

    // spawn the coroutine with its associated cancellation signal
    c.signal.emplace();
    c.canceled = false;

    boost::asio::co_spawn(get_executor(), std::move(cr),
        boost::asio::bind_cancellation_slot(c.signal->slot(),
            child_completion{this, c}));

    co_return std::exchange(unreported_error, {});
  }

  awaitable<boost::system::error_code> wait()
  {
    if (count > 0) {
      auto ec = co_await wait_for(0);
      if (ec) {
        unreported_error.clear();
        co_return ec;
      }
    }
    co_return std::exchange(unreported_error, {});
  }

  void cancel()
  {
    for (child& c : outstanding) {
      c.canceled = true;
      c.signal->emit(boost::asio::cancellation_type::terminal);
    }
    if (wait_handler) {
      wait_complete(make_error_code(boost::asio::error::operation_aborted));
    }
  }

  void service_shutdown()
  {
    wait_handler.reset();
  }

 private:
  service<co_throttle_impl>& svc;
  executor_type ex;
  const size_type limit;
  const cancel_on_error on_error;

  size_type count = 0;
  size_type wait_for_count = 0;

  boost::system::error_code unreported_error;
  std::exception_ptr unreported_exception;

  // track each spawned coroutine for cancellation. these are stored in an
  // array, and recycled after each use via the free list
  struct child : boost::intrusive::list_base_hook<> {
    std::optional<boost::asio::cancellation_signal> signal;
    bool canceled = false;
  };
  std::unique_ptr<child[]> children;

  using child_list = boost::intrusive::list<child,
        boost::intrusive::constant_time_size<false>>;
  child_list outstanding;
  child_list free;

  using use_awaitable_t = boost::asio::use_awaitable_t<executor_type>;

  using wait_signature = void(std::exception_ptr, boost::system::error_code);
  using wait_handler_type = typename boost::asio::async_result<
      use_awaitable_t, wait_signature>::handler_type;
  std::optional<wait_handler_type> wait_handler;

  // return an awaitable that completes once count <= target_count
  auto wait_for(size_type target_count)
      -> awaitable<boost::system::error_code>
  {
    ceph_assert(!wait_handler); // one waiter at a time
    wait_for_count = target_count;

    use_awaitable_t token;
    return boost::asio::async_initiate<use_awaitable_t, wait_signature>(
        [this] (wait_handler_type h) {
          wait_handler.emplace(std::move(h));
        }, token);
  }

  void on_complete(child& c, std::exception_ptr eptr,
                   boost::system::error_code ec)
  {
    --count;

    if (c.canceled) {
      // don't report cancellation errors. cancellation was either requested
      // by the user, or triggered by another failure that is reported
      eptr = nullptr;
      ec = {};
    }

    if (eptr && !unreported_exception) {
      unreported_exception = eptr;
    }
    if (ec && !unreported_error) {
      unreported_error = ec;
    }

    // move back to the free list
    auto next = outstanding.erase(outstanding.iterator_to(c));
    c.signal.reset();
    free.push_back(c);

    // handle cancel_on_error
    if (eptr || ec) {
      auto cancel_begin = outstanding.end();
      if (on_error == cancel_on_error::after) {
        cancel_begin = next;
      } else if (on_error == cancel_on_error::all) {
        cancel_begin = outstanding.begin();
      }
      for (auto i = cancel_begin; i != outstanding.end(); ++i) {
        i->canceled = true;
        i->signal->emit(boost::asio::cancellation_type::terminal);
      }
    }

    // maybe wake the waiter
    if (wait_handler && count <= wait_for_count) {
      wait_complete({});
    }
  }

  void wait_complete(boost::system::error_code ec)
  {
    // bind arguments to the handler for dispatch
    auto eptr = std::exchange(unreported_exception, nullptr);
    auto c = boost::asio::append(std::move(*wait_handler), eptr, ec);
    wait_handler.reset();

    boost::asio::dispatch(std::move(c));
  }

  struct child_completion {
    boost::intrusive_ptr<co_throttle_impl> impl;
    child& c;

    void operator()(std::exception_ptr eptr,
                    boost::system::error_code ec = {}) {
      impl->on_complete(c, eptr, ec);
    }
  };
};

} // namespace ceph::async::detail
