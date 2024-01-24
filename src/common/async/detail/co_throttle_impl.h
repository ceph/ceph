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
#include "common/async/co_waiter.h"
#include "common/async/service.h"
#include "include/ceph_assert.h"

namespace ceph::async::detail {

// Coroutine throttle implementation. This is reference-counted so the
// co_spawn() completion handlers can extend the implementation's lifetime.
// This is required for per-op cancellation because the cancellation_signals
// must outlive their coroutine frames.
template <boost::asio::execution::executor Executor>
class co_throttle_impl :
    public boost::intrusive_ref_counter<co_throttle_impl<Executor>,
        boost::thread_unsafe_counter>,
    public service_list_base_hook
{
 public:
  using size_type = uint16_t;

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

  auto spawn(boost::asio::awaitable<void, executor_type> cr,
             size_type smaller_limit)
      -> boost::asio::awaitable<void, executor_type>
  {
    if (unreported_exception && on_error != cancel_on_error::none) {
      std::rethrow_exception(std::exchange(unreported_exception, nullptr));
    }

    const size_type current_limit = std::min(smaller_limit, limit);
    if (count >= current_limit) {
      co_await wait_for(current_limit - 1);
      if (unreported_exception && on_error != cancel_on_error::none) {
        std::rethrow_exception(std::exchange(unreported_exception, nullptr));
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

    if (unreported_exception) {
      std::rethrow_exception(std::exchange(unreported_exception, nullptr));
    }
  }

  auto wait()
      -> boost::asio::awaitable<void, executor_type>
  {
    if (count > 0) {
      co_await wait_for(0);
    }
    if (unreported_exception) {
      std::rethrow_exception(std::exchange(unreported_exception, nullptr));
    }
  }

  void cancel()
  {
    while (!outstanding.empty()) {
      child& c = outstanding.front();
      outstanding.pop_front();

      c.canceled = true;
      c.signal->emit(boost::asio::cancellation_type::terminal);
    }
  }

  void service_shutdown()
  {
    waiter.shutdown();
  }

 private:
  service<co_throttle_impl>& svc;
  executor_type ex;
  const size_type limit;
  const cancel_on_error on_error;

  size_type count = 0;
  size_type wait_for_count = 0;

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

  co_waiter<void, executor_type> waiter;

  // return an awaitable that completes once count <= target_count
  auto wait_for(size_type target_count)
      -> boost::asio::awaitable<void, executor_type>
  {
    wait_for_count = target_count;
    return waiter.get();
  }

  void on_complete(child& c, std::exception_ptr eptr)
  {
    --count;

    if (c.canceled) {
      // if the child was canceled, it was already removed from outstanding
      ceph_assert(!c.is_linked());
      c.canceled = false;
      c.signal.reset();
      free.push_back(c);
    } else {
      // move back to the free list
      ceph_assert(c.is_linked());
      auto next = outstanding.erase(outstanding.iterator_to(c));
      c.signal.reset();
      free.push_back(c);

      if (eptr) {
        if (eptr && !unreported_exception) {
          unreported_exception = eptr;
        }

        // handle cancel_on_error. cancellation signals may recurse into
        // on_complete(), so move the entries into a separate list first
        child_list to_cancel;
        if (on_error == cancel_on_error::after) {
          to_cancel.splice(to_cancel.end(), outstanding,
                           next, outstanding.end());
        } else if (on_error == cancel_on_error::all) {
          to_cancel = std::move(outstanding);
        }

        for (auto i = to_cancel.begin(); i != to_cancel.end(); ++i) {
          child& c = *i;
          i = to_cancel.erase(i);

          c.canceled = true;
          c.signal->emit(boost::asio::cancellation_type::terminal);
        }
      }
    }

    // maybe wake the waiter
    if (waiter.waiting() && count <= wait_for_count) {
      waiter.complete(nullptr);
    }
  }

  struct child_completion {
    boost::intrusive_ptr<co_throttle_impl> impl;
    child& c;

    void operator()(std::exception_ptr eptr) {
      impl->on_complete(c, eptr);
    }
  };
};

} // namespace ceph::async::detail
