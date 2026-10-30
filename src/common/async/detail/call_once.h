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

#include <condition_variable>
#include <mutex>

#include <boost/intrusive/list.hpp>

#include "common/async/service.h"
#include "common/async/yield_context.h"
#include "common/async/yield_waiter.h"

namespace ceph::async::detail::call_once {

class AsyncWaiter : public boost::intrusive::list_base_hook<> {
  yield_waiter<void> impl;
 public:
  void wait(std::unique_lock<std::mutex>& lock,
            boost::asio::yield_context yield)
  {
    impl.async_wait(lock, yield);
  }
  void wake()
  {
    // async_wait() may complete inline via boost::asio::dispatch(), but would
    // immediately try to reacquire the lock we're already holding. defer the
    // completion until after we've returned and dropped our lock
    boost::asio::defer(impl.get_executor(), [this] {
        impl.complete(boost::system::error_code{});
      });
  }
  void destroy()
  {
    impl.shutdown();
  }
};

class SyncWaiter : public boost::intrusive::list_base_hook<> {
  std::condition_variable cond;
  bool done = false;
 public:
  void wait(std::unique_lock<std::mutex>& lock)
  {
    cond.wait(lock, [this] { return done; });
  }
  void wake()
  {
    done = true;
    cond.notify_one();
  }
};

struct WaitState : service_list_base_hook {
  service<WaitState>* svc = nullptr;
  boost::intrusive::list<SyncWaiter> sync_waiters;
  boost::intrusive::list<AsyncWaiter> async_waiters;

  ~WaitState()
  {
    if (svc) {
      svc->remove(*this);
    }
  }

  void wait(std::unique_lock<std::mutex>& lock, optional_yield y)
  {
    if (y) {
      auto yield = y.get_yield_context();
      if (!svc) {
        // on first async wait, register for service_shutdown() notifications
        svc = &boost::asio::use_service<service<WaitState>>(
            boost::asio::query(yield.get_executor(),
                               boost::asio::execution::context));
        svc->add(*this);
      }

      AsyncWaiter waiter;
      async_waiters.push_back(waiter);

      waiter.wait(lock, yield);
    } else {
      SyncWaiter waiter;
      sync_waiters.push_back(waiter);

      waiter.wait(lock);
    }
  }

  void wake_all()
  {
    while (!sync_waiters.empty()) {
      auto& waiter = sync_waiters.front();
      sync_waiters.pop_front();
      waiter.wake();
    }
    while (!async_waiters.empty()) {
      auto& waiter = async_waiters.front();
      async_waiters.pop_front();
      waiter.wake();
    }
  }

  // on shutdown, destroy async waiters that didn't complete. this breaks any
  // ownership cycles between once_result and the completion handlers
  void service_shutdown()
  {
    while (!async_waiters.empty()) {
      auto& waiter = async_waiters.front();
      async_waiters.pop_front();
      waiter.destroy();
    }
  }
};

} // namespace ceph::async::detail::call_once
