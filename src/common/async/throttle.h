// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef CEPH_ASYNC_THROTTLE_H
#define CEPH_ASYNC_THROTTLE_H

#include <mutex>
#include <boost/intrusive/list.hpp>
#include "completion.h"

namespace ceph::async {

/**
 * An asynchronous throttle implementation for use with boost::asio.
 *
 * Example use:
 *
 *   // process a list of jobs, running up to 10 jobs in parallel
 *   boost::asio::io_context context;
 *   ceph::async::Throttle throttle(context.get_executor(), 10);
 *
 *   for (auto& job : jobs) {
 *     // request a throttle unit for this job
 *     throttle.async_get(1, [&] (boost::system::error_code ec) {
 *         if (!ec) { // throttle was granted
 *           job.process();
 *           throttle.put(1);
 *         }
 *       });
 *   }
 *   context.run();
 */
template <typename Executor1>
class Throttle;


namespace detail {

// Throttle base contains everything that doesn't depend on the Executor type
class BaseThrottle {
 public:
  BaseThrottle(size_t maximum);
  ~BaseThrottle();

  /// returns a number of previously-granted throttle units, then attempts to
  /// grant the next pending requests
  void put(size_t count);

  /// adjust the maximum throttle. if a previous call to async_set_maximum() is
  /// still pending, its completion handler is invoked with operation_aborted.
  /// if pending calls to async_get() can be satisfied by the new maximum
  /// throttle, their completion handlers are invoked
  void set_maximum(size_t count);

  /// cancel any pending calls to async_get() and async_set_maximum(), invoking
  /// their completion handlers with operation_aborted
  void cancel();

 protected:
  template <typename Executor1, typename Handler>
  void start_get(const Executor1& ex1, size_t count, Handler&& handler);

  template <typename Executor1, typename Handler>
  void start_set_maximum(const Executor1& ex1, size_t count, Handler&& handler);

 private:
  std::mutex mutex;
  size_t value = 0; //< amount of outstanding throttle
  size_t maximum; //< maximum amount of throttle

  struct GetRequest : public boost::intrusive::list_base_hook<> {
    const size_t count; //< number of throttle units requested
    explicit GetRequest(size_t count) noexcept : count(count) {}
  };
  using GetCompletion = Completion<void(boost::system::error_code),
                                   AsBase<GetRequest>>;
  using MaxCompletion = Completion<void(boost::system::error_code)>;

  using GetCompletionList = boost::intrusive::list<GetCompletion>;
  /// maintain a list of throttle requests that we're not yet able to satisfy
  GetCompletionList get_requests;

  /// allow a single request to adjust the maximum throttle
  std::unique_ptr<MaxCompletion> max_request;

  /// move any pending throttle requests that we can grant into the given list
  void grant_pending(std::lock_guard<std::mutex>& lock,
                     GetCompletionList& granted) noexcept;

  /// pass each completion to the given callback function. if the callback
  /// throws, any remaining completions are freed
  template <typename Func> // Func(unique_ptr<GetCompletion>&&)
  static void safe_for_each(GetCompletionList&& requests, const Func& func);
};

template <typename Executor1, typename Handler>
void BaseThrottle::start_get(const Executor1& ex1, size_t count,
                             Handler&& handler)
{
  std::lock_guard lock{mutex};

  // if the throttle is available, grant it
  if (get_requests.empty() && value + count <= maximum) {
    value += count;

    auto ex2 = boost::asio::get_associated_executor(handler, ex1);
    auto alloc2 = boost::asio::get_associated_allocator(handler);
    auto b = bind_handler(std::move(handler), boost::system::error_code{});
    ex2.post(std::move(b), alloc2);
  } else {
    // create a Request and add it to the pending list
    auto request = GetCompletion::create(ex1, std::move(handler), count);
    // transfer ownership to the list (push_back doesn't throw)
    get_requests.push_back(*request.release());
  }
}

template <typename Executor1, typename Handler>
void BaseThrottle::start_set_maximum(const Executor1& ex1, size_t count,
                                     Handler&& handler)
{
  GetCompletionList granted;
  std::unique_ptr<MaxCompletion> max;

  {
    std::lock_guard lock{mutex};

    max = std::move(max_request);

    maximum = count;

    if (value <= maximum) {
      // complete successfully
      auto ex2 = boost::asio::get_associated_executor(handler, ex1);
      auto alloc2 = boost::asio::get_associated_allocator(handler);
      auto b = bind_handler(std::move(handler), boost::system::error_code{});
      ex2.post(std::move(b), alloc2);
    } else {
      // create a completion for later invocation
      max_request = MaxCompletion::create(ex1, std::move(handler));
    }

    grant_pending(lock, granted);
  }

  // dispatch successful completions. this has to happen outside the lock,
  // because dispatch() may execute inline and call back into Throttle
  safe_for_each(std::move(granted), [] (auto c) {
      GetCompletion::dispatch(std::move(c), boost::system::error_code{});
    });
  if (max) {
    // cancel the previous async_max_request() completion
    MaxCompletion::dispatch(std::move(max),
                            boost::asio::error::operation_aborted);
  }
}

template <typename Func>
void BaseThrottle::safe_for_each(GetCompletionList&& requests, const Func& func)
{
  try {
    while (!requests.empty()) {
      auto& request = requests.front();
      requests.pop_front();
      // transfer ownership of the request to the callback
      func(std::unique_ptr<GetCompletion>{&request});
    }
  } catch (...) {
    // if the callback throws, clean up any remaining completions
    requests.clear_and_dispose(std::default_delete<GetCompletion>{});
    throw;
  }
}

} // namespace detail


template <typename Executor1>
class Throttle : public detail::BaseThrottle {
  Executor1 ex1; //< default callback executor
 public:
  Throttle(const Executor1& ex1, size_t maximum)
    : BaseThrottle(maximum), ex1(ex1)
  {}

  using executor_type = Executor1;
  executor_type get_executor() const noexcept { return ex1; }

  /// requests a number of throttle units, invoking the given completion handler
  /// with a successful error code once those units are available
  template <typename CompletionToken>
  auto async_get(size_t count, CompletionToken&& token)
  {
    using Signature = void(boost::system::error_code);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    start_get(ex1, count, std::move(init.completion_handler));
    return init.result.get();
  }

  /// adjust the maximum throttle, invoking the given completion handler once
  /// the number of granted throttle units are at or below the new maximum
  /// value. if a previous call to async_set_maximum() is still pending, its
  /// completion handler is invoked with operation_aborted. if pending calls to
  /// async_get() can be satisfied by the new maximum throttle, their completion
  /// handlers are invoked
  template <typename CompletionToken>
  auto async_set_maximum(size_t count, CompletionToken&& token)
  {
    using Signature = void(boost::system::error_code);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    start_set_maximum(ex1, count, std::move(init.completion_handler));
    return init.result.get();
  }
};

} // namespace ceph::async

#endif // CEPH_ASYNC_THROTTLE_H
