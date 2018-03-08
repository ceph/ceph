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

#include "throttle.h"

namespace ceph::async {

namespace detail {

BaseThrottle::BaseThrottle(size_t maximum) : maximum(maximum)
{
}

BaseThrottle::~BaseThrottle()
{
  cancel();
}

void BaseThrottle::put(size_t count)
{
  GetCompletionList granted;
  std::unique_ptr<MaxCompletion> max;
  {
    std::lock_guard lock{mutex};

    assert(value >= count);
    value -= count;

    if (value <= maximum) {
      max = std::move(max_request);
    }

    grant_pending(lock, granted);
  }

  // invoke any successful completions
  safe_for_each(std::move(granted), [] (auto c) {
      // put() is likely being called from a continuation of async_get(), so use
      // post() instead of dispatch() to avoid recursing
      GetCompletion::post(std::move(c), boost::system::error_code{});
    });
  if (max) {
    MaxCompletion::dispatch(std::move(max), boost::system::error_code{});
  }
}

void BaseThrottle::set_maximum(size_t count)
{
  GetCompletionList granted;
  std::unique_ptr<MaxCompletion> max;
  {
    std::lock_guard lock{mutex};

    maximum = count;
    max = std::move(max_request);
    grant_pending(lock, granted);
  }

  // dispatch successful completions
  safe_for_each(std::move(granted), [] (auto c) {
      GetCompletion::dispatch(std::move(c), boost::system::error_code{});
    });
  if (max) {
    // cancel the previous max request
    MaxCompletion::dispatch(std::move(max),
                            boost::asio::error::operation_aborted);
  }
}

void BaseThrottle::cancel()
{
  GetCompletionList requests;
  std::unique_ptr<MaxCompletion> max;
  {
    std::lock_guard lock{mutex};

    requests = std::move(get_requests);
    max = std::move(max_request);
  }

  // cancel all outstanding completions with operation_aborted
  safe_for_each(std::move(requests), [] (auto c) {
      GetCompletion::dispatch(std::move(c),
                              boost::asio::error::operation_aborted);
    });
  if (max) {
    MaxCompletion::dispatch(std::move(max),
                            boost::asio::error::operation_aborted);
  }
}

void BaseThrottle::grant_pending(std::lock_guard<std::mutex>& lock,
                                 GetCompletionList& granted) noexcept
{
  while (!get_requests.empty()) {
    auto& request = get_requests.front();
    if (value + request.count > maximum) {
      break;
    }
    value += request.count;

    // transfer ownership from get_requests to granted
    get_requests.pop_front();
    granted.push_back(request);
  }
}

} // namespace detail
} // namespace ceph::async
