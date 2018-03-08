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

#include "common/ceph_context.h"
#include "common/config.h"
#include "throttle.h"

namespace ceph::async {

namespace detail {

BaseThrottle::BaseThrottle(size_t maximum, PerfCountersRef&& counters)
  : maximum(maximum), counters(std::move(counters))
{
  on_set_maximum();
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
    on_put(count);

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
    on_set_maximum();
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
    on_canceled(requests);

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

  on_granted(granted);
}

void BaseThrottle::on_set_maximum()
{
  if (counters) {
    counters->set(throttle_counters::l_max, maximum);
  }
}

void BaseThrottle::on_get(size_t count)
{
  if (counters) {
    counters->set(throttle_counters::l_val, value);
    counters->inc(throttle_counters::l_get, count);
    counters->inc(throttle_counters::l_get_sum, count);
    auto latency = ceph::timespan{}; // no wait time
    counters->tinc(throttle_counters::l_latency, latency);
  }
}

void BaseThrottle::on_wait(size_t count, Clock::time_point *started)
{
  if (counters) {
    counters->inc(throttle_counters::l_pending, count);
    counters->inc(throttle_counters::l_wait);
    counters->inc(throttle_counters::l_wait_sum, count);
    *started = Clock::now();
  }
}

void BaseThrottle::on_put(size_t count)
{
  if (counters) {
    counters->inc(throttle_counters::l_put);
    counters->inc(throttle_counters::l_put_sum, count);
  }
}

void BaseThrottle::on_granted(const GetCompletionList& requests)
{
  if (!counters) {
    return;
  }
  counters->set(throttle_counters::l_val, value);
  if (requests.empty()) {
    return;
  }
  uint64_t sum = 0;
  auto now = Clock::now();
  for (auto& request : requests) {
    sum += request.count;
    counters->tinc(throttle_counters::l_latency, now - request.started);
  }
  counters->dec(throttle_counters::l_pending, sum);
  counters->inc(throttle_counters::l_get, requests.size());
  counters->inc(throttle_counters::l_get_sum, sum);
}

void BaseThrottle::on_canceled(const GetCompletionList& requests)
{
  if (!counters || requests.empty()) {
    return;
  }
  counters->inc(throttle_counters::l_cancel, requests.size());
  uint64_t sum = 0;
  for (auto& request : requests) { sum += request.count; }
  counters->inc(throttle_counters::l_cancel_sum, sum);
  counters->dec(throttle_counters::l_pending, sum);
}

} // namespace detail

namespace throttle_counters {

PerfCountersRef build(CephContext *cct, const std::string& name)
{
  if (!cct->_conf->throttler_perf_counter) {
    return {};
  }

  PerfCountersBuilder b(cct, name, l_first, l_last);
  b.add_u64(l_val, "val", "Granted throttle");
  b.add_u64(l_max, "max", "Maximum throttle");
  b.add_u64(l_pending, "pending", "Pending throttle");
  b.add_u64_counter(l_get, "get", "Gets");
  b.add_u64_counter(l_get_sum, "get_sum", "Got data");
  b.add_u64_counter(l_put, "put", "Puts");
  b.add_u64_counter(l_put_sum, "put_sum", "Put data");
  b.add_u64_counter(l_wait, "wait", "Waits");
  b.add_u64_counter(l_wait_sum, "wait_sum", "Wait data");
  b.add_u64_counter(l_cancel, "cancel", "Cancels");
  b.add_u64_counter(l_cancel_sum, "cancel_sum", "Canceled data");
  b.add_time_avg(l_latency, "latency", "Waiting latency");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // namespace throttle_counters

} // namespace ceph::async
