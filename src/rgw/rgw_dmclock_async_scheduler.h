// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "common/async/completion.h"

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include "rgw_dmclock_scheduler.h"
#include "rgw_dmclock_scheduler_ctx.h"

namespace rgw::dmclock {
  namespace async = ceph::async;

/*
 * A dmclock request scheduling service for use with boost::asio.
 *
 * An asynchronous dmclock priority queue, where scheduled requests complete
 * on a boost::asio executor.
 */
class AsyncScheduler : public md_config_obs_t, public Scheduler {
 public:
  template <typename ...Args> // args forwarded to PullPriorityQueue ctor
  AsyncScheduler(CephContext *cct, boost::asio::io_context& context,
            GetClientCounters&& counters, md_config_obs_t *observer,
            Args&& ...args);
  ~AsyncScheduler();

  using executor_type = boost::asio::io_context::executor_type;

  /// return the default executor for async_request() callbacks
  executor_type get_executor() noexcept {
    return timer.get_executor();
  }

  /// submit an async request for dmclock scheduling. the given completion
  /// handler will be invoked with (error_code, PhaseType) when the request
  /// is ready or canceled. on success, this grants a throttle unit that must
  /// be returned with a call to request_complete()
  template <typename CompletionToken>
  auto async_request(const client_id& client, const ReqParams& params,
                     const Time& time, Cost cost, CompletionToken&& token);

  /// returns a throttle unit granted by async_request()
  void request_complete() override;

  /// cancel all queued requests, invoking their completion handlers with an
  /// operation_aborted error and default-constructed result
  void cancel();

  /// cancel all queued requests for a given client, invoking their completion
  /// handler with an operation_aborted error and default-constructed result
  void cancel(const client_id& client);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;

 private:
  int schedule_request_impl(const client_id& client, const ReqParams& params,
                            const Time& time, const Cost& cost,
                            optional_yield yield_ctx) override;

  static constexpr bool IsDelayed = false;
  using Queue = crimson::dmclock::PullPriorityQueue<client_id, Request, IsDelayed>;
  using RequestRef = typename Queue::RequestRef;
  Queue queue; //< dmclock priority queue

  using Signature = void(boost::system::error_code, PhaseType);
  using Completion = async::Completion<Signature, async::AsBase<Request>>;

  using Clock = ceph::coarse_real_clock;
  using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, executor_type>;
  Timer timer; //< timer for the next scheduled request

  CephContext *const cct;
  md_config_obs_t *const observer; //< observer to update ClientInfoFunc
  GetClientCounters counters; //< provides per-client perf counters

  /// max request throttle
  std::atomic<int64_t> max_requests;
  std::atomic<int64_t> outstanding_requests = 0;

  /// set a timer to process the next request
  void schedule(const Time& time);

  /// process ready requests, then schedule the next pending request
  void process(const Time& now);
};


template <typename ...Args>
AsyncScheduler::AsyncScheduler(CephContext *cct, boost::asio::io_context& context,
                               GetClientCounters&& counters,
                               md_config_obs_t *observer, Args&& ...args)
  : queue(std::forward<Args>(args)...),
    timer(context), cct(cct), observer(observer),
    counters(std::move(counters)),
    max_requests(cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests"))
{
  if (max_requests <= 0) {
    max_requests = std::numeric_limits<int64_t>::max();
  }
  if (observer) {
    cct->_conf.add_observer(this);
  }
}

template <typename CompletionToken>
auto AsyncScheduler::async_request(const client_id& client,
                              const ReqParams& params,
                              const Time& time, Cost cost,
                              CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, auto ex, const client_id& client,
              const ReqParams& params, const Time& time, Cost cost) {
        // allocate the Request and add it to the queue
        auto completion = Completion::create(ex, std::move(handler),
                                             Request{client, time, cost});
        // cast to unique_ptr<Request>
        auto req = RequestRef{std::move(completion)};
        int r = queue.add_request(std::move(req), client, params, time, cost);
        if (r == 0) {
          // schedule an immediate call to process() on the executor
          schedule(crimson::dmclock::TimeZero);
          if (auto c = counters(client)) {
            c->inc(queue_counters::l_qlen);
            c->inc(queue_counters::l_cost, cost);
          }
        } else {
          // post the error code
          boost::system::error_code ec(r, boost::system::system_category());
          // cast back to Completion
          auto completion = static_cast<Completion*>(req.release());
          async::post(std::unique_ptr<Completion>{completion},
                      ec, PhaseType::priority);
          if (auto c = counters(client)) {
            c->inc(queue_counters::l_limit);
            c->inc(queue_counters::l_limit_cost, cost);
          }
        }
      }, token, get_executor(), client, params, time, cost);
}

class SimpleThrottler : public md_config_obs_t, public dmclock::Scheduler {
public:
  SimpleThrottler(CephContext *cct) :
    max_requests(cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests")),
    counters(cct, "simple-throttler")
  {
    if (max_requests <= 0) {
      max_requests = std::numeric_limits<int64_t>::max();
    }
    cct->_conf.add_observer(this);
  }

  const char** get_tracked_conf_keys() const override {
    static const char* keys[] = { "rgw_max_concurrent_requests", nullptr };
    return keys;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override
  {
    if (changed.count("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      max_requests = new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
    }
  }

  void request_complete() override {
    --outstanding_requests;
    if (auto c = counters();
        c != nullptr) {
      c->inc(throttle_counters::l_outstanding, -1);
    }

  }

private:
  int schedule_request_impl(const client_id&, const ReqParams&,
                            const Time&, const Cost&,
                            optional_yield) override {
    if (outstanding_requests++ >= max_requests) {
      if (auto c = counters();
          c != nullptr) {
        c->inc(throttle_counters::l_outstanding);
        c->inc(throttle_counters::l_throttle);
      }
      return -EAGAIN;
    }

    return 0 ;
  }

  std::atomic<int64_t> max_requests;
  std::atomic<int64_t> outstanding_requests = 0;
  ThrottleCounters counters;
};

} // namespace rgw::dmclock
