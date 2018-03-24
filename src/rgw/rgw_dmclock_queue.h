// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef RGW_DMCLOCK_QUEUE_H
#define RGW_DMCLOCK_QUEUE_H

#include <boost/asio.hpp>
#include "common/ceph_time.h"
#include "common/async/completion.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "rgw_dmclock.h"

namespace rgw::dmclock {

namespace queue_counters {

enum {
  l_first = 427150,
  l_qlen,
  l_cost,
  l_res,
  l_res_cost,
  l_prio,
  l_prio_cost,
  l_limit,
  l_limit_cost,
  l_cancel,
  l_cancel_cost,
  l_res_latency,
  l_prio_latency,
  l_last,
};

PerfCountersRef build(CephContext *cct, const std::string& name);

} // namespace queue_counters

/// function to provide client counters
using GetClientCounters = std::function<PerfCounters*(client_id)>;

namespace async = ceph::async;


/*
 * A dmclock request scheduling service for use with boost::asio.
 */
class PriorityQueue : public md_config_obs_t {
 public:
  template <typename ...Args> // args forwarded to PullPriorityQueue ctor
  PriorityQueue(CephContext *cct, boost::asio::io_context& context,
                GetClientCounters&& counters, md_config_obs_t *observer,
                Args&& ...args);
  ~PriorityQueue();

  using executor_type = boost::asio::io_context::executor_type;

  /// return the default executor for async_request() callbacks
  executor_type get_executor() noexcept {
    return timer.get_executor();
  }

  /// submit an async request for dmclock scheduling. the given completion
  /// handler will be invoked with (error_code, PhaseType) when the request
  /// is ready or canceled
  template <typename CompletionToken>
  auto async_request(const client_id& client, const ReqParams& params,
                     const Time& time, Cost cost, CompletionToken&& token);

  /// cancel all queued requests, invoking their completion handlers with an
  /// operation_aborted error and default-constructed result
  void cancel();

  /// cancel all queued requests for a given client, invoking their completion
  /// handler with an operation_aborted error and default-constructed result
  void cancel(const client_id& client);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const md_config_t *conf,
                          const std::set<std::string>& changed) override;

 private:
  struct Request {
    client_id client;
    Time started;
    Cost cost;
  };
  using Queue = crimson::dmclock::PullPriorityQueue<client_id, Request>;
  using RequestRef = typename Queue::RequestRef;
  Queue queue; //< dmclock priority queue

  using Signature = void(boost::system::error_code, PhaseType);
  using Completion = async::Completion<Signature, async::AsBase<Request>>;

  using Clock = ceph::coarse_real_clock;
  using Timer = boost::asio::basic_waitable_timer<Clock>;
  Timer timer; //< timer for the next scheduled request

  CephContext *const cct;
  md_config_obs_t *const observer; //< observer to update ClientInfoFunc
  GetClientCounters counters; //< provides per-client perf counters

  /// set a timer to process the next request
  void schedule(const Time& time);

  /// process ready requests, then schedule the next pending request
  void process(const Time& now);
};


template <typename ...Args>
PriorityQueue::PriorityQueue(CephContext *cct, boost::asio::io_context& context,
                             GetClientCounters&& counters,
                             md_config_obs_t *observer, Args&& ...args)
  : queue(std::forward<Args>(args)...),
    timer(context), cct(cct), observer(observer),
    counters(std::move(counters))
{
  if (observer) {
    cct->_conf->add_observer(this);
  }
}

template <typename CompletionToken>
auto PriorityQueue::async_request(const client_id& client,
                                  const ReqParams& params,
                                  const Time& time, Cost cost,
                                  CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);

  auto ex1 = get_executor();
  auto& handler = init.completion_handler;

  // allocate the request and add it to the queue
  auto req = Completion::create(ex1, std::move(handler),
                                Request{client, time, cost});
  int r = 0; // TODO: https://github.com/ceph/dmclock/pull/50
  queue.add_request(std::move(req), client, params, time, cost);
  if (r == 0) {
    // schedule an immediate call to process() on the executor
    schedule(crimson::dmclock::TimeZero);
    if (auto c = counters(client)) {
      c->inc(queue_counters::l_qlen);
      c->inc(queue_counters::l_cost, cost);
    }
  } else {
    boost::system::error_code ec(r, boost::system::system_category());
    async::post(std::move(req), ec, PhaseType::priority);
    if (auto c = counters(client)) {
      c->inc(queue_counters::l_limit);
      c->inc(queue_counters::l_limit_cost, cost);
    }
  }

  return init.result.get();
}


/// array of per-client counters to serve as GetClientCounters
class ClientCounters {
  std::array<PerfCountersRef, static_cast<size_t>(client_id::count)> clients;
 public:
  ClientCounters(CephContext *cct);

  PerfCounters* operator()(client_id client) const {
    return clients[static_cast<size_t>(client)].get();
  }
};

} // namespace rgw::dmclock

#endif // RGW_DMCLOCK_QUEUE_H
