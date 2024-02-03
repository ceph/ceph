// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 SUSE Linux Gmbh
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_dmclock_scheduler.h"
#include "rgw_dmclock_scheduler_ctx.h"

namespace rgw::dmclock {
// For a blocking SyncRequest we hold a reference to a cv and the caller must
// ensure the lifetime
struct SyncRequest : public Request {
  std::mutex& req_mtx;
  std::condition_variable& req_cv;
  ReqState& req_state;
  GetClientCounters& counters;
  explicit SyncRequest(client_id _id, Time started, Cost cost,
                       std::mutex& mtx, std::condition_variable& _cv,
                       ReqState& _state, GetClientCounters& counters):
    Request{_id, started, cost}, req_mtx(mtx), req_cv(_cv), req_state(_state), counters(counters) {};
};

class SyncScheduler: public Scheduler {
public:
  template <typename ...Args>
  SyncScheduler(CephContext *cct, GetClientCounters&& counters,
		Args&& ...args);
  ~SyncScheduler();

  // submit a blocking request for dmclock scheduling, this function waits until
  // the request is ready.
  int add_request(const client_id& client, const ReqParams& params,
		  const Time& time, Cost cost);


  void cancel();

  void cancel(const client_id& client);

  static void handle_request_cb(const client_id& c, std::unique_ptr<SyncRequest> req,
				PhaseType phase, Cost cost);
private:
  int schedule_request_impl(const client_id& client, const ReqParams& params,
			    const Time& time, const Cost& cost,
			    optional_yield _y [[maybe_unused]]) override
  {
    return add_request(client, params, time, cost);
  }

  static constexpr bool IsDelayed = false;
  using Queue = crimson::dmclock::PushPriorityQueue<client_id, SyncRequest, IsDelayed>;
  using RequestRef = typename Queue::RequestRef;
  using Clock = ceph::coarse_real_clock;

  Queue queue;
  CephContext const *cct;
  GetClientCounters counters; //< provides per-client perf counters
};

template <typename ...Args>
SyncScheduler::SyncScheduler(CephContext *cct, GetClientCounters&& counters,
			     Args&& ...args):
  queue(std::forward<Args>(args)...), cct(cct), counters(std::move(counters))
{}

} // namespace rgw::dmclock
