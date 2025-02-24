// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *           (C) 2019 SUSE LLC
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "common/ceph_time.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/async/yield_context.h"
#include "rgw_dmclock.h"

namespace rgw::dmclock {

using crimson::dmclock::ReqParams;
using crimson::dmclock::PhaseType;
using crimson::dmclock::AtLimit;
using crimson::dmclock::Time;
using crimson::dmclock::get_time;

/// function to provide client counters
using GetClientCounters = std::function<PerfCounters*(client_id)>;

struct Request {
  client_id client;
  Time started;
  Cost cost;
};

enum class ReqState {
  Wait,
  Ready,
  Cancelled
};

template <typename F>
class Completer {
public:
  Completer(F &&f): f(std::move(f)) {}
  // Default constructor is needed as we need to create an empty completer
  // that'll be move assigned later in process request
  Completer() = default;
  ~Completer() {
    if (f) {
      f();
    }
  }
  Completer(const Completer&) = delete;
  Completer& operator=(const Completer&) = delete;
  Completer(Completer&& other) = default;
  Completer& operator=(Completer&& other) = default;
private:
  F f;
};

using SchedulerCompleter = Completer<std::function<void()>>;

class Scheduler  {
public:
  auto schedule_request(const client_id& client, const ReqParams& params,
			const Time& time, const Cost& cost,
			optional_yield yield)
  {
    int r = schedule_request_impl(client,params,time,cost,yield);
    return std::make_pair(r,SchedulerCompleter(std::bind(&Scheduler::request_complete,this)));
  }
  virtual void request_complete() {};

  virtual ~Scheduler() {};
private:
  virtual int schedule_request_impl(const client_id&, const ReqParams&,
				    const Time&, const Cost&,
				    optional_yield) = 0;
};

} // namespace rgw::dmclock
