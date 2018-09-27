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

#ifndef RGW_DMCLOCK_SCHEDULER_H
#define RGW_DMCLOCK_SCHEDULER_H

#include "common/ceph_time.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "rgw_dmclock.h"
#include "rgw_yield_context.h"

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
			optional_yield_context yield)
  {
    int r = schedule_request_impl(client,params,time,cost,yield);
    return std::make_pair(r,SchedulerCompleter(std::bind(&Scheduler::request_complete,this)));
  }
  virtual void request_complete() {};

  virtual ~Scheduler() {};
private:
  virtual int schedule_request_impl(const client_id&, const ReqParams&,
				    const Time&, const Cost&,
				    optional_yield_context) = 0;
};

/// array of per-client counters to serve as GetClientCounters
class ClientCounters {
  std::array<PerfCountersRef, static_cast<size_t>(client_id::count)> clients;
 public:
  ClientCounters(CephContext *cct);

  PerfCounters* operator()(client_id client) const {
    return clients[static_cast<size_t>(client)].get();
  }
};

struct ClientSum {
  uint64_t count{0};
  Cost cost{0};
};

constexpr auto client_count = static_cast<size_t>(client_id::count);
using ClientSums = std::array<ClientSum, client_count>;

void inc(ClientSums& sums, client_id client, Cost cost);
void on_cancel(PerfCounters *c, const ClientSum& sum);
void on_process(PerfCounters* c, const ClientSum& rsum, const ClientSum& psum);

/// a simple wrapper to hold client config. objects needed to construct a
/// scheduler instance, the primary utility of this being to optionally
/// construct scheduler only when configured in the frontends.
class optional_scheduler_ctx {
  std::optional<ClientConfig> clients;
  std::optional<ClientCounters> counters;
public:
  optional_scheduler_ctx(CephContext *cct) {
    if(cct->_conf.get_val<bool>("rgw_dmclock_enabled")){
      clients.emplace(ClientConfig(cct));
      counters.emplace(ClientCounters(cct));
    }
  }
  operator bool() const noexcept { return counters && clients; }
  // both the get functions below will throw
  ClientCounters& get_counters() { return counters.value(); }
  ClientConfig& get_clients() { return clients.value(); }
};
} // namespace rgw::dmclock

#endif // RGW_DMCLOCK_SCHEDULER_H
