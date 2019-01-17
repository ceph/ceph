// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *           (C) 2019 SUSE Linux LLC
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "rgw_dmclock_scheduler_ctx.h"

namespace rgw::dmclock {

ClientConfig::ClientConfig(CephContext *cct)
{
  update(cct->_conf);
}

ClientInfo* ClientConfig::operator()(client_id client)
{
  return &clients[static_cast<size_t>(client)];
}

const char** ClientConfig::get_tracked_conf_keys() const
{
  static const char* keys[] = {
    "rgw_dmclock_admin_res",
    "rgw_dmclock_admin_wgt",
    "rgw_dmclock_admin_lim",
    "rgw_dmclock_auth_res",
    "rgw_dmclock_auth_wgt",
    "rgw_dmclock_auth_lim",
    "rgw_dmclock_data_res",
    "rgw_dmclock_data_wgt",
    "rgw_dmclock_data_lim",
    "rgw_dmclock_metadata_res",
    "rgw_dmclock_metadata_wgt",
    "rgw_dmclock_metadata_lim",
    "rgw_max_concurrent_requests",
    nullptr
  };
  return keys;
}

void ClientConfig::update(const ConfigProxy& conf)
{
  clients.clear();
  static_assert(0 == static_cast<int>(client_id::admin));
  clients.emplace_back(conf.get_val<double>("rgw_dmclock_admin_res"),
                       conf.get_val<double>("rgw_dmclock_admin_wgt"),
                       conf.get_val<double>("rgw_dmclock_admin_lim"));
  static_assert(1 == static_cast<int>(client_id::auth));
  clients.emplace_back(conf.get_val<double>("rgw_dmclock_auth_res"),
                       conf.get_val<double>("rgw_dmclock_auth_wgt"),
                       conf.get_val<double>("rgw_dmclock_auth_lim"));
  static_assert(2 == static_cast<int>(client_id::data));
  clients.emplace_back(conf.get_val<double>("rgw_dmclock_data_res"),
                       conf.get_val<double>("rgw_dmclock_data_wgt"),
                       conf.get_val<double>("rgw_dmclock_data_lim"));
  static_assert(3 == static_cast<int>(client_id::metadata));
  clients.emplace_back(conf.get_val<double>("rgw_dmclock_metadata_res"),
                       conf.get_val<double>("rgw_dmclock_metadata_wgt"),
                       conf.get_val<double>("rgw_dmclock_metadata_lim"));
}

void ClientConfig::handle_conf_change(const ConfigProxy& conf,
                                      const std::set<std::string>& changed)
{
  update(conf);
}

ClientCounters::ClientCounters(CephContext *cct)
{
  clients[static_cast<size_t>(client_id::admin)] =
      queue_counters::build(cct, "dmclock-admin");
  clients[static_cast<size_t>(client_id::auth)] =
      queue_counters::build(cct, "dmclock-auth");
  clients[static_cast<size_t>(client_id::data)] =
      queue_counters::build(cct, "dmclock-data");
  clients[static_cast<size_t>(client_id::metadata)] =
      queue_counters::build(cct, "dmclock-metadata");
  clients[static_cast<size_t>(client_id::count)] =
      throttle_counters::build(cct, "dmclock-scheduler");
}

void inc(ClientSums& sums, client_id client, Cost cost)
{
  auto& sum = sums[static_cast<size_t>(client)];
  sum.count++;
  sum.cost += cost;
}

void on_cancel(PerfCounters *c, const ClientSum& sum)
{
  if (sum.count) {
    c->dec(queue_counters::l_qlen, sum.count);
    c->inc(queue_counters::l_cancel, sum.count);
  }
  if (sum.cost) {
    c->dec(queue_counters::l_cost, sum.cost);
    c->inc(queue_counters::l_cancel_cost, sum.cost);
  }
}

void on_process(PerfCounters* c, const ClientSum& rsum, const ClientSum& psum)
{
  if (rsum.count) {
    c->inc(queue_counters::l_res, rsum.count);
  }
  if (rsum.cost) {
    c->inc(queue_counters::l_res_cost, rsum.cost);
  }
  if (psum.count) {
    c->inc(queue_counters::l_prio, psum.count);
  }
  if (psum.cost) {
    c->inc(queue_counters::l_prio_cost, psum.cost);
  }
  if (rsum.count + psum.count) {
    c->dec(queue_counters::l_qlen, rsum.count + psum.count);
  }
  if (rsum.cost + psum.cost) {
    c->dec(queue_counters::l_cost, rsum.cost + psum.cost);
  }
}
} // namespace rgw::dmclock

namespace queue_counters {

PerfCountersRef build(CephContext *cct, const std::string& name)
{
  if (!cct->_conf->throttler_perf_counter) {
    return {};
  }

  PerfCountersBuilder b(cct, name, l_first, l_last);
  b.add_u64(l_qlen, "qlen", "Queue size");
  b.add_u64(l_cost, "cost", "Cost of queued requests");
  b.add_u64_counter(l_res, "res", "Requests satisfied by reservation");
  b.add_u64_counter(l_res_cost, "res_cost", "Cost satisfied by reservation");
  b.add_u64_counter(l_prio, "prio", "Requests satisfied by priority");
  b.add_u64_counter(l_prio_cost, "prio_cost", "Cost satisfied by priority");
  b.add_u64_counter(l_limit, "limit", "Requests rejected by limit");
  b.add_u64_counter(l_limit_cost, "limit_cost", "Cost rejected by limit");
  b.add_u64_counter(l_cancel, "cancel", "Cancels");
  b.add_u64_counter(l_cancel_cost, "cancel_cost", "Canceled cost");
  b.add_time_avg(l_res_latency, "res latency", "Reservation latency");
  b.add_time_avg(l_prio_latency, "prio latency", "Priority latency");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // namespace queue_counters

namespace throttle_counters {

PerfCountersRef build(CephContext *cct, const std::string& name)
{
  if (!cct->_conf->throttler_perf_counter) {
    return {};
  }

  PerfCountersBuilder b(cct, name, l_first, l_last);
  b.add_u64(l_throttle, "throttle", "Requests throttled");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // namespace throttle_counters
