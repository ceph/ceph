// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>
#include <functional>

#include "osd/scheduler/mClockScheduler.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace ceph::osd::scheduler {

mClockScheduler::mClockScheduler(CephContext *cct,
  uint32_t num_shards,
  bool is_rotational)
  : cct(cct),
    num_shards(num_shards),
    is_rotational(is_rotational),
    scheduler(
      std::bind(&mClockScheduler::ClientRegistry::get_info,
                &client_registry,
                _1),
      dmc::AtLimit::Wait,
      cct->_conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
{
  cct->_conf.add_observer(this);
  ceph_assert(num_shards > 0);
  set_max_osd_capacity();
  set_osd_mclock_cost_per_io();
  set_osd_mclock_cost_per_byte();
  set_mclock_profile();
  enable_mclock_profile_settings();
  client_registry.update_from_config(cct->_conf);
}

void mClockScheduler::ClientRegistry::update_from_config(const ConfigProxy &conf)
{
  default_external_client_info.update(
    conf.get_val<uint64_t>("osd_mclock_scheduler_client_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_client_wgt"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_client_lim"));

  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_recovery_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_recovery_wgt"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_recovery_lim"));

  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_best_effort_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_best_effort_wgt"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_best_effort_lim"));
}

const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_external_client(
  const client_profile_id_t &client) const
{
  auto ret = external_client_infos.find(client);
  if (ret == external_client_infos.end())
    return &default_external_client_info;
  else
    return &(ret->second);
}

const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_info(
  const scheduler_id_t &id) const {
  switch (id.class_id) {
  case op_scheduler_class::immediate:
    ceph_assert(0 == "Cannot schedule immediate");
    return (dmc::ClientInfo*)nullptr;
  case op_scheduler_class::client:
    return get_external_client(id.client_profile_id);
  default:
    ceph_assert(static_cast<size_t>(id.class_id) < internal_client_infos.size());
    return &internal_client_infos[static_cast<size_t>(id.class_id)];
  }
}

void mClockScheduler::set_max_osd_capacity()
{
  if (is_rotational) {
    max_osd_capacity =
      cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_hdd");
  } else {
    max_osd_capacity =
      cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_ssd");
  }
  // Set per op-shard iops limit
  max_osd_capacity /= num_shards;
  dout(1) << __func__ << " #op shards: " << num_shards
          << std::fixed << std::setprecision(2)
          << " max osd capacity(iops) per shard: " << max_osd_capacity
          << dendl;
}

void mClockScheduler::set_osd_mclock_cost_per_io()
{
  std::chrono::seconds sec(1);
  if (cct->_conf.get_val<double>("osd_mclock_cost_per_io_usec")) {
    osd_mclock_cost_per_io =
      cct->_conf.get_val<double>("osd_mclock_cost_per_io_usec");
  } else {
    if (is_rotational) {
      osd_mclock_cost_per_io =
        cct->_conf.get_val<double>("osd_mclock_cost_per_io_usec_hdd");
      // For HDDs, convert value to seconds
      osd_mclock_cost_per_io /= std::chrono::microseconds(sec).count();
    } else {
      // For SSDs, convert value to milliseconds
      osd_mclock_cost_per_io =
        cct->_conf.get_val<double>("osd_mclock_cost_per_io_usec_ssd");
      osd_mclock_cost_per_io /= std::chrono::milliseconds(sec).count();
    }
  }
  dout(1) << __func__ << " osd_mclock_cost_per_io: "
          << std::fixed << std::setprecision(7) << osd_mclock_cost_per_io
          << dendl;
}

void mClockScheduler::set_osd_mclock_cost_per_byte()
{
  std::chrono::seconds sec(1);
  if (cct->_conf.get_val<double>("osd_mclock_cost_per_byte_usec")) {
    osd_mclock_cost_per_byte =
      cct->_conf.get_val<double>("osd_mclock_cost_per_byte_usec");
  } else {
    if (is_rotational) {
      osd_mclock_cost_per_byte =
        cct->_conf.get_val<double>("osd_mclock_cost_per_byte_usec_hdd");
      // For HDDs, convert value to seconds
      osd_mclock_cost_per_byte /= std::chrono::microseconds(sec).count();
    } else {
      osd_mclock_cost_per_byte =
        cct->_conf.get_val<double>("osd_mclock_cost_per_byte_usec_ssd");
      // For SSDs, convert value to milliseconds
      osd_mclock_cost_per_byte /= std::chrono::milliseconds(sec).count();
    }
  }
  dout(1) << __func__ << " osd_mclock_cost_per_byte: "
          << std::fixed << std::setprecision(7) << osd_mclock_cost_per_byte
          << dendl;
}

void mClockScheduler::set_mclock_profile()
{
  mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
  dout(1) << __func__ << " mclock profile: " << mclock_profile << dendl;
}

std::string mClockScheduler::get_mclock_profile()
{
  return mclock_profile;
}

void mClockScheduler::set_balanced_profile_allocations()
{
  // Client Allocation:
  //   reservation: 40% | weight: 1 | limit: 100% |
  // Background Recovery Allocation:
  //   reservation: 40% | weight: 1 | limit: 80% |
  // Background Best Effort Allocation:
  //   reservation: 20% | weight: 1 | limit: 50% |

  // Client
  uint64_t client_res = static_cast<uint64_t>(
    std::round(0.40 * max_osd_capacity));
  uint64_t client_lim = static_cast<uint64_t>(
    std::round(max_osd_capacity));
  uint64_t client_wgt = default_min;

  // Background Recovery
  uint64_t rec_res = static_cast<uint64_t>(
    std::round(0.40 * max_osd_capacity));
  uint64_t rec_lim = static_cast<uint64_t>(
    std::round(0.80 * max_osd_capacity));
  uint64_t rec_wgt = default_min;

  // Background Best Effort
  uint64_t best_effort_res = static_cast<uint64_t>(
    std::round(0.20 * max_osd_capacity));
  uint64_t best_effort_lim = static_cast<uint64_t>(
    std::round(0.50 * max_osd_capacity));
  uint64_t best_effort_wgt = default_min;

  // Set the allocations for the mclock clients
  client_allocs[
    static_cast<size_t>(op_scheduler_class::client)].update(
      client_res,
      client_wgt,
      client_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
      rec_res,
      rec_wgt,
      rec_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
      best_effort_res,
      best_effort_wgt,
      best_effort_lim);
}

void mClockScheduler::set_high_recovery_ops_profile_allocations()
{
  // Client Allocation:
  //   reservation: 30% | weight: 1 | limit: 80% |
  // Background Recovery Allocation:
  //   reservation: 60% | weight: 2 | limit: 100% |
  // Background Best Effort Allocation:
  //   reservation: 10% | weight: 1 | limit: 40% |

  // Client
  uint64_t client_res = static_cast<uint64_t>(
    std::round(0.30 * max_osd_capacity));
  uint64_t client_lim = static_cast<uint64_t>(
    std::round(0.80 * max_osd_capacity));
  uint64_t client_wgt = default_min;

  // Background Recovery
  uint64_t rec_res = static_cast<uint64_t>(
    std::round(0.60 * max_osd_capacity));
  uint64_t rec_lim = static_cast<uint64_t>(
    std::round(max_osd_capacity));
  uint64_t rec_wgt = 2;

  // Background Best Effort
  uint64_t best_effort_res = static_cast<uint64_t>(
    std::round(0.10 * max_osd_capacity));
  uint64_t best_effort_lim = static_cast<uint64_t>(
    std::round(0.40 * max_osd_capacity));
  uint64_t best_effort_wgt = default_min;

  // Set the allocations for the mclock clients
  client_allocs[
    static_cast<size_t>(op_scheduler_class::client)].update(
      client_res,
      client_wgt,
      client_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
      rec_res,
      rec_wgt,
      rec_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
      best_effort_res,
      best_effort_wgt,
      best_effort_lim);
}

void mClockScheduler::set_high_client_ops_profile_allocations()
{
  // Client Allocation:
  //   reservation: 60% | weight: 2 | limit: max |
  // Background Recovery Allocation:
  //   reservation: 25% | weight: 1 | limit: 50% |
  // Background Best Effort Allocation:
  //   reservation: 15% | weight: 1 | limit: 30% |

  // Client
  uint64_t client_res = static_cast<uint64_t>(
    std::round(0.60 * max_osd_capacity));
  uint64_t client_lim = default_max;
  uint64_t client_wgt = 2;

  // Background Recovery
  uint64_t rec_res = static_cast<uint64_t>(
    std::round(0.25 * max_osd_capacity));
  uint64_t rec_lim = static_cast<uint64_t>(
    std::round(0.50 * max_osd_capacity));
  uint64_t rec_wgt = default_min;

  // Background Best Effort
  uint64_t best_effort_res = static_cast<uint64_t>(
    std::round(0.15 * max_osd_capacity));
  uint64_t best_effort_lim = static_cast<uint64_t>(
    std::round(0.30 * max_osd_capacity));
  uint64_t best_effort_wgt = default_min;

  // Set the allocations for the mclock clients
  client_allocs[
    static_cast<size_t>(op_scheduler_class::client)].update(
      client_res,
      client_wgt,
      client_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
      rec_res,
      rec_wgt,
      rec_lim);
  client_allocs[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
      best_effort_res,
      best_effort_wgt,
      best_effort_lim);
}

void mClockScheduler::enable_mclock_profile_settings()
{
  // Nothing to do for "custom" profile
  if (mclock_profile == "custom") {
    return;
  }

  // Set mclock and ceph config options for the chosen profile
  if (mclock_profile == "balanced") {
    set_balanced_profile_allocations();
  } else if (mclock_profile == "high_recovery_ops") {
    set_high_recovery_ops_profile_allocations();
  } else if (mclock_profile == "high_client_ops") {
    set_high_client_ops_profile_allocations();
  } else {
    ceph_assert("Invalid choice of mclock profile" == 0);
    return;
  }

  // Set the mclock config parameters
  set_profile_config();
}

void mClockScheduler::set_profile_config()
{
  ClientAllocs client = client_allocs[
    static_cast<size_t>(op_scheduler_class::client)];
  ClientAllocs rec = client_allocs[
    static_cast<size_t>(op_scheduler_class::background_recovery)];
  ClientAllocs best_effort = client_allocs[
    static_cast<size_t>(op_scheduler_class::background_best_effort)];

  // Set external client params
  cct->_conf.set_val("osd_mclock_scheduler_client_res",
    std::to_string(client.res));
  cct->_conf.set_val("osd_mclock_scheduler_client_wgt",
    std::to_string(client.wgt));
  cct->_conf.set_val("osd_mclock_scheduler_client_lim",
    std::to_string(client.lim));
  dout(10) << __func__ << " client QoS params: " << "["
           << client.res << "," << client.wgt << "," << client.lim
           << "]" << dendl;

  // Set background recovery client params
  cct->_conf.set_val("osd_mclock_scheduler_background_recovery_res",
    std::to_string(rec.res));
  cct->_conf.set_val("osd_mclock_scheduler_background_recovery_wgt",
    std::to_string(rec.wgt));
  cct->_conf.set_val("osd_mclock_scheduler_background_recovery_lim",
    std::to_string(rec.lim));
  dout(10) << __func__ << " Recovery QoS params: " << "["
           << rec.res << "," << rec.wgt << "," << rec.lim
           << "]" << dendl;

  // Set background best effort client params
  cct->_conf.set_val("osd_mclock_scheduler_background_best_effort_res",
    std::to_string(best_effort.res));
  cct->_conf.set_val("osd_mclock_scheduler_background_best_effort_wgt",
    std::to_string(best_effort.wgt));
  cct->_conf.set_val("osd_mclock_scheduler_background_best_effort_lim",
    std::to_string(best_effort.lim));
  dout(10) << __func__ << " Best effort QoS params: " << "["
    << best_effort.res << "," << best_effort.wgt << "," << best_effort.lim
    << "]" << dendl;
}

int mClockScheduler::calc_scaled_cost(int item_cost)
{
  // Calculate total scaled cost in secs
  int scaled_cost =
    std::round(osd_mclock_cost_per_io + (osd_mclock_cost_per_byte * item_cost));
  return std::max(scaled_cost, 1);
}

void mClockScheduler::update_configuration()
{
  // Apply configuration change. The expectation is that
  // at least one of the tracked mclock config option keys
  // is modified before calling this method.
  cct->_conf.apply_changes(nullptr);
}

void mClockScheduler::dump(ceph::Formatter &f) const
{
  // Display queue sizes
  f.open_object_section("queue_sizes");
  f.dump_int("immediate", immediate.size());
  f.dump_int("scheduler", scheduler.request_count());
  f.close_section();

  // client map and queue tops (res, wgt, lim)
  std::ostringstream out;
  f.open_object_section("mClockClients");
  f.dump_int("client_count", scheduler.client_count());
  out << scheduler;
  f.dump_string("clients", out.str());
  f.close_section();

  // Display sorted queues (res, wgt, lim)
  f.open_object_section("mClockQueues");
  f.dump_string("queues", display_queues());
  f.close_section();
}

void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  auto id = get_scheduler_id(item);

  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (op_scheduler_class::immediate == id.class_id) {
    immediate.push_front(std::move(item));
  } else {
    int cost = calc_scaled_cost(item.get_cost());
    item.set_qos_cost(cost);
    dout(20) << __func__ << " " << id
             << " item_cost: " << item.get_cost()
             << " scaled_cost: " << cost
             << dendl;

    // Add item to scheduler queue
    scheduler.add_request(
      std::move(item),
      id,
      cost);
  }

 dout(20) << __func__ << " client_count: " << scheduler.client_count()
          << " queue_sizes: [ imm: " << immediate.size()
          << " sched: " << scheduler.request_count() << " ]"
          << dendl;
 dout(30) << __func__ << " mClockClients: "
          << scheduler
          << dendl;
 dout(30) << __func__ << " mClockQueues: { "
          << display_queues() << " }"
          << dendl;
}

void mClockScheduler::enqueue_front(OpSchedulerItem&& item)
{
  immediate.push_back(std::move(item));
  // TODO: item may not be immediate, update mclock machinery to permit
  // putting the item back in the queue
}

WorkItem mClockScheduler::dequeue()
{
  if (!immediate.empty()) {
    WorkItem work_item{std::move(immediate.back())};
    immediate.pop_back();
    return work_item;
  } else {
    mclock_queue_t::PullReq result = scheduler.pull_request();
    if (result.is_future()) {
      return result.getTime();
    } else if (result.is_none()) {
      ceph_assert(
	0 == "Impossible, must have checked empty() first");
      return {};
    } else {
      ceph_assert(result.is_retn());

      auto &retn = result.get_retn();
      return std::move(*retn.request);
    }
  }
}

std::string mClockScheduler::display_queues() const
{
  std::ostringstream out;
  scheduler.display_queues(out);
  return out.str();
}

const char** mClockScheduler::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_mclock_scheduler_client_res",
    "osd_mclock_scheduler_client_wgt",
    "osd_mclock_scheduler_client_lim",
    "osd_mclock_scheduler_background_recovery_res",
    "osd_mclock_scheduler_background_recovery_wgt",
    "osd_mclock_scheduler_background_recovery_lim",
    "osd_mclock_scheduler_background_best_effort_res",
    "osd_mclock_scheduler_background_best_effort_wgt",
    "osd_mclock_scheduler_background_best_effort_lim",
    "osd_mclock_cost_per_io_usec",
    "osd_mclock_cost_per_io_usec_hdd",
    "osd_mclock_cost_per_io_usec_ssd",
    "osd_mclock_cost_per_byte_usec",
    "osd_mclock_cost_per_byte_usec_hdd",
    "osd_mclock_cost_per_byte_usec_ssd",
    "osd_mclock_max_capacity_iops_hdd",
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_profile",
    NULL
  };
  return KEYS;
}

void mClockScheduler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  if (changed.count("osd_mclock_cost_per_io_usec") ||
      changed.count("osd_mclock_cost_per_io_usec_hdd") ||
      changed.count("osd_mclock_cost_per_io_usec_ssd")) {
    set_osd_mclock_cost_per_io();
  }
  if (changed.count("osd_mclock_cost_per_byte_usec") ||
      changed.count("osd_mclock_cost_per_byte_usec_hdd") ||
      changed.count("osd_mclock_cost_per_byte_usec_ssd")) {
    set_osd_mclock_cost_per_byte();
  }
  if (changed.count("osd_mclock_max_capacity_iops_hdd") ||
      changed.count("osd_mclock_max_capacity_iops_ssd")) {
    set_max_osd_capacity();
    if (mclock_profile != "custom") {
      enable_mclock_profile_settings();
      client_registry.update_from_config(conf);
    }
  }
  if (changed.count("osd_mclock_profile")) {
    set_mclock_profile();
    if (mclock_profile != "custom") {
      enable_mclock_profile_settings();
      client_registry.update_from_config(conf);
    }
  }
  if (changed.count("osd_mclock_scheduler_client_res") ||
      changed.count("osd_mclock_scheduler_client_wgt") ||
      changed.count("osd_mclock_scheduler_client_lim") ||
      changed.count("osd_mclock_scheduler_background_recovery_res") ||
      changed.count("osd_mclock_scheduler_background_recovery_wgt") ||
      changed.count("osd_mclock_scheduler_background_recovery_lim") ||
      changed.count("osd_mclock_scheduler_background_best_effort_res") ||
      changed.count("osd_mclock_scheduler_background_best_effort_wgt") ||
      changed.count("osd_mclock_scheduler_background_best_effort_lim")) {
    if (mclock_profile == "custom") {
      client_registry.update_from_config(conf);
    }
  }
}

mClockScheduler::~mClockScheduler()
{
  cct->_conf.remove_observer(this);
}

}
