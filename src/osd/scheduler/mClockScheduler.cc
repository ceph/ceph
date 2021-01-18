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

#include "include/stringify.h"
#include "osd/scheduler/mClockScheduler.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
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
  // Set default blocksize and cost for all op types.
  for (op_type_t op_type = op_type_t::client_op;
       op_type <= op_type_t::bg_pg_delete;
       op_type = op_type_t(static_cast<size_t>(op_type) + 1)) {
    client_cost_infos[op_type] = 4 * 1024;
    client_scaled_cost_infos[op_type] = 1;
  }
  set_max_osd_capacity();
  set_osd_mclock_cost_per_io();
  mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
  set_client_allocations();
  enable_mclock_profile();
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
  if (cct->_conf.get_val<double>("osd_mclock_max_capacity_iops")) {
    max_osd_capacity =
      cct->_conf.get_val<double>("osd_mclock_max_capacity_iops");
  } else {
    if (is_rotational) {
      max_osd_capacity =
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_hdd");
    } else {
      max_osd_capacity =
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_ssd");
    }
  }
  // Set per op-shard iops limit
  max_osd_capacity /= num_shards;
}

void mClockScheduler::set_osd_mclock_cost_per_io()
{
  if (cct->_conf.get_val<uint64_t>("osd_mclock_cost_per_io_msec")) {
    osd_mclock_cost_per_io_msec =
      cct->_conf.get_val<uint64_t>("osd_mclock_cost_per_io_msec");
  } else {
    if (is_rotational) {
      osd_mclock_cost_per_io_msec =
        cct->_conf.get_val<uint64_t>("osd_mclock_cost_per_io_msec_hdd");
    } else {
      osd_mclock_cost_per_io_msec =
        cct->_conf.get_val<uint64_t>("osd_mclock_cost_per_io_msec_ssd");
    }
  }
}

void mClockScheduler::set_client_allocations()
{
  // Set profile specific client capacity allocations
  if (mclock_profile == "balanced") {
    double capacity = std::round(0.5 * max_osd_capacity);
    client_allocs[op_scheduler_class::client] = capacity;
    client_allocs[op_scheduler_class::background_recovery] = capacity;
  } else if (mclock_profile == "high_recovery_ops") {
    client_allocs[op_scheduler_class::client] =
      std::round(0.25 * max_osd_capacity);
    client_allocs[op_scheduler_class::background_recovery] =
      std::round(0.75 * max_osd_capacity);
  } else if (mclock_profile == "high_client_ops") {
    client_allocs[op_scheduler_class::client] =
      std::round(0.75 * max_osd_capacity);
    client_allocs[op_scheduler_class::background_recovery] =
      std::round(0.25 * max_osd_capacity);
  } else {
    ceph_assert("Invalid mclock profile" == 0);
    return;
  }
}

double mClockScheduler::get_client_allocation(op_type_t op_type)
{
  double default_allocation = 1.0;

  switch (op_type) {
  case op_type_t::client_op:
    return client_allocs[op_scheduler_class::client];
  case op_type_t::bg_recovery:
    return client_allocs[op_scheduler_class::background_recovery];
  default:
    // TODO for other op types.
    return default_allocation;
  }
}

void mClockScheduler::enable_mclock_profile()
{
  // Nothing to do for "custom" profile
  if (mclock_profile == "custom") {
    return;
  }

  // Set mclock and ceph config options for the chosen profile
  if (mclock_profile == "balanced") {
    set_balanced_profile_config();
  } else if (mclock_profile == "high_recovery_ops") {
    set_high_recovery_ops_profile_config();
  } else if (mclock_profile == "high_client_ops") {
    set_high_client_ops_profile_config();
  } else {
    ceph_assert("Invalid choice of mclock profile" == 0);
    return;
  }

  // Set recovery specific Ceph options
  set_global_recovery_options();
}

std::string mClockScheduler::get_mclock_profile()
{
  return mclock_profile;
}

void mClockScheduler::set_balanced_profile_config()
{
  double client_lim = get_client_allocation(op_type_t::client_op);
  double rec_lim = get_client_allocation(op_type_t::bg_recovery);
  int client_wgt = 10;

  // Set external client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_wgt", stringify(client_wgt));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_lim", stringify(client_lim));

  // Set background recovery client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_wgt", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_lim", stringify(rec_lim));
}

void mClockScheduler::set_high_recovery_ops_profile_config()
{
  double client_lim = get_client_allocation(op_type_t::client_op);
  double rec_lim = get_client_allocation(op_type_t::bg_recovery);
  int rec_wgt = 10;

  // Set external client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_wgt", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_lim", stringify(client_lim));

  // Set background recovery client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_wgt", stringify(rec_wgt));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_lim", stringify(rec_lim));
}

void mClockScheduler::set_high_client_ops_profile_config()
{
  double client_lim = get_client_allocation(op_type_t::client_op);
  double rec_lim = get_client_allocation(op_type_t::bg_recovery);
  int client_wgt = 10;

  // Set external client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_wgt", stringify(client_wgt));
  cct->_conf.set_val(
    "osd_mclock_scheduler_client_lim", stringify(client_lim));

  // Set background recovery client params
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_res", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_wgt", stringify(default_min));
  cct->_conf.set_val(
    "osd_mclock_scheduler_background_recovery_lim", stringify(rec_lim));
}

void mClockScheduler::set_global_recovery_options()
{
  // Set high value for recovery max active and max backfill
  int rec_max_active = 1000;
  int max_backfills = 1000;
  cct->_conf.set_val("osd_recovery_max_active", stringify(rec_max_active));
  cct->_conf.set_val("osd_max_backfills", stringify(max_backfills));

  // Disable recovery sleep
  cct->_conf.set_val("osd_recovery_sleep", stringify(0));
  cct->_conf.set_val("osd_recovery_sleep_hdd", stringify(0));
  cct->_conf.set_val("osd_recovery_sleep_ssd", stringify(0));
  cct->_conf.set_val("osd_recovery_sleep_hybrid", stringify(0));

  // Apply the changes
  cct->_conf.apply_changes(nullptr);
}

int mClockScheduler::calc_scaled_cost(op_type_t op_type, int cost)
{
  double client_alloc = get_client_allocation(op_type);
  if (client_alloc == 1.0) {
    // Client not yet supported, return default cost.
    return 1;
  }

  // Calculate bandwidth from max osd capacity (at 4KiB blocksize).
  double max_osd_bandwidth = max_osd_capacity * num_shards * 4 * 1024;

  // Calculate scaled cost based on item cost
  double scaled_cost = (cost / max_osd_bandwidth) * client_alloc;

  // Scale the cost down by an additional cost factor if specified
  // to account for different device characteristics (hdd, ssd).
  // This option can be used to further tune the performance further
  // if necessary (disabled by default).
  if (osd_mclock_cost_per_io_msec > 0) {
    scaled_cost *= osd_mclock_cost_per_io_msec / 1000.0;
  }

  return std::floor(scaled_cost);
}

bool mClockScheduler::maybe_update_client_cost_info(
  op_type_t op_type, int new_cost)
{
  int capped_item_cost = 4 * 1024 * 1024;

  if (new_cost == 0) {
    return false;
  }

  // The mclock params represented in terms of the per-osd capacity
  // are scaled up or down according to the cost associated with
  // item cost and updated within the dmclock server.
  int cur_cost = client_cost_infos[op_type];

  // Note: Cap the scaling of item cost to ~4MiB as the tag increments
  // beyond this point are too long causing performance issues. This may
  // need to be in place until benchmark data is available or a better
  // scaling model can be put in place. This is a TODO.
  if (new_cost >= capped_item_cost) {
    new_cost = capped_item_cost;
  }

  bool cost_changed =
    ((new_cost >= (cur_cost << 1)) || (cur_cost >= (new_cost << 1)));

  if (cost_changed) {
    client_cost_infos[op_type] = new_cost;
    // Update client scaled cost info
    int scaled_cost = std::max(calc_scaled_cost(op_type, new_cost), 1);
    if (scaled_cost != client_scaled_cost_infos[op_type]) {
      client_scaled_cost_infos[op_type] = scaled_cost;
      return true;
    }
  }

  return false;
}

void mClockScheduler::dump(ceph::Formatter &f) const
{
}

void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  auto id = get_scheduler_id(item);
  auto op_type = item.get_op_type();
  int cost = client_scaled_cost_infos[op_type];

  // Re-calculate the scaled cost for the client if the item cost changed
  if (maybe_update_client_cost_info(op_type, item.get_cost())) {
    cost = client_scaled_cost_infos[op_type];
  }

  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (op_scheduler_class::immediate == item.get_scheduler_class()) {
    immediate.push_front(std::move(item));
  } else {
    scheduler.add_request(
      std::move(item),
      id,
      cost);
  }
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
    "osd_mclock_cost_per_io_msec",
    "osd_mclock_cost_per_io_msec_hdd",
    "osd_mclock_cost_per_io_msec_ssd",
    "osd_mclock_max_capacity_iops",
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
  if (changed.count("osd_mclock_cost_per_io_msec") ||
      changed.count("osd_mclock_cost_per_io_msec_hdd") ||
      changed.count("osd_mclock_cost_per_io_msec_ssd")) {
    set_osd_mclock_cost_per_io();
  }
  if (changed.count("osd_mclock_max_capacity_iops") ||
      changed.count("osd_mclock_max_capacity_iops_hdd") ||
      changed.count("osd_mclock_max_capacity_iops_ssd")) {
    set_max_osd_capacity();
    enable_mclock_profile();
    client_registry.update_from_config(conf);
  }
  if (changed.count("osd_mclock_profile")) {
    enable_mclock_profile();
    if (mclock_profile != "custom") {
      client_registry.update_from_config(conf);
    }
  }
  if (changed.count("osd_mclock_scheduler_client_res") ||
      changed.count("osd_mclock_scheduler_client_wgt") ||
      changed.count("osd_mclock_scheduler_client_lim") ||
      changed.count("osd_mclock_scheduler_background_recovery_res") ||
      changed.count("osd_mclock_scheduler_background_recovery_wgt") ||
      changed.count("osd_mclock_scheduler_background_recovery_lim")) {
    if (mclock_profile == "custom") {
      client_registry.update_from_config(conf);
    }
  }
}

}
