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
  int whoami,
  uint32_t num_shards,
  int shard_id,
  bool is_rotational,
  unsigned cutoff_priority,
  MonClient *monc)
  : cct(cct),
    whoami(whoami),
    num_shards(num_shards),
    shard_id(shard_id),
    is_rotational(is_rotational),
    cutoff_priority(cutoff_priority),
    monc(monc),
    scheduler(
      std::bind(&mClockScheduler::ClientRegistry::get_info,
                &client_registry,
                _1),
      dmc::AtLimit::Wait,
      cct->_conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
{
  cct->_conf.add_observer(this);
  ceph_assert(num_shards > 0);
  set_osd_capacity_params_from_config();
  set_config_defaults_from_profile();
  client_registry.update_from_config(
    cct->_conf, osd_bandwidth_capacity_per_shard);
}

/* ClientRegistry holds the dmclock::ClientInfo configuration parameters
 * (reservation (bytes/second), weight (unitless), limit (bytes/second))
 * for each IO class in the OSD (client, background_recovery,
 * background_best_effort).
 *
 * mclock expects limit and reservation to have units of <cost>/second
 * (bytes/second), but osd_mclock_scheduler_client_(lim|res) are provided
 * as ratios of the OSD's capacity.  We convert from the one to the other
 * using the capacity_per_shard parameter.
 *
 * Note, mclock profile information will already have been set as a default
 * for the osd_mclock_scheduler_client_* parameters prior to calling
 * update_from_config -- see set_config_defaults_from_profile().
 */
void mClockScheduler::ClientRegistry::update_from_config(
  const ConfigProxy &conf,
  const double capacity_per_shard)
{

  auto get_res = [&](double res) {
    if (res) {
      return res * capacity_per_shard;
    } else {
      return default_min; // min reservation
    }
  };

  auto get_lim = [&](double lim) {
    if (lim) {
      return lim * capacity_per_shard;
    } else {
      return default_max; // high limit
    }
  };

  // Set external client infos
  double res = conf.get_val<double>(
    "osd_mclock_scheduler_client_res");
  double lim = conf.get_val<double>(
    "osd_mclock_scheduler_client_lim");
  uint64_t wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_client_wgt");
  default_external_client_info.update(
    get_res(res),
    wgt,
    get_lim(lim));

  // Set background recovery client infos
  res = conf.get_val<double>(
    "osd_mclock_scheduler_background_recovery_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_background_recovery_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_background_recovery_wgt");
  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
      get_res(res),
      wgt,
      get_lim(lim));

  // Set background best effort client infos
  res = conf.get_val<double>(
    "osd_mclock_scheduler_background_best_effort_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_background_best_effort_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_background_best_effort_wgt");
  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
      get_res(res),
      wgt,
      get_lim(lim));
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

void mClockScheduler::set_osd_capacity_params_from_config()
{
  uint64_t osd_bandwidth_capacity;
  double osd_iop_capacity;

  std::tie(osd_bandwidth_capacity, osd_iop_capacity) = [&, this] {
    if (is_rotational) {
      return std::make_tuple(
        cct->_conf.get_val<Option::size_t>(
          "osd_mclock_max_sequential_bandwidth_hdd"),
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_hdd"));
    } else {
      return std::make_tuple(
        cct->_conf.get_val<Option::size_t>(
          "osd_mclock_max_sequential_bandwidth_ssd"),
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_ssd"));
    }
  }();

  osd_bandwidth_capacity = std::max<uint64_t>(1, osd_bandwidth_capacity);
  osd_iop_capacity = std::max<double>(1.0, osd_iop_capacity);

  osd_bandwidth_cost_per_io =
    static_cast<double>(osd_bandwidth_capacity) / osd_iop_capacity;
  osd_bandwidth_capacity_per_shard = static_cast<double>(osd_bandwidth_capacity)
    / static_cast<double>(num_shards);

  dout(1) << __func__ << ": osd_bandwidth_cost_per_io: "
          << std::fixed << std::setprecision(2)
          << osd_bandwidth_cost_per_io << " bytes/io"
          << ", osd_bandwidth_capacity_per_shard "
          << osd_bandwidth_capacity_per_shard << " bytes/second"
          << dendl;
}

/**
 * profile_t
 *
 * mclock profile -- 3 params for each of 3 client classes
 * 0 (min): specifies no minimum reservation
 * 0 (max): specifies no upper limit
 */
struct profile_t {
  struct client_config_t {
    double reservation;
    uint64_t weight;
    double limit;
  };
  client_config_t client;
  client_config_t background_recovery;
  client_config_t background_best_effort;
};

static std::ostream &operator<<(
  std::ostream &lhs, const profile_t::client_config_t &rhs)
{
  return lhs << "{res: " << rhs.reservation
             << ", wgt: " << rhs.weight
             << ", lim: " << rhs.limit
             << "}";
}

static std::ostream &operator<<(std::ostream &lhs, const profile_t &rhs)
{
  return lhs << "[client: " << rhs.client
             << ", background_recovery: " << rhs.background_recovery
             << ", background_best_effort: " << rhs.background_best_effort
             << "]";
}

void mClockScheduler::set_config_defaults_from_profile()
{
  // Let only a single osd shard (id:0) set the profile configs
  if (shard_id > 0) {
    return;
  }

  /**
   * high_client_ops
   *
   * Client Allocation:
   *   reservation: 60% | weight: 2 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 40% | weight: 1 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 70% |
   */
  static constexpr profile_t high_client_ops_profile{
    { .6, 2,  0 },
    { .4, 1,  0 },
    {  0, 1, .7 }
  };

  /**
   * high_recovery_ops
   *
   * Client Allocation:
   *   reservation: 30% | weight: 1 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 70% | weight: 2 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 0 (max) |
   */
  static constexpr profile_t high_recovery_ops_profile{
    { .3, 1, 0 },
    { .7, 2, 0 },
    {  0, 1, 0 }
  };

  /**
   * balanced
   *
   * Client Allocation:
   *   reservation: 50% | weight: 1 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 50% | weight: 1 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 90% |
   */
  static constexpr profile_t balanced_profile{
    { .5, 1, 0 },
    { .5, 1, 0 },
    {  0, 1, .9 }
  };

  const profile_t *profile = nullptr;
  auto mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
  if (mclock_profile == "high_client_ops") {
    profile = &high_client_ops_profile;
    dout(10) << "Setting high_client_ops profile " << *profile << dendl;
  } else if (mclock_profile == "high_recovery_ops") {
    profile = &high_recovery_ops_profile;
    dout(10) << "Setting high_recovery_ops profile " << *profile << dendl;
  } else if (mclock_profile == "balanced") {
    profile = &balanced_profile;
    dout(10) << "Setting balanced profile " << *profile << dendl;
  } else if (mclock_profile == "custom") {
    dout(10) << "Profile set to custom, not setting defaults" << dendl;
    return;
  } else {
    derr << "Invalid mclock profile: " << mclock_profile << dendl;
    ceph_assert("Invalid choice of mclock profile" == 0);
    return;
  }
  ceph_assert(nullptr != profile);

  auto set_config = [&conf = cct->_conf](const char *key, auto val) {
    conf.set_val_default(key, std::to_string(val));
  };

  set_config("osd_mclock_scheduler_client_res", profile->client.reservation);
  set_config("osd_mclock_scheduler_client_wgt", profile->client.weight);
  set_config("osd_mclock_scheduler_client_lim", profile->client.limit);

  set_config(
    "osd_mclock_scheduler_background_recovery_res",
    profile->background_recovery.reservation);
  set_config(
    "osd_mclock_scheduler_background_recovery_wgt",
    profile->background_recovery.weight);
  set_config(
    "osd_mclock_scheduler_background_recovery_lim",
    profile->background_recovery.limit);

  set_config(
    "osd_mclock_scheduler_background_best_effort_res",
    profile->background_best_effort.reservation);
  set_config(
    "osd_mclock_scheduler_background_best_effort_wgt",
    profile->background_best_effort.weight);
  set_config(
    "osd_mclock_scheduler_background_best_effort_lim",
    profile->background_best_effort.limit);

  cct->_conf.apply_changes(nullptr);
}

uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  auto cost = static_cast<uint32_t>(
    std::max<int>(
      1, // ensure cost is non-zero and positive
      item_cost));
  auto cost_per_io = static_cast<uint32_t>(osd_bandwidth_cost_per_io);

  return std::max<uint32_t>(cost, cost_per_io);
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
  f.dump_int("high_priority_queue", high_priority.size());
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

  f.open_object_section("HighPriorityQueue");
  for (auto it = high_priority.begin();
       it != high_priority.end(); it++) {
    f.dump_int("priority", it->first);
    f.dump_int("queue_size", it->second.size());
  }
  f.close_section();
}

void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  auto id = get_scheduler_id(item);
  unsigned priority = item.get_priority();
  
  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (op_scheduler_class::immediate == id.class_id) {
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item));
  } else {
    auto cost = calc_scaled_cost(item.get_cost());
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
          << " queue_sizes: [ "
	  << " high_priority_queue: " << high_priority.size()
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
  unsigned priority = item.get_priority();
  auto id = get_scheduler_id(item);

  if (op_scheduler_class::immediate == id.class_id) {
    enqueue_high(immediate_class_priority, std::move(item), true);
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item), true);
  } else {
    // mClock does not support enqueue at front, so we use
    // the high queue with priority 0
    enqueue_high(0, std::move(item), true);
  }
}

void mClockScheduler::enqueue_high(unsigned priority,
                                   OpSchedulerItem&& item,
				   bool front)
{
  if (front) {
    high_priority[priority].push_back(std::move(item));
  } else {
    high_priority[priority].push_front(std::move(item));
  }
}

WorkItem mClockScheduler::dequeue()
{
  if (!high_priority.empty()) {
    auto iter = high_priority.begin();
    // invariant: high_priority entries are never empty
    assert(!iter->second.empty());
    WorkItem ret{std::move(iter->second.back())};
    iter->second.pop_back();
    if (iter->second.empty()) {
      // maintain invariant, high priority entries are never empty
      high_priority.erase(iter);
    }
    ceph_assert(std::get_if<OpSchedulerItem>(&ret));
    return ret;
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
    "osd_mclock_max_capacity_iops_hdd",
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_max_sequential_bandwidth_hdd",
    "osd_mclock_max_sequential_bandwidth_ssd",
    "osd_mclock_profile",
    NULL
  };
  return KEYS;
}

void mClockScheduler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  if (changed.count("osd_mclock_max_capacity_iops_hdd") ||
      changed.count("osd_mclock_max_capacity_iops_ssd")) {
    set_osd_capacity_params_from_config();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard);
  }
  if (changed.count("osd_mclock_max_sequential_bandwidth_hdd") ||
      changed.count("osd_mclock_max_sequential_bandwidth_ssd")) {
    set_osd_capacity_params_from_config();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard);
  }
  if (changed.count("osd_mclock_profile")) {
    set_config_defaults_from_profile();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard);
  }

  auto get_changed_key = [&changed]() -> std::optional<std::string> {
    static const std::vector<std::string> qos_params = {
      "osd_mclock_scheduler_client_res",
      "osd_mclock_scheduler_client_wgt",
      "osd_mclock_scheduler_client_lim",
      "osd_mclock_scheduler_background_recovery_res",
      "osd_mclock_scheduler_background_recovery_wgt",
      "osd_mclock_scheduler_background_recovery_lim",
      "osd_mclock_scheduler_background_best_effort_res",
      "osd_mclock_scheduler_background_best_effort_wgt",
      "osd_mclock_scheduler_background_best_effort_lim"
    };

    for (auto &qp : qos_params) {
      if (changed.count(qp)) {
        return qp;
      }
    }
    return std::nullopt;
  };

  if (auto key = get_changed_key(); key.has_value()) {
    auto mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
    if (mclock_profile == "custom") {
      client_registry.update_from_config(
        conf, osd_bandwidth_capacity_per_shard);
    } else {
      // Attempt to change QoS parameter for a built-in profile. Restore the
      // profile defaults by making one of the OSD shards remove the key from
      // config monitor store. Note: monc is included in the check since the
      // mock unit test currently doesn't initialize it.
      if (shard_id == 0 && monc) {
        static const std::vector<std::string> osds = {
          "osd",
          "osd." + std::to_string(whoami)
        };

        for (auto osd : osds) {
          std::string cmd =
            "{"
              "\"prefix\": \"config rm\", "
              "\"who\": \"" + osd + "\", "
              "\"name\": \"" + *key + "\""
            "}";
          std::vector<std::string> vcmd{cmd};

          dout(10) << __func__ << " Removing Key: " << *key
                   << " for " << osd << " from Mon db" << dendl;
          monc->start_mon_command(vcmd, {}, nullptr, nullptr, nullptr);
        }
      }
    }
    // Alternatively, the QoS parameter, if set ephemerally for this OSD via
    // the 'daemon' or 'tell' interfaces must be removed.
    if (!cct->_conf.rm_val(*key)) {
      dout(10) << __func__ << " Restored " << *key << " to default" << dendl;
      cct->_conf.apply_changes(nullptr);
    }
  }
}

mClockScheduler::~mClockScheduler()
{
  cct->_conf.remove_observer(this);
}

}
