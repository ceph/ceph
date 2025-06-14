// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <memory>
#include <functional>

#include "mclock_common.h"
#include "debug.h"

#ifdef WITH_CRIMSON
#include "crimson/common/perf_counters_collection.h"
#else
#include "perf_counters_collection.h"
#endif

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mclock_common: "

namespace dmc = crimson::dmclock;

std::ostream &operator<<(std::ostream &lhs, const SchedulerClass &c)
{
  lhs << static_cast<size_t>(c);
  switch (c) {
  case SchedulerClass::background_best_effort:
    return lhs << "background_best_effort";
  case SchedulerClass::background_recovery:
    return lhs << "background_recovery";
  case SchedulerClass::client:
    return lhs << "client";
#ifdef WITH_CRIMSON
  case SchedulerClass::repop:
    return lhs << "repop";
#endif
  case SchedulerClass::immediate:
    return lhs << "immediate";
  default:
    return lhs;
  }
}

std::ostream& operator<<(std::ostream& out,
                         const client_profile_id_t& client_profile) {
    out << " client_id: " << client_profile.client_id
        << " profile_id: " << client_profile.profile_id;
    return out;
}

std::ostream& operator<<(std::ostream& out,
                         const scheduler_id_t& sched_id) {
    out << "{ class_id: " << sched_id.class_id
        << sched_id.client_profile_id;
    return out << " }";
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
void ClientRegistry::update_from_config(const ConfigProxy &conf,
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
    static_cast<size_t>(SchedulerClass::background_recovery)].update(
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
    static_cast<size_t>(SchedulerClass::background_best_effort)].update(
      get_res(res),
      wgt,
      get_lim(lim));
}

const dmc::ClientInfo *ClientRegistry::get_external_client(
  const client_profile_id_t &client) const
{
  auto ret = external_client_infos.find(client);
  if (ret == external_client_infos.end())
    return &default_external_client_info;
  else
    return &(ret->second);
}

const dmc::ClientInfo *ClientRegistry::get_info(
  const scheduler_id_t &id) const {
  switch (id.class_id) {
  case SchedulerClass::immediate:
    ceph_assert(0 == "Cannot schedule immediate");
    return (dmc::ClientInfo*)nullptr;
  case SchedulerClass::client:
    return get_external_client(id.client_profile_id);
  default:
    ceph_assert(static_cast<size_t>(id.class_id) < internal_client_infos.size());
    return &internal_client_infos[static_cast<size_t>(id.class_id)];
  }
}

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

void MclockConfig::set_config_defaults_from_profile()
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

void MclockConfig::set_osd_capacity_params_from_config()
{
  uint64_t osd_bandwidth_capacity;
  double osd_iop_capacity;

  std::tie(osd_bandwidth_capacity, osd_iop_capacity) = [&] {
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
  osd_bandwidth_capacity_per_shard =
    static_cast<double>(osd_bandwidth_capacity) /
    static_cast<double>(num_shards);
  dout(1) << __func__ << ": osd_bandwidth_cost_per_io: "
          << std::fixed << std::setprecision(2)
          << osd_bandwidth_cost_per_io << " bytes/io"
          << ", osd_bandwidth_capacity_per_shard "
          << osd_bandwidth_capacity_per_shard << " bytes/second"
          << dendl;
}

void MclockConfig::init_logger()
{
  PerfCountersBuilder m(cct, "mclock-shard-queue-" + std::to_string(shard_id),
                        l_mclock_first, l_mclock_last);

  m.add_u64_counter(l_mclock_immediate_queue_len, "mclock_immediate_queue_len",
                    "high_priority op count in mclock queue");
  m.add_u64_counter(l_mclock_client_queue_len, "mclock_client_queue_len",
                    "client type op count in mclock queue");
  m.add_u64_counter(l_mclock_recovery_queue_len, "mclock_recovery_queue_len",
                    "background_recovery type op count in mclock queue");
  m.add_u64_counter(l_mclock_best_effort_queue_len, "mclock_best_effort_queue_len",
                    "background_best_effort type op count in mclock queue");
  m.add_u64_counter(l_mclock_all_type_queue_len, "mclock_all_type_queue_len",
                    "all type op count in mclock queue");

  logger = m.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  logger->set(l_mclock_immediate_queue_len, 0);
  logger->set(l_mclock_client_queue_len, 0);
  logger->set(l_mclock_recovery_queue_len, 0);
  logger->set(l_mclock_best_effort_queue_len, 0);
  logger->set(l_mclock_all_type_queue_len, 0);
}

void MclockConfig::get_mclock_counter(scheduler_id_t id)
{
  if (!logger) {
    return;
  }

  /* op enter mclock queue will +1 */
  logger->inc(l_mclock_all_type_queue_len);

  switch (id.class_id) {
  case SchedulerClass::immediate:
    logger->inc(l_mclock_immediate_queue_len);
    break;
  case SchedulerClass::client:
    logger->inc(l_mclock_client_queue_len);
    break;
  case SchedulerClass::background_recovery:
    logger->inc(l_mclock_recovery_queue_len);
    break;
  case SchedulerClass::background_best_effort:
    logger->inc(l_mclock_best_effort_queue_len);
    break;
   default:
    derr << __func__ << " unknown class_id=" << id.class_id
         << " unknown id=" << id << dendl;
    break;
  }
}

void MclockConfig::put_mclock_counter(scheduler_id_t id)
{
  if (!logger) {
    return;
  }

  /* op leave mclock queue will -1 */
  logger->dec(l_mclock_all_type_queue_len);

  switch (id.class_id) {
  case SchedulerClass::immediate:
    logger->dec(l_mclock_immediate_queue_len);
    break;
  case SchedulerClass::client:
    logger->dec(l_mclock_client_queue_len);
    break;
  case SchedulerClass::background_recovery:
    logger->dec(l_mclock_recovery_queue_len);
    break;
  case SchedulerClass::background_best_effort:
    logger->dec(l_mclock_best_effort_queue_len);
    break;
   default:
    derr << __func__ << " unknown class_id=" << id.class_id
         << " unknown id=" << id << dendl;
    break;
  }
}

double MclockConfig::get_cost_per_io() const {
    return osd_bandwidth_cost_per_io;
}

double MclockConfig::get_capacity_per_shard() const {
    return  osd_bandwidth_capacity_per_shard;
}

uint32_t MclockConfig::calc_scaled_cost(int item_cost)
{
  auto cost = static_cast<uint32_t>(
    std::max<int>(
      1, // ensure cost is non-zero and positive
      item_cost));
  auto cost_per_io = static_cast<uint32_t>(osd_bandwidth_cost_per_io);

  return std::max<uint32_t>(cost, cost_per_io);
}

void MclockConfig::mclock_handle_conf_change(const ConfigProxy& conf,
                                             const std::set<std::string>
					     &changed)
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

MclockConfig::~MclockConfig()
{
  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = nullptr;
  }
}
