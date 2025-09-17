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
void ClientRegistry::update_from_profile(const profile_t &current_profile,
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

  default_external_client_info.update(
    get_res(current_profile.client.reservation),
    current_profile.client.weight,
    get_lim(current_profile.client.limit));

  internal_client_infos[
    static_cast<size_t>(SchedulerClass::background_recovery)].update(
      get_res(current_profile.background_recovery.reservation),
      current_profile.background_recovery.weight,
      get_lim(current_profile.background_recovery.limit));

  internal_client_infos[
    static_cast<size_t>(SchedulerClass::background_best_effort)].update(
      get_res(current_profile.background_best_effort.reservation),
      current_profile.background_best_effort.weight,
      get_lim(current_profile.background_best_effort.limit));
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

void MclockConfig::set_from_config()
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

  auto mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
  if (mclock_profile == "high_client_ops") {
    current_profile = HIGH_CLIENT_OPS;
    dout(10) << "Setting high_client_ops profile " << current_profile << dendl;
  } else if (mclock_profile == "high_recovery_ops") {
    current_profile = HIGH_RECOVERY_OPS;
    dout(10) << "Setting high_recovery_ops profile " << current_profile << dendl;
  } else if (mclock_profile == "balanced") {
    current_profile = BALANCED;
    dout(10) << "Setting balanced profile " << current_profile << dendl;
  } else if (mclock_profile == "custom") {
    current_profile = {
      {
	cct->_conf.get_val<double>("osd_mclock_scheduler_client_res"),
	cct->_conf.get_val<uint64_t>("osd_mclock_scheduler_client_wgt"),
	cct->_conf.get_val<double>("osd_mclock_scheduler_client_lim")
      }, {
	cct->_conf.get_val<double>(
	  "osd_mclock_scheduler_background_recovery_res"),
	cct->_conf.get_val<uint64_t>(
	  "osd_mclock_scheduler_background_recovery_wgt"),
	cct->_conf.get_val<double>(
	  "osd_mclock_scheduler_background_recovery_lim")
      }, {
	cct->_conf.get_val<double>(
	  "osd_mclock_scheduler_background_best_effort_res"),
	cct->_conf.get_val<uint64_t>(
	  "osd_mclock_scheduler_background_best_effort_wgt"),
	cct->_conf.get_val<double>(
	  "osd_mclock_scheduler_background_best_effort_lim")
      }
    };
    dout(10) << "Setting custom profile " << current_profile << dendl;
  } else {
    derr << "Invalid mclock profile: " << mclock_profile << dendl;
    ceph_assert("Invalid choice of mclock profile" == 0);
    return;
  }
  client_registry.update_from_profile(
    current_profile, osd_bandwidth_capacity_per_shard);
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

void MclockConfig::handle_conf_change(const ConfigProxy& conf,
				      const std::set<std::string> &changed)
{
  for (auto &key : get_tracked_keys()) {
    if (changed.count(key)) {
      set_from_config();
      return;
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
  cct->_conf.remove_observer(this);
}
