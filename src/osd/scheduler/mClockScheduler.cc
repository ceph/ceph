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
using namespace std::literals;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace ceph::osd::scheduler {

void mClockScheduler::_get_mclock_counter(scheduler_id_t id)
{
  if (!logger) {
    return;
  }

  /* op enter mclock queue will +1 */
  logger->inc(l_mclock_all_type_queue_len);

  switch (id.class_id) {
  case op_scheduler_class::immediate:
    logger->inc(l_mclock_immediate_queue_len);
    break;
  case op_scheduler_class::client:
    logger->inc(l_mclock_client_queue_len);
    break;
  case op_scheduler_class::background_recovery:
    logger->inc(l_mclock_recovery_queue_len);
    break;
  case op_scheduler_class::background_best_effort:
    logger->inc(l_mclock_best_effort_queue_len);
    break;
   default:
    derr << __func__ << " unknown class_id=" << id.class_id
         << " unknown id=" << id << dendl;
    break;
  }
}

void mClockScheduler::_put_mclock_counter(scheduler_id_t id)
{
  if (!logger) {
    return;
  }

  /* op leave mclock queue will -1 */
  logger->dec(l_mclock_all_type_queue_len);

  switch (id.class_id) {
  case op_scheduler_class::immediate:
    logger->dec(l_mclock_immediate_queue_len);
    break;
  case op_scheduler_class::client:
    logger->dec(l_mclock_client_queue_len);
    break;
  case op_scheduler_class::background_recovery:
    logger->dec(l_mclock_recovery_queue_len);
    break;
  case op_scheduler_class::background_best_effort:
    logger->dec(l_mclock_best_effort_queue_len);
    break;
   default:
    derr << __func__ << " unknown class_id=" << id.class_id
         << " unknown id=" << id << dendl;
    break;
  }
}

void mClockScheduler::_init_logger()
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
  std::lock_guard rl(reg_lock);
  auto ret = external_client_infos.find(client);
  if (ret == external_client_infos.end()) {
    return &default_external_client_info;
  } else {
    return &(ret->second);
  }
}

const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_info(
  const scheduler_id_t &id) const
{
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

void mClockScheduler::ClientRegistry::set_info(
  const ConfigProxy &conf,
  const bool is_rotational,
  const scheduler_id_t &id,
  const client_qos_params_t &qos_params,
  const double capacity_per_shard)
{
  std::lock_guard rl(reg_lock);
  if (op_scheduler_class::client == id.class_id) {
    auto osd_iops_capacity = [&]() {
      if (is_rotational) {
        return std::max<double>(1.0,
          conf.get_val<double>("osd_mclock_max_capacity_iops_hdd"));
      } else {
        return std::max<double>(1.0,
          conf.get_val<double>("osd_mclock_max_capacity_iops_ssd"));
      }
    };

    auto get_res = [&]() {
      if (qos_params.reservation == 0) {
        return default_min;
      }
      /**
       * Cap the maximum reservation requested by a client to the current
       * osd_mclock_scheduler_client_res specification. This is to prevent
       * clients from overprovisioning the OSD's bandwidth and deny
       * reservations requested by other services on the OSD.
       */
      double max_allowed_res = conf.get_val<double>(
        "osd_mclock_scheduler_client_res");
      double res_req = std::min<double>(
        max_allowed_res, qos_params.reservation / osd_iops_capacity());
      // Return reservation in terms of bytes/sec
      return res_req * capacity_per_shard;
    };

    auto get_lim = [&]() {
      if (qos_params.limit == 0) {
        return default_max;
      }
      double lim_req = std::min<double>(
        1.0, qos_params.limit / osd_iops_capacity());
      // Return limit in terms of bytes/sec
      return lim_req * capacity_per_shard;
    };

    double res;
    double lim;
    double wgt;
    bool client_info_changed = false;
    auto ci = external_client_infos_tracker.find(id.client_profile_id);
    if (ci == external_client_infos_tracker.end()) {
      res = get_res();
      lim = get_lim();
      wgt = double(qos_params.weight);
      client_info_changed = true;
    } else {
      enum { RES, WGT, LIM };
      // Return changed client info parameter
      auto get_ci_param = [&](int type, uint64_t val) -> std::optional<double> {
        switch (type) {
          case RES:
            if (val != qos_params.reservation) {
              return get_res();
            }
            break;
          case LIM:
            if (val != qos_params.limit) {
              return get_lim();
            }
            break;
          case WGT:
            if (val != qos_params.weight) {
              return double(qos_params.weight);
            }
            break;
          default:
            ceph_assert(0 == "Unknown client info type");
        }
        return std::nullopt;
      };

      auto _res = get_ci_param(RES, ci->second.res_iops);
      auto _lim = get_ci_param(LIM, ci->second.lim_iops);
      auto _wgt = get_ci_param(WGT, ci->second.wgt);
      client_info_changed =
        (_res.has_value() || _lim.has_value() || _wgt.has_value());

      res = _res.value_or(ci->second.res_bw);
      lim = _lim.value_or(ci->second.lim_bw);
      wgt = _wgt.value_or(ci->second.wgt);
    }

    // Add/update QoS profile params for the client in the client infos map
    if (client_info_changed) {
      auto r =
        external_client_infos.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(id.client_profile_id),
          std::forward_as_tuple(res, wgt, lim));
      if (!r.second) {
        r.first->second.update(res, wgt, lim);
      }
    }

    // Set/update tick version for the client in the client infos tracker map
    set_client_infos_tracker(id.client_profile_id, qos_params, res, lim, wgt);
  }
}

void mClockScheduler::ClientRegistry::clear_info(const scheduler_id_t &id)
{
  std::lock_guard rl(reg_lock);
  auto ret = external_client_infos.find(id.client_profile_id);
  if (ret != external_client_infos.end()) {
    external_client_infos.erase(ret);
  }
}

void mClockScheduler::ClientRegistry::set_client_infos_tracker(
  const client_profile_id_t &id,
  const client_qos_params_t &qos_params,
  const double res_bw,
  const double lim_bw,
  const double wgt)
{
  ceph_assert(ceph_mutex_is_locked(reg_lock));
  ++tick;
  auto r =
    external_client_infos_tracker.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(id),
      std::forward_as_tuple(
        tick,
        qos_params.reservation,
        qos_params.limit,
        res_bw,
        lim_bw,
        wgt));
  if (!r.second) {
    r.first->second.update(tick, qos_params.reservation, qos_params.limit,
      res_bw, lim_bw, wgt);
  }
}

/*
 * This is being called regularly by the clear_timer thread. Every
 * time it's called it notes the time and version counter (mark
 * point) in a deque. It also looks at the deque to find the most
 * recent mark point that is older than clear_age. It then walks
 * the map and delete all external client entries that were last
 * used before that mark point.
 */
void mClockScheduler::ClientRegistry::do_clean()
{
  ceph_assert(ceph_mutex_is_locked(reg_lock));
  TimePoint now = std::chrono::steady_clock::now();
  clear_mark_points.emplace_back(MarkPoint(now, tick));

  counter_t clear_point = 0;
  auto point = clear_mark_points.front();
  while (point.first <= now - ceph::make_timespan(clear_age)) {
    clear_point = point.second;
    clear_mark_points.pop_front();
    point = clear_mark_points.front();
  }

  counter_t cleared_num = 0;
  if (clear_point > 0) {
    for (auto i = external_client_infos_tracker.begin();
         i != external_client_infos_tracker.end();
         /* empty */) {
      auto i2 = i++;
      if (cleared_num < clear_max &&
          i2->second.tick_count <= clear_point) {
        // Clear the entry from both the external client infos
        // and external client infos tracker map.
        auto ci = external_client_infos.find(i2->first);
        if (ci != external_client_infos.end()) {
          external_client_infos.erase(ci);
        }
        external_client_infos_tracker.erase(i2);
        cleared_num++;
      }
    }
  }
  clear_timer->add_event_after(clear_period, new Clear_Registry(this));
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

    /**
     * If this is a client Op with a non-zero qos_profile_id, it indicates that
     * a valid QoS profile and therefore QoS based on the profile and request
     * params must be provided to the client. Therefore, an entry is either
     * created or updated in the external client registry.
     */
    const client_qos_params_t& qos_profile_params =
      item.get_qos_profile_params();
    dmc::ReqParams qos_req_params = item.get_qos_req_params();
    if (qos_profile_params.qos_profile_id != 0) {
      client_registry.set_info(cct->_conf, is_rotational, id,
        qos_profile_params, osd_bandwidth_capacity_per_shard);
      dout(20) << __func__ << " qos_profile_params: "
               << qos_profile_params << dendl;
      dout(20) << __func__ << " qos_req_params: "
               << qos_req_params << dendl;
    }
    scheduler.add_request(
      std::move(item),
      id,
      qos_req_params, // delta & rho = 0 if qos_profile_id == 0
      cost);
    _get_mclock_counter(id);
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

  scheduler_id_t id = scheduler_id_t {
    op_scheduler_class::immediate,
    client_profile_id_t()
  };
  _get_mclock_counter(id);
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

    scheduler_id_t id = scheduler_id_t {
      op_scheduler_class::immediate,
      client_profile_id_t()
    };
    _put_mclock_counter(id);
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
      _put_mclock_counter(retn.client);
      std::optional<OpRequestRef> _op = retn.request->maybe_get_op();
      if (_op.has_value()) {
        auto req = (*_op)->get_req();
        if (req->get_type() == CEPH_MSG_OSD_OP) {
          (*_op)->qos_phase = retn.phase; // 'reservation' or 'priority'
          (*_op)->qos_cost = retn.cost; // cost in terms of Bytes
          dout(20) << __func__ << " qos_phase: " << (*_op)->qos_phase
                   << " qos_cost: " << (*_op)->qos_cost << dendl;
        }
      }
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

std::vector<std::string> mClockScheduler::get_tracked_keys() const noexcept
{
  return {
    "osd_mclock_scheduler_client_res"s,
    "osd_mclock_scheduler_client_wgt"s,
    "osd_mclock_scheduler_client_lim"s,
    "osd_mclock_scheduler_background_recovery_res"s,
    "osd_mclock_scheduler_background_recovery_wgt"s,
    "osd_mclock_scheduler_background_recovery_lim"s,
    "osd_mclock_scheduler_background_best_effort_res"s,
    "osd_mclock_scheduler_background_best_effort_wgt"s,
    "osd_mclock_scheduler_background_best_effort_lim"s,
    "osd_mclock_max_capacity_iops_hdd"s,
    "osd_mclock_max_capacity_iops_ssd"s,
    "osd_mclock_max_sequential_bandwidth_hdd"s,
    "osd_mclock_max_sequential_bandwidth_ssd"s,
    "osd_mclock_profile"s
  };
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
  client_registry.shutdown_clear_timer();
  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = nullptr;
  }
}

}
