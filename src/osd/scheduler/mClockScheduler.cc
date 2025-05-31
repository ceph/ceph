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
#include "common/debug.h"

#ifdef WITH_CRIMSON
#include "crimson/common/perf_counters_collection.h"
#else
#include "common/perf_counters_collection.h"
#endif

namespace dmc = crimson::dmclock;
using namespace std::placeholders;
using namespace std::literals;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace ceph::osd::scheduler {

uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  return mclock_conf.calc_scaled_cost(item_cost);
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
  if (SchedulerClass::immediate == id.class_id) {
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
    mclock_conf.get_mclock_counter(id);
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

  if (SchedulerClass::immediate == id.class_id) {
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
    SchedulerClass::immediate,
    client_profile_id_t()
  };
  mclock_conf.get_mclock_counter(id);
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
      SchedulerClass::immediate,
      client_profile_id_t()
    };
    mclock_conf.put_mclock_counter(id);
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
      mclock_conf.put_mclock_counter(retn.client);
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
  mclock_conf.mclock_handle_conf_change(conf, changed);
}

mClockScheduler::~mClockScheduler()
{
  cct->_conf.remove_observer(this);
}

}
