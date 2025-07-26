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

#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;
using namespace std::literals;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace crimson::osd::scheduler {

mClockScheduler::mClockScheduler(CephContext *cct,
  int whoami,
  uint32_t num_shards,
  int shard_id,
  bool is_rotational,
  bool init_perfcounter)
  : cct(cct),
    logger(nullptr),
    mclock_conf(cct, client_registry, num_shards, is_rotational, shard_id, whoami),
    scheduler(
      std::bind(&ClientRegistry::get_info,
                &client_registry,
                _1),
      dmc::AtLimit::Wait,
      cct->_conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
{
  cct->_conf.add_observer(this);
  ceph_assert(num_shards > 0);
  auto get_op_queue_cut_off = [&conf = cct->_conf]() {
    if (conf.get_val<std::string>("osd_op_queue_cut_off") == "debug_random") {
      std::random_device rd;
      std::mt19937 random_gen(rd());
      return (random_gen() % 2 < 1) ? CEPH_MSG_PRIO_HIGH : CEPH_MSG_PRIO_LOW;
    } else if (conf.get_val<std::string>("osd_op_queue_cut_off") == "high") {
      return CEPH_MSG_PRIO_HIGH;
    } else {
      // default / catch-all is 'low'
      return CEPH_MSG_PRIO_LOW;
    }
  };
  cutoff_priority = get_op_queue_cut_off();
  mclock_conf.set_osd_capacity_params_from_config();
  mclock_conf.set_config_defaults_from_profile();
  client_registry.update_from_config(
      cct->_conf, mclock_conf.get_capacity_per_shard());

}

uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  return mclock_conf.calc_scaled_cost(item_cost);
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
  //out << scheduler;
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

void mClockScheduler::enqueue(item_t&& item)
{
  auto id = get_scheduler_id(item);
  unsigned priority = item.get_priority();

  // TODO: move this check into item, handle backwards compat
  if (SchedulerClass::immediate == item.params.klass) {
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item));
  } else {
    auto cost = calc_scaled_cost(item.get_cost());
    dout(20) << __func__ << " " << id
             << " item_cost: " << item.get_cost()
             << " scaled_cost: " << cost
             << dendl;

    // Add item to scheduler queue
    scheduler.add_request(
      std::move(item),
      id,
      cost);
    //_get_mclock_counter(id);
  }

 dout(0) << __func__ << " client_count: " << scheduler.client_count()
          << " queue_sizes: [ "
	  << " high_priority_queue: " << high_priority.size()
          << " sched: " << scheduler.request_count() << " ]"
          << dendl;
 dout(0) << __func__ << " mClockClients: "
          << dendl;
 dout(10) << __func__ << " mClockQueues: { "
          << display_queues() << " }"
          << dendl;
}

void mClockScheduler::enqueue_front(item_t&& item)
{
  unsigned priority = item.get_priority();

  if (SchedulerClass::immediate == item.params.klass) {
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
                                   item_t&& item,
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
     dout(0) << __func__ << "MY_LOG Return high priority" << dendl;
    return ret;
  } else {
    mclock_queue_t::PullReq result = scheduler.pull_request();
    if (result.is_future()) {
       dout(0) << __func__ << "MY_LOG Return time" << dendl;
      return result.getTime();
    } else if (result.is_none()) {
      ceph_abort_msg(
	"Impossible, must have checked empty() first");
      return std::move(*(item_t*)nullptr);
    } else {
      ceph_assert(result.is_retn());

      auto &retn = result.get_retn();
       dout(0) << __func__ << "MY_LOG Return item" << dendl;
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
    "osd_mclock_profile"
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
