// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

scheduler_op_type_t
mClockScheduler::get_scheduler_op_type(const OpSchedulerItem &item)
{
  if (item.is_peering()) {
    return scheduler_op_type_t::peering_op;
  }

  const auto op = item.maybe_get_op();
  if (!op.has_value() || !(*op) || !(*op)->get_req()) {
    return scheduler_op_type_t::unknown;
  }

  scheduler_op_type_t sch_op_type = scheduler_op_type_t::unknown;
  const auto op_type = (*op)->get_req()->get_type();
  const auto id = get_scheduler_id(item);
  switch (id.class_id) {
  case SchedulerClass::immediate: {
    if (op_type == MSG_OSD_EC_READ) {
      sch_op_type = scheduler_op_type_t::ec_read_op;
    } else if (op_type == MSG_OSD_EC_WRITE) {
      sch_op_type = scheduler_op_type_t::ec_write_op;
    }
    break;
  }
  case SchedulerClass::background_best_effort: {
    if (op_type == MSG_OSD_EC_READ) {
      sch_op_type = scheduler_op_type_t::ec_rec_read_op;
    }
    break;
  }
  default:
    break;
  }

  return sch_op_type;
}

void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  auto id = get_scheduler_id(item);
  unsigned priority = item.get_priority();
  scheduler_op_type_t sch_op_type = get_scheduler_op_type(item);
  auto item_cost = item.get_cost();

  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (SchedulerClass::immediate == id.class_id) {
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item));
  } else {
    auto qos_cost = calc_scaled_cost(item_cost);
    item.set_qos_cost(qos_cost);
    dout(20) << __func__ << " " << id
             << " item_cost: " << item_cost
             << " scaled_cost: " << qos_cost
             << dendl;

    // trigger perf counter calculations first
    mclock_conf.get_mclock_counter(id, sch_op_type, item_cost);

    // Add item to scheduler queue
    scheduler.add_request(
      std::move(item),
      id,
      qos_cost);
  }

  dout(20) << __func__ << ": sched client_count: " << scheduler.client_count()
           << " sched queue size: " << scheduler.request_count()
           << dendl;

  auto fmt_prio = [this](priority_t p) -> std::string {
    return (p == immediate_class_priority) ? "MAX" : std::to_string(p);
  };

  dout(20) << __func__ << " high_priority queues: " << high_priority.size();
  for (const auto& [prio, queue] : high_priority) {
    *_dout << ", priority " << fmt_prio(prio) << ": " << queue.size();
  }
  *_dout << dendl;

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
  scheduler_op_type_t sch_op_type = get_scheduler_op_type(item);
  scheduler_id_t id = scheduler_id_t {
    SchedulerClass::immediate,
    client_profile_id_t()
  };

  // trigger perf counter calculations first
  mclock_conf.get_mclock_counter(id, sch_op_type, item.get_cost());

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
    ceph_assert(!iter->second.empty());
    WorkItem ret{std::move(iter->second.back())};
    iter->second.pop_back();
    if (iter->second.empty()) {
      // maintain invariant, high priority entries are never empty
      high_priority.erase(iter);
    }
    auto *item_ptr = std::get_if<OpSchedulerItem>(&ret);
    ceph_assert(item_ptr);

    scheduler_id_t id = scheduler_id_t {
      SchedulerClass::immediate,
      client_profile_id_t()
    };
    scheduler_op_type_t op_type = get_scheduler_op_type(*item_ptr);
    mclock_conf.put_mclock_counter(id, op_type, item_ptr->get_time_queued());
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

      // update perf counters
      scheduler_op_type_t op_type = scheduler_op_type_t::unknown;
      utime_t time_queued = utime_t();
      if (retn.request) {
        op_type = get_scheduler_op_type(*retn.request);
        time_queued = retn.request->get_time_queued();
      }
      mclock_conf.put_mclock_counter(retn.client, op_type, time_queued);

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

}
