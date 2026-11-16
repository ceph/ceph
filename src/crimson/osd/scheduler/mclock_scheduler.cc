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

#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "common/debug.h"
#include "common/dout.h"

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace crimson::osd::scheduler {

uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  return mclock_conf.calc_scaled_cost(item_cost);
}

void mClockScheduler::dump(ceph::Formatter &f) const
{
  // Display queue sizes
  f.open_object_section("queue_sizes");
  f.dump_int("high_priority_queue", high_priority.size());
  size_t pending = 0;
  for (const auto &[id, queue] : waiting) {
    pending += queue.items.size();
  }
  f.dump_int("scheduler", pending);
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

void mClockScheduler::enqueue(item_t &&item)
{
  const auto priority = item.get_priority();
  if (item.params.klass == SchedulerClass::immediate) {
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item));
  } else {
    waiting[get_scheduler_id(item)].items.push(std::move(item));
  }
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

void mClockScheduler::enqueue_high(unsigned priority, item_t &&item, bool front)
{
  high_priority[priority].push(std::move(item), front);
}

void mClockScheduler::mclock_queue_t::set_candidate(
  const scheduler_id_t &id, LevelQueue::Entry *entry, uint32_t cost)
{
  if (auto previous = unserved.find(id); previous != unserved.end()) {
    ceph_assert(client_map.at(id)->request_count() == 1);
    remove_by_client(id);
    auto &client = *client_map.at(id);
    client.update_req_tag(previous->second, client.last_tick);
    unserved.erase(previous);
  }
  if (entry) {
    auto client = client_map.find(id);
    unserved.emplace(id, client == client_map.end()
      ? dmc::RequestTag{0, 0, 0, dmc::TimeZero}
      : client->second->get_req_tag());
    add_request_time(std::move(entry), id, dmc::ReqParams{}, entry->arrival, cost);
  }
}

mClockScheduler::mclock_queue_t::PullReq
mClockScheduler::mclock_queue_t::pull_candidate(double now)
{
  auto result = pull_request(now);
  if (result.is_retn()) {
    unserved.erase(result.get_retn().client);
  }
  return result;
}

WorkItem mClockScheduler::dequeue(throttle_level_t max_level, double now)
{
  for (auto it = high_priority.begin(); it != high_priority.end(); ++it) {
    if (auto *candidate = it->second.front(max_level)) {
      auto item = it->second.pop(candidate);
      if (it->second.empty()) {
        high_priority.erase(it);
      }
      return item;
    }
  }

  // All levels of a QoS class share its R/W/L tags. Expose its oldest
  // eligible head, retaining the original arrival time when it changes.
  // Remove all displaced heads first, so newly active classes cannot
  // inherit their unserved tags through dmClock's idle-client adjustment.
  for (auto &[id, pending] : waiting) {
    if (pending.candidate != pending.items.front(max_level)) {
      scheduler.set_candidate(id, nullptr, 0);
      pending.candidate = nullptr;
    }
  }
  for (auto &[id, pending] : waiting) {
    if (auto *candidate = pending.items.front(max_level);
        candidate && !pending.candidate) {
      scheduler.set_candidate(id, candidate,
        calc_scaled_cost(candidate->item.get_cost()));
      pending.candidate = candidate;
    }
  }
  auto result = scheduler.pull_candidate(now);
  if (result.is_future()) {
    return result.getTime();
  }
  if (result.is_none()) {
    return std::monostate{};
  }
  auto &retn = result.get_retn();
  auto it = waiting.find(retn.client);
  ceph_assert(it != waiting.end());
  auto item = it->second.items.pop(*retn.request);
  it->second.candidate = nullptr;
  if (it->second.items.empty()) {
    waiting.erase(it);
  }
  return item;
}

void mClockScheduler::abort(std::exception_ptr error)
{
  for (auto &[id, pending] : waiting) {
    if (pending.candidate) {
      scheduler.set_candidate(id, nullptr, 0);
    }
    pending.items.abort(error);
  }
  waiting.clear();
  for (auto &[priority, queue] : high_priority) {
    queue.abort(error);
  }
  high_priority.clear();
}

std::string mClockScheduler::display_queues() const
{
  std::ostringstream out;
  scheduler.display_queues(out);
  return out.str();
}


std::vector<std::string> mClockScheduler::get_tracked_keys() const noexcept
{
  return mclock_conf.get_tracked_keys();
}

void mClockScheduler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  mclock_conf.handle_conf_change(conf, changed);
}

mClockScheduler::~mClockScheduler()
{
  cct->_conf.remove_observer(this);
}

}
