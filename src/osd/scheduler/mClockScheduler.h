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


#pragma once

#include <functional>
#include <ostream>
#include <map>
#include <vector>

#include "boost/variant.hpp"

#include "dmclock/src/dmclock_server.h"

#include "osd/scheduler/OpScheduler.h"
#include "common/config.h"
#include "common/mclock_common.h"
#include "common/ceph_context.h"
#include "osd/scheduler/OpSchedulerItem.h"


namespace ceph::osd::scheduler {

/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public OpScheduler {

  CephContext *cct;
  const unsigned cutoff_priority;

  ClientRegistry client_registry;
  MclockConfig mclock_conf;
  using mclock_queue_t = crimson::dmclock::PullPriorityQueue<
    scheduler_id_t,
    OpSchedulerItem,
    true,
    true,
    2>;
  using priority_t = unsigned;
  using SubQueue = std::map<priority_t,
	std::list<OpSchedulerItem>,
	std::greater<priority_t>>;
  mclock_queue_t scheduler;
  /**
   * high_priority
   *
   * Holds entries to be dequeued in strict order ahead of mClock
   * Invariant: entries are never empty
   */
  SubQueue high_priority;
  priority_t immediate_class_priority = std::numeric_limits<priority_t>::max();

  static scheduler_id_t get_scheduler_id(const OpSchedulerItem &item) {
    return scheduler_id_t{
      item.get_scheduler_class(),
      client_profile_id_t()
    };
  }

public: 
  template<typename Rep, typename Per>
  mClockScheduler(
    CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational, unsigned cutoff_priority,
    std::chrono::duration<Rep,Per> idle_age,
    std::chrono::duration<Rep,Per> erase_age,
    std::chrono::duration<Rep,Per> check_time,
    bool init_perfcounter=true)
    : cct(cct),
      cutoff_priority(cutoff_priority),
      mclock_conf(cct, client_registry, num_shards,
	          is_rotational, shard_id, whoami),
      scheduler(
	std::bind(&ClientRegistry::get_info,
		  &client_registry,
		  std::placeholders::_1),
	idle_age, erase_age, check_time,
	crimson::dmclock::AtLimit::Wait,
	cct->_conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
  {
    ceph_assert(num_shards > 0);
    if (init_perfcounter) {
      mclock_conf.init_logger();
    }
  }
  mClockScheduler(
    CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational, unsigned cutoff_priority,
    bool init_perfcounter=true) :
    mClockScheduler(
      cct, whoami, num_shards, shard_id, is_rotational, cutoff_priority,
      crimson::dmclock::standard_idle_age,
      crimson::dmclock::standard_erase_age,
      crimson::dmclock::standard_check_time,
      init_perfcounter) {}

  /// Calculate scaled cost per item
  uint32_t calc_scaled_cost(int cost);

  // Helper method to display mclock queues
  std::string display_queues() const;

  // Enqueue op in the back of the regular queue
  void enqueue(OpSchedulerItem &&item) final;

  // Enqueue the op in the front of the high priority queue
  void enqueue_front(OpSchedulerItem &&item) final;

  // Return an op to be dispatch
  WorkItem dequeue() final;

  // Returns if the queue is empty
  bool empty() const final {
    return scheduler.empty() && high_priority.empty();
  }

  // Formatted output of the queue
  void dump(ceph::Formatter &f) const final;

  void print(std::ostream &ostream) const final {
    ostream << get_op_queue_type_name(get_type());
    ostream << ", cutoff=" << cutoff_priority;
  }

  // Return the scheduler type
  op_queue_type_t get_type() const final {
    return op_queue_type_t::mClockScheduler;
  }

  double get_cost_per_io() const {
    return mclock_conf.get_cost_per_io();
  }
private:
  // Enqueue the op to the high priority queue
  void enqueue_high(unsigned prio, OpSchedulerItem &&item, bool front = false);
};

}
