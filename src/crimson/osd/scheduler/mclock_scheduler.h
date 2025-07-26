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
#include "crimson/osd/scheduler/scheduler.h"
#include "crimson/mon/MonClient.h"

#include "common/config.h"
#include "common/ceph_context.h"
#include "common/mclock_common.h"

namespace crimson::osd::scheduler {

 
/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public Scheduler, md_config_obs_t {

  crimson::common::CephContext *cct;
  //const int whoami;
  //const uint32_t num_shards;
  //const int shard_id;
  //const bool is_rotational;
  unsigned cutoff_priority;
  PerfCounters *logger;

  ClientRegistry client_registry;
  MclockConfig mclock_conf;

  using mclock_queue_t = crimson::dmclock::PullPriorityQueue<
    scheduler_id_t,
    item_t,
    true,
    true,
    2>;
  using priority_t = unsigned;
  using SubQueue = std::map<priority_t,
	std::list<item_t>,
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

  static scheduler_id_t get_scheduler_id(const item_t &item) {
    return scheduler_id_t{
      item.params.klass,
      client_profile_id_t{
        item.params.owner,
        0
      }
    };
  }

public:
  mClockScheduler(CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational, bool init_perfcounter=true);
  ~mClockScheduler() override;

  /// Calculate scaled cost per item
  uint32_t calc_scaled_cost(int cost);

  // Helper method to display mclock queues
  std::string display_queues() const;

  // Enqueue op in the back of the regular queue
  void enqueue(item_t &&item) final;

  // Enqueue the op in the front of the high priority queue
  void enqueue_front(item_t &&item) final;

  // Return an op to be dispatch
  WorkItem dequeue() final;

  // Returns if the queue is empty
  bool empty() const final {
    return scheduler.empty() && high_priority.empty();
  }

  // Formatted output of the queue
  void dump(ceph::Formatter &f) const final;

  void print(std::ostream &ostream) const final {
    ostream << "mClockScheduer ";
    ostream << ", cutoff=" << cutoff_priority;
  }

  std::vector<std::string> get_tracked_keys() const noexcept final;

  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;

  double get_cost_per_io() const {
    return mclock_conf.get_cost_per_io();
  }
private:
  // Enqueue the op to the high priority queue
  void enqueue_high(unsigned prio, item_t &&item, bool front = false);
};

}
