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

#include <ostream>
#include <map>
#include <vector>

#include "boost/variant.hpp"

#include "dmclock/src/dmclock_server.h"

#include "osd/scheduler/OpScheduler.h"
#include "common/config.h"
#include "include/cmp.h"
#include "common/ceph_context.h"
#include "common/mClockPriorityQueue.h"
#include "osd/scheduler/OpSchedulerItem.h"


namespace ceph::osd::scheduler {

constexpr uint64_t default_min = 1;
constexpr uint64_t default_max = 999999;

using client_id_t = uint64_t;
using profile_id_t = uint64_t;
using op_type_t = OpSchedulerItem::OpQueueable::op_type_t;

struct client_profile_id_t {
  client_id_t client_id;
  profile_id_t profile_id;
};

WRITE_EQ_OPERATORS_2(client_profile_id_t, client_id, profile_id)
WRITE_CMP_OPERATORS_2(client_profile_id_t, client_id, profile_id)


struct scheduler_id_t {
  op_scheduler_class class_id;
  client_profile_id_t client_profile_id;
};

WRITE_EQ_OPERATORS_2(scheduler_id_t, class_id, client_profile_id)
WRITE_CMP_OPERATORS_2(scheduler_id_t, class_id, client_profile_id)

/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public OpScheduler, md_config_obs_t {

  CephContext *cct;
  const uint32_t num_shards;
  bool is_rotational;
  double max_osd_capacity;
  uint64_t osd_mclock_cost_per_io_msec;
  std::string mclock_profile = "balanced";
  std::map<op_scheduler_class, double> client_allocs;
  std::map<op_type_t, int> client_cost_infos;
  std::map<op_type_t, int> client_scaled_cost_infos;
  class ClientRegistry {
    std::array<
      crimson::dmclock::ClientInfo,
      static_cast<size_t>(op_scheduler_class::immediate)
    > internal_client_infos = {
      // Placeholder, gets replaced with configured values
      crimson::dmclock::ClientInfo(1, 1, 1),
      crimson::dmclock::ClientInfo(1, 1, 1)
    };

    crimson::dmclock::ClientInfo default_external_client_info = {1, 1, 1};
    std::map<client_profile_id_t,
	     crimson::dmclock::ClientInfo> external_client_infos;
    const crimson::dmclock::ClientInfo *get_external_client(
      const client_profile_id_t &client) const;
  public:
    void update_from_config(const ConfigProxy &conf);
    const crimson::dmclock::ClientInfo *get_info(
      const scheduler_id_t &id) const;
  } client_registry;

  using mclock_queue_t = crimson::dmclock::PullPriorityQueue<
    scheduler_id_t,
    OpSchedulerItem,
    true,
    true,
    2>;
  mclock_queue_t scheduler;
  std::list<OpSchedulerItem> immediate;

  static scheduler_id_t get_scheduler_id(const OpSchedulerItem &item) {
    return scheduler_id_t{
      item.get_scheduler_class(),
	client_profile_id_t{
	item.get_owner(),
	  0
	  }
    };
  }

public:
  mClockScheduler(CephContext *cct, uint32_t num_shards, bool is_rotational);

  // Set the max osd capacity in iops
  void set_max_osd_capacity();

  // Set the cost per io for the osd
  void set_osd_mclock_cost_per_io();

  // Set the mclock related config params based on the profile
  void enable_mclock_profile();

  // Get the active mclock profile
  std::string get_mclock_profile();

  // Set client capacity allocations based on profile
  void set_client_allocations();

  // Get client allocation
  double get_client_allocation(op_type_t op_type);

  // Set "balanced" profile parameters
  void set_balanced_profile_config();

  // Set "high_recovery_ops" profile parameters
  void set_high_recovery_ops_profile_config();

  // Set "high_client_ops" profile parameters
  void set_high_client_ops_profile_config();

  // Set recovery specific Ceph settings for profiles
  void set_global_recovery_options();

  // Calculate scale cost per item
  int calc_scaled_cost(op_type_t op_type, int cost);

  // Update mclock client cost info
  bool maybe_update_client_cost_info(op_type_t op_type, int new_cost);

  // Enqueue op in the back of the regular queue
  void enqueue(OpSchedulerItem &&item) final;

  // Enqueue the op in the front of the regular queue
  void enqueue_front(OpSchedulerItem &&item) final;

  // Return an op to be dispatch
  WorkItem dequeue() final;

  // Returns if the queue is empty
  bool empty() const final {
    return immediate.empty() && scheduler.empty();
  }

  // Formatted output of the queue
  void dump(ceph::Formatter &f) const final;

  void print(std::ostream &ostream) const final {
    ostream << "mClockScheduler";
  }

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
};

}
