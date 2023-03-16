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
#include "common/ceph_context.h"
#include "common/mClockPriorityQueue.h"
#include "osd/scheduler/OpSchedulerItem.h"


namespace ceph::osd::scheduler {

constexpr uint64_t default_min = 1;
constexpr uint64_t default_max = 999999;

using client_id_t = uint64_t;
using profile_id_t = uint64_t;

struct client_profile_id_t {
  client_id_t client_id;
  profile_id_t profile_id;

  auto operator<=>(const client_profile_id_t&) const = default;
  friend std::ostream& operator<<(std::ostream& out,
                                  const client_profile_id_t& client_profile) {
    out << " client_id: " << client_profile.client_id
        << " profile_id: " << client_profile.profile_id;
    return out;
  }
};

struct scheduler_id_t {
  op_scheduler_class class_id;
  client_profile_id_t client_profile_id;

  auto operator<=>(const scheduler_id_t&) const = default;
  friend std::ostream& operator<<(std::ostream& out,
                                  const scheduler_id_t& sched_id) {
    out << "{ class_id: " << sched_id.class_id
        << sched_id.client_profile_id;
    return out << " }";
  }
};

/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public OpScheduler, md_config_obs_t {

  CephContext *cct;
  const int whoami;
  const uint32_t num_shards;
  const int shard_id;
  bool is_rotational;
  MonClient *monc;
  double max_osd_capacity;
  double osd_mclock_cost_per_io;
  double osd_mclock_cost_per_byte;
  std::string mclock_profile = "high_client_ops";
  struct ClientAllocs {
    uint64_t res;
    uint64_t wgt;
    uint64_t lim;

    ClientAllocs(uint64_t _res, uint64_t _wgt, uint64_t _lim) {
      update(_res, _wgt, _lim);
    }

    inline void update(uint64_t _res, uint64_t _wgt, uint64_t _lim) {
      res = _res;
      wgt = _wgt;
      lim = _lim;
    }
  };
  std::array<
    ClientAllocs,
    static_cast<size_t>(op_scheduler_class::client) + 1
  > client_allocs = {
    // Placeholder, get replaced with configured values
    ClientAllocs(1, 1, 1), // background_recovery
    ClientAllocs(1, 1, 1), // background_best_effort
    ClientAllocs(1, 1, 1), // immediate (not used)
    ClientAllocs(1, 1, 1)  // client
  };
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
	client_profile_id_t{
	item.get_owner(),
	  0
	  }
    };
  }

  static unsigned int get_io_prio_cut(CephContext *cct) {
    if (cct->_conf->osd_op_queue_cut_off == "debug_random") {
      std::random_device rd;
      std::mt19937 random_gen(rd());
      return (random_gen() % 2 < 1) ? CEPH_MSG_PRIO_HIGH : CEPH_MSG_PRIO_LOW;
    } else if (cct->_conf->osd_op_queue_cut_off == "high") {
      return CEPH_MSG_PRIO_HIGH;
    } else {
      // default / catch-all is 'low'
      return CEPH_MSG_PRIO_LOW;
    }
  }

public:
  mClockScheduler(CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational, MonClient *monc);
  ~mClockScheduler() override;

  // Set the max osd capacity in iops
  void set_max_osd_capacity();

  // Set the cost per io for the osd
  void set_osd_mclock_cost_per_io();

  // Set the cost per byte for the osd
  void set_osd_mclock_cost_per_byte();

  // Set the mclock profile type to enable
  void set_mclock_profile();

  // Get the active mclock profile
  std::string get_mclock_profile();

  // Set "balanced" profile allocations
  void set_balanced_profile_allocations();

  // Set "high_recovery_ops" profile allocations
  void set_high_recovery_ops_profile_allocations();

  // Set "high_client_ops" profile allocations
  void set_high_client_ops_profile_allocations();

  // Set the mclock related config params based on the profile
  void enable_mclock_profile_settings();

  // Set mclock config parameter based on allocations
  void set_profile_config();

  // Calculate scale cost per item
  int calc_scaled_cost(int cost);

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
    ostream << "mClockScheduler";
  }

  // Update data associated with the modified mclock config key(s)
  void update_configuration() final;

  const char** get_tracked_conf_keys() const final;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
private:
  // Enqueue the op to the high priority queue
  void enqueue_high(unsigned prio, OpSchedulerItem &&item, bool front = false);
};

}
