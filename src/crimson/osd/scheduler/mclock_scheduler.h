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


#pragma once

#include <chrono>
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

namespace dmc = crimson::dmclock;
using namespace std::placeholders;
using namespace std::literals;

namespace crimson::osd::scheduler {

/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public Scheduler, md_config_obs_t {

  crimson::common::CephContext *cct;
  unsigned cutoff_priority;
  PerfCounters *logger;

  ClientRegistry client_registry;
  MclockConfig mclock_conf;

  class crimson_mclock_cleaning_job_t {
    struct job_control_t {
      std::chrono::milliseconds period;
      std::function<void()> body;

      bool stopping = false;
      seastar::condition_variable cv;


      template <typename D, typename F>
      job_control_t(D _period, F &&_body) :
	period(std::chrono::duration_cast<decltype(period)>(_period)),
	body(std::forward<F>(_body)) {
      }
    };
    seastar::lw_shared_ptr<job_control_t> control;

    static seastar::future<> run(
      seastar::lw_shared_ptr<job_control_t> control) {
      while (!control->stopping) {
	std::invoke(control->body);
	co_await control->cv.wait(control->period);
      }
    }
  public:
    template<typename... Args>
      crimson_mclock_cleaning_job_t(Args&&... args) :
      control(seastar::make_lw_shared<job_control_t>(
		std::forward<Args>(args)...))
    {
      std::ignore = run(control);
    }

    void try_update(milliseconds _period) {
      control->period = _period;
      control->cv.signal();
    }

    ~crimson_mclock_cleaning_job_t() {
      control->stopping = true;
      control->cv.signal();
    }
  };
  using mclock_queue_t = crimson::dmclock::PullPriorityQueue<
    scheduler_id_t,
    item_t,
    true,
    true,
    2,
    crimson_mclock_cleaning_job_t>;
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
      client_profile_id_t()
    };
  }

public:
  template<typename Rep, typename Per>
  mClockScheduler(CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational,
    std::chrono::duration<Rep,Per> idle_age,
    std::chrono::duration<Rep,Per> erase_age,
    std::chrono::duration<Rep,Per> check_time,
    bool init_perfcounter=true, MonClient *monc=nullptr)
  : cct(cct),
    logger(nullptr),
    mclock_conf(cct, client_registry, num_shards, is_rotational, shard_id, whoami),
    scheduler(
      std::bind(&ClientRegistry::get_info,
                &client_registry,
                _1),
      idle_age, erase_age, check_time,
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
}
  mClockScheduler(CephContext *cct, int whoami, uint32_t num_shards,
    int shard_id, bool is_rotational,
    bool init_perfcounter=true, MonClient *monc=nullptr) :
    mClockScheduler(
      cct, whoami, num_shards, shard_id, is_rotational,
      crimson::dmclock::standard_idle_age,
      crimson::dmclock::standard_erase_age,
      crimson::dmclock::standard_check_time,
      init_perfcounter, monc) {}
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
