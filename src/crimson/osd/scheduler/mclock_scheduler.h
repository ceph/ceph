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
#include "common/config.h"
#include "common/ceph_context.h"


namespace crimson::osd::scheduler {

using client_id_t = uint64_t;
using profile_id_t = uint64_t;

struct client_profile_id_t {
  client_id_t client_id;
  profile_id_t profile_id;
  auto operator<=>(const client_profile_id_t&) const = default;
};


struct scheduler_id_t {
  scheduler_class_t class_id;
  client_profile_id_t client_profile_id;
  auto operator<=>(const scheduler_id_t&) const = default;
};

/**
 * Scheduler implementation based on mclock.
 *
 * TODO: explain configs
 */
class mClockScheduler : public Scheduler, md_config_obs_t {

  class ClientRegistry {
    std::array<
      crimson::dmclock::ClientInfo,
      static_cast<size_t>(scheduler_class_t::client)
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
  mclock_queue_t scheduler;
  std::list<item_t> immediate;

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
  mClockScheduler(ConfigProxy &conf);

  // Enqueue op in the back of the regular queue
  void enqueue(item_t &&item) final;

  // Enqueue the op in the front of the regular queue
  void enqueue_front(item_t &&item) final;

  // Return an op to be dispatch
  item_t dequeue() final;

  // Returns if the queue is empty
  bool empty() const final {
    return immediate.empty() && scheduler.empty();
  }

  // Formatted output of the queue
  void dump(ceph::Formatter &f) const final;

  void print(std::ostream &ostream) const final {
    ostream << "mClockScheduler";
  }

  std::vector<std::string> get_tracked_keys() const noexcept final;

  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
};

}
