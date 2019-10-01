// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <ostream>

#include <seastar/core/print.hh>

#include "crimson/osd/scheduler/scheduler.h"
#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "common/WeightedPriorityQueue.h"

namespace crimson::osd::scheduler {

std::ostream &operator<<(std::ostream &lhs, const scheduler_class_t &c)
{
  switch (c) {
  case scheduler_class_t::background_best_effort:
    return lhs << "background_best_effort";
  case scheduler_class_t::background_recovery:
    return lhs << "background_recovery";
  case scheduler_class_t::client:
    return lhs << "client";
  case scheduler_class_t::repop:
    return lhs << "repop";
  case scheduler_class_t::immediate:
    return lhs << "immediate";
  default:
    return lhs;
  }
}

/**
 * Implements Scheduler in terms of OpQueue
 *
 * Templated on queue type to avoid dynamic dispatch, T should implement
 * OpQueue<Scheduleritem_t, client_t>.  This adapter is mainly responsible for
 * the boilerplate priority cutoff/strict concept which is needed for
 * OpQueue based implementations.
 */
template <typename T>
class ClassedOpQueueScheduler : public Scheduler {
  const scheduler_class_t cutoff;
  T queue;

  using priority_t = uint64_t;
  std::array<
    priority_t,
    static_cast<size_t>(scheduler_class_t::immediate)
  > priority_map = {
    // Placeholder, gets replaced with configured values
    0, 0, 0
  };

  static scheduler_class_t get_io_prio_cut(ConfigProxy &conf) {
    if (conf.get_val<std::string>("osd_op_queue_cut_off") == "debug_random") {
      srand(time(NULL));
      return (rand() % 2 < 1) ?
	scheduler_class_t::repop : scheduler_class_t::immediate;
    } else if (conf.get_val<std::string>("osd_op_queue_cut_off") == "high") {
      return scheduler_class_t::immediate;
    } else {
      return scheduler_class_t::repop;
    }
  }

  bool use_strict(scheduler_class_t kl) const {
    return static_cast<uint8_t>(kl) >= static_cast<uint8_t>(cutoff);
  }

  priority_t get_priority(scheduler_class_t kl) const {
    ceph_assert(static_cast<size_t>(kl) <
		static_cast<size_t>(scheduler_class_t::immediate));
    return priority_map[static_cast<size_t>(kl)];
  }

public:
  template <typename... Args>
  ClassedOpQueueScheduler(ConfigProxy &conf, Args&&... args) :
    cutoff(get_io_prio_cut(conf)),
    queue(std::forward<Args>(args)...)
  {
    priority_map[
      static_cast<size_t>(scheduler_class_t::background_best_effort)
    ] = conf.get_val<uint64_t>("osd_scrub_priority");
    priority_map[
      static_cast<size_t>(scheduler_class_t::background_recovery)
    ] = conf.get_val<uint64_t>("osd_recovery_op_priority");
    priority_map[
      static_cast<size_t>(scheduler_class_t::client)
    ] = conf.get_val<uint64_t>("osd_client_op_priority");
    priority_map[
      static_cast<size_t>(scheduler_class_t::repop)
    ] = conf.get_val<uint64_t>("osd_client_op_priority");
  }

  void enqueue(item_t &&item) final {
    if (use_strict(item.params.klass))
      queue.enqueue_strict(
	item.params.owner, get_priority(item.params.klass), std::move(item));
    else
      queue.enqueue(
	item.params.owner, get_priority(item.params.klass),
	item.params.cost, std::move(item));
  }

  void enqueue_front(item_t &&item) final {
    if (use_strict(item.params.klass))
      queue.enqueue_strict_front(
	item.params.owner, get_priority(item.params.klass), std::move(item));
    else
      queue.enqueue_front(
	item.params.owner, get_priority(item.params.klass),
	item.params.cost, std::move(item));
  }

  bool empty() const final {
    return queue.empty();
  }

  item_t dequeue() final {
    return queue.dequeue();
  }

  void dump(ceph::Formatter &f) const final {
    return queue.dump(&f);
  }

  void print(std::ostream &out) const final {
    out << "ClassedOpQueueScheduler(queue=";
    queue.print(out);
    out << ", cutoff=" << cutoff << ")";
  }

  ~ClassedOpQueueScheduler() final {};
};

SchedulerRef make_scheduler(ConfigProxy &conf)
{
  const std::string _type = conf.get_val<std::string>("osd_op_queue");
  const std::string *type = &_type;
  if (*type == "debug_random") {
    static const std::string index_lookup[] = { "mclock_scheduler",
						"wpq" };
    srand(time(NULL));
    unsigned which = rand() % (sizeof(index_lookup) / sizeof(index_lookup[0]));
    type = &index_lookup[which];
  }

  if (*type == "wpq" ) {
    // default is 'wpq'
    return std::make_unique<
      ClassedOpQueueScheduler<WeightedPriorityQueue<item_t, client_t>>>(
	conf,
	conf.get_val<uint64_t>("osd_op_pq_max_tokens_per_priority"),
	conf->osd_op_pq_min_cost
      );
  } else if (*type == "mclock_scheduler") {
    return std::make_unique<mClockScheduler>(conf);
  } else {
    ceph_assert("Invalid choice of wq" == 0);
    return std::unique_ptr<mClockScheduler>();
  }
}

std::ostream &operator<<(std::ostream &lhs, const Scheduler &rhs) {
  rhs.print(lhs);
  return lhs;
}

}
