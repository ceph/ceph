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

#pragma once

#include <ostream>
#include <variant>

#include "common/ceph_context.h"
#include "osd/scheduler/OpSchedulerItem.h"

namespace ceph::osd::scheduler {

using client = uint64_t;
using WorkItem = std::variant<std::monostate, OpSchedulerItem, double>;

/**
 * Base interface for classes responsible for choosing
 * op processing order in the OSD.
 */
class OpScheduler {
public:
  // Enqueue op for scheduling
  virtual void enqueue(OpSchedulerItem &&item) = 0;

  // Enqueue op for processing as though it were enqueued prior
  // to other items already scheduled.
  virtual void enqueue_front(OpSchedulerItem &&item) = 0;

  // Returns true iff there are no ops scheduled
  virtual bool empty() const = 0;

  // Return next op to be processed
  virtual WorkItem dequeue() = 0;

  // Dump formatted representation for the queue
  virtual void dump(ceph::Formatter &f) const = 0;

  // Print human readable brief description with relevant parameters
  virtual void print(std::ostream &out) const = 0;

  // Apply config changes to the scheduler (if any)
  virtual void update_configuration() = 0;

  // Destructor
  virtual ~OpScheduler() {};
};

std::ostream &operator<<(std::ostream &lhs, const OpScheduler &);
using OpSchedulerRef = std::unique_ptr<OpScheduler>;

OpSchedulerRef make_scheduler(
  CephContext *cct, uint32_t num_shards, bool is_rotational,
  std::string_view osd_objectstore);

/**
 * Implements OpScheduler in terms of OpQueue
 *
 * Templated on queue type to avoid dynamic dispatch, T should implement
 * OpQueue<OpSchedulerItem, client>.  This adapter is mainly responsible for
 * the boilerplate priority cutoff/strict concept which is needed for
 * OpQueue based implementations.
 */
template <typename T>
class ClassedOpQueueScheduler final : public OpScheduler {
  unsigned cutoff;
  T queue;

  static unsigned int get_io_prio_cut(CephContext *cct) {
    if (cct->_conf->osd_op_queue_cut_off == "debug_random") {
      srand(time(NULL));
      return (rand() % 2 < 1) ? CEPH_MSG_PRIO_HIGH : CEPH_MSG_PRIO_LOW;
    } else if (cct->_conf->osd_op_queue_cut_off == "high") {
      return CEPH_MSG_PRIO_HIGH;
    } else {
      // default / catch-all is 'low'
      return CEPH_MSG_PRIO_LOW;
    }
  }
public:
  template <typename... Args>
  ClassedOpQueueScheduler(CephContext *cct, Args&&... args) :
    cutoff(get_io_prio_cut(cct)),
    queue(std::forward<Args>(args)...)
  {}

  void enqueue(OpSchedulerItem &&item) final {
    unsigned priority = item.get_priority();
    unsigned cost = item.get_cost();

    if (priority >= cutoff)
      queue.enqueue_strict(
	item.get_owner(), priority, std::move(item));
    else
      queue.enqueue(
	item.get_owner(), priority, cost, std::move(item));
  }

  void enqueue_front(OpSchedulerItem &&item) final {
    unsigned priority = item.get_priority();
    unsigned cost = item.get_cost();
    if (priority >= cutoff)
      queue.enqueue_strict_front(
	item.get_owner(),
	priority, std::move(item));
    else
      queue.enqueue_front(
	item.get_owner(),
	priority, cost, std::move(item));
  }

  bool empty() const final {
    return queue.empty();
  }

  WorkItem dequeue() final {
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

  void update_configuration() final {
    // no-op
  }

  ~ClassedOpQueueScheduler() final {};
};

}
