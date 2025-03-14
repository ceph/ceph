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

#include <seastar/core/future.hh>
#include <ostream>

#include "crimson/common/config_proxy.h"

namespace crimson::osd::scheduler {

enum class scheduler_class_t : uint8_t {
  background_best_effort = 0,
  background_recovery,
  client,
  repop,
  immediate,
};

std::ostream &operator<<(std::ostream &, const scheduler_class_t &);

using client_t = uint64_t;
using cost_t = uint64_t;

struct params_t {
  cost_t cost = 1;
  client_t owner;
  scheduler_class_t klass;
};

struct item_t {
  params_t params;
  seastar::promise<> wake;
};

/**
 * Base interface for classes responsible for choosing
 * op processing order in the OSD.
 */
class Scheduler {
public:
  // Enqueue op for scheduling
  virtual void enqueue(item_t &&item) = 0;

  // Enqueue op for processing as though it were enqueued prior
  // to other items already scheduled.
  virtual void enqueue_front(item_t &&item) = 0;

  // Returns true iff there are no ops scheduled
  virtual bool empty() const = 0;

  // Return next op to be processed
  virtual item_t dequeue() = 0;

  // Dump formatted representation for the queue
  virtual void dump(ceph::Formatter &f) const = 0;

  // Print human readable brief description with relevant parameters
  virtual void print(std::ostream &out) const = 0;

  // Destructor
  virtual ~Scheduler() {};
};

std::ostream &operator<<(std::ostream &lhs, const Scheduler &);
using SchedulerRef = std::unique_ptr<Scheduler>;

SchedulerRef make_scheduler(ConfigProxy &);

}
