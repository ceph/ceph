// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/mclock_common.h"

namespace crimson::osd::scheduler {

using client_t = uint64_t;

struct params_t {
  int cost = 1;
  unsigned priority = 0;
  client_t owner;
  SchedulerClass klass;
};

// What an operation may wait on while it holds a throttle slot.  This is
// about dependencies, not QoS: SchedulerClass decides the order in which
// operations are admitted, while the level decides whether an operation can
// be admitted at all.  An operation at a level only waits on operations at
// lower levels, so as long as the lower levels always have slots of their
// own, every slot holder eventually finishes.
enum class throttle_level_t : uint8_t {
  // only does local work, and is waited on by other slot holders: RepRequest,
  // EC sub-ops, recovery pushes and pulls, ScrubScan
  leaf,
  // waits on leaf operations, local or remote: recover_object, backfill scans
  recovery,
  // waits on recovery and leaf operations: client requests, snaptrim
  foreground,
  count
};

struct item_t {
  params_t params;
  throttle_level_t level = throttle_level_t::foreground;
  seastar::promise<> wake;
  int get_cost() const { return params.cost; }
  unsigned get_priority() const { return params.priority; }
};

using WorkItem = std::variant<std::monostate, item_t, double>;
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
  virtual WorkItem dequeue() = 0;

  // Dump formatted representation for the queue
  virtual void dump(ceph::Formatter &f) const = 0;

  // Print human readable brief description with relevant parameters
  virtual void print(std::ostream &out) const = 0;

  // Destructor
  virtual ~Scheduler() {};
};

std::ostream &operator<<(std::ostream &lhs, const Scheduler &);
using SchedulerRef = std::unique_ptr<Scheduler>;

SchedulerRef make_scheduler(CephContext *cct, ConfigProxy &, int whoami, uint32_t num_shards,
                            int shard_id, bool is_rotational, bool perf_cnt);

}
