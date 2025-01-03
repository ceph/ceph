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

#include "osd/scheduler/OpScheduler.h"

#include "common/WeightedPriorityQueue.h"
#include "osd/scheduler/mClockScheduler.h"

namespace ceph::osd::scheduler {

OpSchedulerRef make_scheduler(
  CephContext *cct, int whoami, uint32_t num_shards, int shard_id,
  bool is_rotational, std::string_view osd_objectstore,
  op_queue_type_t osd_scheduler, unsigned op_queue_cut_off, MonClient *monc)
{
  // Force the use of 'wpq' scheduler for filestore OSDs.
  // The 'mclock_scheduler' is not supported for filestore OSDs.
  if (op_queue_type_t::WeightedPriorityQueue == osd_scheduler ||
      osd_objectstore == "filestore") {
    return std::make_unique<
      ClassedOpQueueScheduler<WeightedPriorityQueue<OpSchedulerItem, client>>>(
	cct,
        op_queue_cut_off,
	cct->_conf->osd_op_pq_max_tokens_per_priority,
	cct->_conf->osd_op_pq_min_cost
    );
  } else if (op_queue_type_t::mClockScheduler == osd_scheduler) {
    // default is 'mclock_scheduler'
    return std::make_unique<
      mClockScheduler>(cct, whoami, num_shards, shard_id, is_rotational,
        op_queue_cut_off, monc);
  } else {
    ceph_assert("Invalid choice of wq" == 0);
  }
}

std::ostream &operator<<(std::ostream &lhs, const OpScheduler &rhs) {
  rhs.print(lhs);
  return lhs;
}

}
