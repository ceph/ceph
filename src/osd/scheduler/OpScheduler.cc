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

OpSchedulerRef make_scheduler(CephContext *cct)
{
  const std::string *type = &cct->_conf->osd_op_queue;
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
      ClassedOpQueueScheduler<WeightedPriorityQueue<OpSchedulerItem, client>>>(
	cct,
	cct->_conf->osd_op_pq_max_tokens_per_priority,
	cct->_conf->osd_op_pq_min_cost
    );
  } else if (*type == "mclock_scheduler") {
    return std::make_unique<mClockScheduler>(cct);
  } else {
    ceph_assert("Invalid choice of wq" == 0);
  }
}

std::ostream &operator<<(std::ostream &lhs, const OpScheduler &rhs) {
  rhs.print(lhs);
  return lhs;
}

}
