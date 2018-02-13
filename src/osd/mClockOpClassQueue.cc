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


#include <memory>

#include "osd/mClockOpClassQueue.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  /*
   * class mClockOpClassQueue
   */

  mClockOpClassQueue::mClockOpClassQueue(CephContext *cct) :
    queue(std::bind(&mClockOpClassQueue::op_class_client_info_f, this, _1),
	  cct->_conf->osd_op_queue_mclock_anticipation_timeout),
    client_info_mgr(cct)
  {
    // empty
  }

  const dmc::ClientInfo* mClockOpClassQueue::op_class_client_info_f(
    const osd_op_type_t& op_type)
  {
    return client_info_mgr.get_client_info(op_type);
  }

  // Formatted output of the queue
  void mClockOpClassQueue::dump(ceph::Formatter *f) const {
    queue.dump(f);
  }
} // namespace ceph
