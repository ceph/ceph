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

#include "osd/mClockClientQueue.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  /*
   * class mClockClientQueue
   */

  mClockClientQueue::mClockClientQueue(CephContext *cct) :
    queue(std::bind(&mClockClientQueue::op_class_client_info_f, this, _1),
	  cct->_conf->osd_op_queue_mclock_anticipation_timeout),
    client_info_mgr(cct)
  {
    // empty
  }

  const dmc::ClientInfo* mClockClientQueue::op_class_client_info_f(
    const mClockClientQueue::InnerClient& client)
  {
    return client_info_mgr.get_client_info(client.second);
  }

  mClockClientQueue::InnerClient
  inline mClockClientQueue::get_inner_client(const Client& cl,
					     const Request& request) {
    return InnerClient(cl, client_info_mgr.osd_op_type(request));
  }

  // Formatted output of the queue
  inline void mClockClientQueue::dump(ceph::Formatter *f) const {
    queue.dump(f);
  }

  inline void mClockClientQueue::enqueue_strict(Client cl,
						unsigned priority,
						Request&& item) {
    queue.enqueue_strict(get_inner_client(cl, item), priority,
			 std::move(item));
  }

  // Enqueue op in the front of the strict queue
  inline void mClockClientQueue::enqueue_strict_front(Client cl,
						      unsigned priority,
						      Request&& item) {
    queue.enqueue_strict_front(get_inner_client(cl, item), priority,
			       std::move(item));
  }

  // Enqueue op in the back of the regular queue
  inline void mClockClientQueue::enqueue(Client cl,
					 unsigned priority,
					 unsigned cost,
					 Request&& item) {
    auto qos_params = item.get_qos_params();
    queue.enqueue_distributed(get_inner_client(cl, item), priority, cost,
			      std::move(item), qos_params);
  }

  // Enqueue the op in the front of the regular queue
  inline void mClockClientQueue::enqueue_front(Client cl,
					       unsigned priority,
					       unsigned cost,
					       Request&& item) {
    queue.enqueue_front(get_inner_client(cl, item), priority, cost,
			std::move(item));
  }

  // Return an op to be dispatched
  inline Request mClockClientQueue::dequeue() {
    std::pair<Request, dmc::PhaseType> retn = queue.dequeue_distributed();

    if (boost::optional<OpRequestRef> _op = retn.first.maybe_get_op()) {
      (*_op)->qos_resp = retn.second;
    }
    return std::move(retn.first);
  }
} // namespace ceph
