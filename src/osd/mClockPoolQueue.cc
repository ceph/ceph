// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 SK Telecom
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>

#include "osd/mClockPoolQueue.h"
#include "common/dout.h"
#include "osd/OSD.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  /*
   * class mClockPoolQueue
   */

  mClockPoolQueue::mClockPoolQueue(CephContext *cct) :
    queue(std::bind(&mClockPoolQueue::op_class_client_info_f, this, _1)),
    client_info_mgr(cct),
    service(nullptr)
  {
    // empty
  }

  const dmc::ClientInfo* mClockPoolQueue::op_class_client_info_f(
    const mClockPoolQueue::InnerClient& client)
  {
    if (service && client.second == osd_op_type_t::client_op) {
      OSDMapRef osdmap = service->get_osdmap();
      const pg_pool_t* pp = osdmap->get_pg_pool(client.first);
      if (!pp) {
	return client_info_mgr.get_pool_default();
      }

      Mutex::Locker l(client_info_mgr.get_lock());
      auto i = client_info_mgr.cli_info_map.find(client.first);
      if (client_info_mgr.cli_info_map.end() != i) {
	*i->second = dmc::ClientInfo(pp->get_qos_res(),
				     pp->get_qos_wgt(),
				     pp->get_qos_lim());
	return i->second;
      } else {
	dmc::ClientInfo *client_info = new dmc::ClientInfo(pp->get_qos_res(),
							   pp->get_qos_wgt(),
							   pp->get_qos_lim());
	client_info_mgr.cli_info_map[client.first] = client_info;
	return client_info;
      }
    } else {
      return client_info_mgr.get_client_info(client.second);
    }
  }

  void mClockPoolQueue::set_osd_service(OSDService *osd_service) {
    service = osd_service;
  }

  int64_t mClockPoolQueue::get_pool(const Request& request) {
    if (osd_op_type_t::client_op == client_info_mgr.osd_op_type(request)) {
      return request.get_ordering_token().pool();
    }
    return -1;
  }

  mClockPoolQueue::InnerClient
  mClockPoolQueue::get_inner_client(const Client& cl,
				    const Request& request) {
    return InnerClient(get_pool(request), client_info_mgr.osd_op_type(request));
  }

  // Formatted output of the queue
  void mClockPoolQueue::dump(ceph::Formatter *f) const {
    queue.dump(f);
  }

  void mClockPoolQueue::enqueue_strict(Client cl,
				       unsigned priority,
				       Request&& item) {
    queue.enqueue_strict(get_inner_client(cl, item), priority,
			 std::move(item));
  }

  // Enqueue op in the front of the strict queue
  void mClockPoolQueue::enqueue_strict_front(Client cl,
					     unsigned priority,
					     Request&& item) {
    queue.enqueue_strict_front(get_inner_client(cl, item), priority,
			       std::move(item));
  }

  // Enqueue op in the back of the regular queue
  void mClockPoolQueue::enqueue(Client cl,
				unsigned priority,
				unsigned cost,
				Request&& item) {
    auto qos_params = item.get_qos_params();
    queue.enqueue_distributed(get_inner_client(cl, item), priority, cost,
			      std::move(item), qos_params);
  }

  // Enqueue the op in the front of the regular queue
  void mClockPoolQueue::enqueue_front(Client cl,
				      unsigned priority,
				      unsigned cost,
				      Request&& item) {
    queue.enqueue_front(get_inner_client(cl, item), priority, cost,
			std::move(item));
  }

  // Return an op to be dispatched
  Request mClockPoolQueue::dequeue() {
    std::pair<Request, dmc::PhaseType> retn = queue.dequeue_distributed();

    if (boost::optional<OpRequestRef> _op = retn.first.maybe_get_op()) {
      (*_op)->qos_resp = retn.second;
    }
    return std::move(retn.first);
  }
} // namespace ceph
