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
#include <functional>

#include "osd/mClockClientProfileQueue.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  /*
   * class mClockClientProfileQueue
   */

  mClockClientProfileQueue::mClockClientProfileQueue(CephContext *cct) :
    op_class_mgr(cct),
    client_queue(std::bind(&client_profile_mgr_t::get,
			   &client_profile_mgr, _1),
		 true,
		 cct->_conf->osd_op_queue_mclock_anticipation_timeout,
		 std::bind(&client_profile_mgr_t::clear,
			   &client_profile_mgr, _1)),
    top_queue(std::bind(&ceph::mclock::OpClassClientInfoMgr::get_client_info,
			&op_class_mgr, _1),
	      cct->_conf->osd_op_queue_mclock_anticipation_timeout)
  { }

  // Formatted output of the queue
  inline void mClockClientProfileQueue::dump(ceph::Formatter *f) const {
    top_queue.dump(f);

    f->open_object_section("client_request_subqueue");
    f->dump_int("size", client_queue.request_count());
    f->close_section();
  }

  inline void mClockClientProfileQueue::enqueue_strict(Client cl,
						       unsigned priority,
						       Request&& item) {
    top_queue.enqueue_strict(op_class_mgr.osd_op_type(item),
			     priority,
			     RequestProxyWrapper(std::move(item)));
  }

  // Enqueue op in the front of the strict queue
  inline void mClockClientProfileQueue::enqueue_strict_front(Client cl,
							     unsigned priority,
							     Request&& item) {
    top_queue.enqueue_strict_front(op_class_mgr.osd_op_type(item),
				   priority,
				   RequestProxyWrapper(std::move(item)));
  }

  // Enqueue op in the back of the regular queue
  inline void mClockClientProfileQueue::enqueue(Client cl,
						unsigned priority,
						unsigned cost,
						Request&& item) {
    dmc::ReqParams qos_req_params = item.get_qos_req_params();
    const qos_params_t& qos_profile_params = item.get_qos_profile_params();
    ClientProfile client_profile(cl,
				 qos_profile_params.qos_profile_id);
    osd_op_type_t op_type = op_class_mgr.osd_op_type(item);

    if (osd_op_type_t::client_op == op_type) {
      client_profile_mgr.set(client_profile,
			     qos_profile_params.reservation,
			     qos_profile_params.weight,
			     qos_profile_params.limit);
#warning how is cost determined?
#warning enqueue a proxy here
      client_queue.add_request(std::move(item),
			       client_profile,
			       qos_req_params,
			       cost);
      top_queue.enqueue(op_type, priority, cost, RequestProxyWrapper());
    } else {
      top_queue.enqueue(op_type,
			priority, cost,
			RequestProxyWrapper(std::move(item)));
    }
  }

  // Enqueue the op in the front of the regular queue
  inline void mClockClientProfileQueue::enqueue_front(Client cl,
						      unsigned priority,
						      unsigned cost,
						      Request&& item) {
    top_queue.enqueue_front(op_class_mgr.osd_op_type(item),
			    priority,
			    cost,
			    RequestProxyWrapper(std::move(item)));
  }

  // Return an op to be dispatched
  inline Request mClockClientProfileQueue::dequeue() {
    top_queue_t::Retn top_retn = top_queue.dequeue_distributed();
    if (top_retn.request.is_proxy) {
      auto pr = client_queue.pull_request();
      assert(pr.is_retn());
      auto& client_retn = pr.get_retn();
      boost::optional<OpRequestRef> _op =
	client_retn.request->maybe_get_op();
      assert(_op);
      (*_op)->qos_phase = client_retn.phase;
      (*_op)->qos_cost = client_retn.cost;
      return std::move(*client_retn.request);
    } else {
      if (boost::optional<OpRequestRef> _op =
	  top_retn.request.request->maybe_get_op()) {
	(*_op)->qos_phase = top_retn.phase;
	(*_op)->qos_cost = top_retn.cost;
      }
      return std::move(*top_retn.request.request);
    }
  }
} // namespace ceph
