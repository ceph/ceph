// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SK Telecom
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


#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  mClockPoolQueue::mclock_op_tags_t::mclock_op_tags_t(CephContext *cct) :
    client_op(cct->_conf->osd_op_queue_mclock_client_op_res,
	      cct->_conf->osd_op_queue_mclock_client_op_wgt,
	      cct->_conf->osd_op_queue_mclock_client_op_lim),
    osd_subop(cct->_conf->osd_op_queue_mclock_osd_subop_res,
	      cct->_conf->osd_op_queue_mclock_osd_subop_wgt,
	      cct->_conf->osd_op_queue_mclock_osd_subop_lim),
    snaptrim(cct->_conf->osd_op_queue_mclock_snap_res,
	     cct->_conf->osd_op_queue_mclock_snap_wgt,
	     cct->_conf->osd_op_queue_mclock_snap_lim),
    recov(cct->_conf->osd_op_queue_mclock_recov_res,
	  cct->_conf->osd_op_queue_mclock_recov_wgt,
	  cct->_conf->osd_op_queue_mclock_recov_lim),
    scrub(cct->_conf->osd_op_queue_mclock_scrub_res,
	  cct->_conf->osd_op_queue_mclock_scrub_wgt,
	  cct->_conf->osd_op_queue_mclock_scrub_lim)
  {
    dout(20) <<
      "mClockPoolQueue settings:: " <<
      "client_op:" << client_op <<
      "; osd_subop:" << osd_subop <<
      "; snaptrim:" << snaptrim <<
      "; recov:" << recov <<
      "; scrub:" << scrub <<
      dendl;
  }


  dmc::ClientInfo
  mClockPoolQueue::op_class_client_info_f(
    const mClockPoolQueue::InnerClient& client)
  {
    OSDMapRef osdmap = mclock_service->get_osdmap();
    const pg_pool_t* pp = osdmap->get_pg_pool(client.first);

    switch(client.second) {
    case osd_op_type_t::client_op:
    case osd_op_type_t::osd_subop:
      return dmc::ClientInfo(pp->mclock_res, pp->mclock_wgt, pp->mclock_lim);
    case osd_op_type_t::bg_snaptrim:
      return mclock_op_tags->snaptrim;
    case osd_op_type_t::bg_recovery:
      return mclock_op_tags->recov;
    case osd_op_type_t::bg_scrub:
      return mclock_op_tags->scrub;
    default:
      assert(0);
      return dmc::ClientInfo(-1, -1, -1);
    }
  }


  /*
   * class mClockPoolQueue
   */

  std::unique_ptr<mClockPoolQueue::mclock_op_tags_t>
  mClockPoolQueue::mclock_op_tags(nullptr);

  std::unique_ptr<OSDService>
  mClockPoolQueue::mclock_service(nullptr);

  mClockPoolQueue::pg_queueable_visitor_t
  mClockPoolQueue::pg_queueable_visitor;

  mClockPoolQueue::mClockPoolQueue(CephContext *cct,
                                   bool _allow_limit_break) :
    queue(&mClockPoolQueue::op_class_client_info_f, _allow_limit_break)
  {
    // manage the singleton
    if (!mclock_op_tags) {
      mclock_op_tags.reset(new mclock_op_tags_t(cct));
    }
  }

  void mClockPoolQueue::set_mclock_service(OSDService *service)
  {
    // manage the singleton
    if (!mclock_service) {
      mclock_service.reset(service);
    }
  }

  mClockPoolQueue::osd_op_type_t
  mClockPoolQueue::get_osd_op_type(const Request& request) {
    osd_op_type_t type =
      boost::apply_visitor(pg_queueable_visitor, request.second.get_variant());

    // if we got client_op back then we need to distinguish between
    // a client op and an osd subop.

    if (osd_op_type_t::client_op != type) {
      return type;
    } else if (CEPH_MSG_OSD_OP !=
	       boost::get<OpRequestRef>(
		 request.second.get_variant())->get_req()->get_header().type) {
      return osd_op_type_t::osd_subop;
    } else {
      return osd_op_type_t::client_op;
    }
  }

  int64_t mClockPoolQueue::get_pool(const Request& request) {
    osd_op_type_t type =
      boost::apply_visitor(pg_queueable_visitor, request.second.get_variant());

    if (osd_op_type_t::client_op != type) { 
      return -1;
    }
    return request.first.pool();
  }

  mClockPoolQueue::InnerClient
  inline mClockPoolQueue::get_inner_client(const Client& cl,
				    const Request& request) {
    return InnerClient(get_pool(request), get_osd_op_type(request));
  }

  // Formatted output of the queue
  inline void mClockPoolQueue::dump(ceph::Formatter *f) const {
    queue.dump(f);
  }

  inline void mClockPoolQueue::enqueue_strict(Client cl,
					      unsigned priority,
					      Request item) {
    queue.enqueue_strict(get_inner_client(cl, item), priority, item);
  }

  // Enqueue op in the front of the strict queue
  inline void mClockPoolQueue::enqueue_strict_front(Client cl,
						    unsigned priority,
						    Request item) {
    queue.enqueue_strict_front(get_inner_client(cl, item), priority, item);
  }

  // Enqueue op in the back of the regular queue
  inline void mClockPoolQueue::enqueue(Client cl,
				       unsigned priority,
				       unsigned cost,
				       Request item) {
    queue.enqueue(get_inner_client(cl, item), priority, cost, item,
		  item.second.get_qos_params());
  }

  // Enqueue the op in the front of the regular queue
  inline void mClockPoolQueue::enqueue_front(Client cl,
					     unsigned priority,
					     unsigned cost,
					     Request item) {
    queue.enqueue_front(get_inner_client(cl, item), priority, cost, item);
  }

  // Return an op to be dispatched
  inline Request mClockPoolQueue::dequeue() {
    std::pair<Request, dmc::PhaseType> retn = queue._dequeue();

    if (!retn.first.second.is_future()) {
      if (boost::optional<OpRequestRef> _op = retn.first.second.maybe_get_op()) {
	(*_op)->qos_resp = retn.second;
      }
    }
    return retn.first;
  }
} // namespace ceph
