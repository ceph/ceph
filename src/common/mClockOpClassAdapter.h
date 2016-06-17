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


#pragma once

#include "boost/variant.hpp"

#include "common/config.h"
#include "common/ceph_context.h"
#include "osd/PGQueueable.h"

#include "mClockPriorityQueue.h"


namespace ceph {

  namespace dmc = crimson::dmclock;

  using Request = std::pair<PGRef, PGQueueable>;
  using Client = entity_inst_t;

  using T = Request;
  using K = Client;

  enum class osd_op_type_t {
    client_op, osd_subop, bg_snaptrim, bg_recovery, bg_scrub };

  struct mclock_op_tags_t {
    dmc::ClientInfo client_op;
    dmc::ClientInfo osd_subop;
    dmc::ClientInfo snaptrim;
    dmc::ClientInfo recov;
    dmc::ClientInfo scrub;

    mclock_op_tags_t(CephContext *cct);
  };


  extern std::unique_ptr<mclock_op_tags_t> mclock_op_tags;

  dmc::ClientInfo op_class_client_info_f(const osd_op_type_t& op_type);


#if 0 // USE?
  // turn cost, which can range from 1000 to 50 * 2^20
  double cost_to_tag(unsigned cost) {
    static const double log_of_2 = std::log(2.0);
    return cost_factor * std::log(cost) / log_of_2;
  }
#endif


  // This class exists to bridge the ceph code, which treats the class
  // as the client, and the queue, where the class is
  // osd_op_type_t. So this adpater class will transform calls
  // appropriately.
  class mClockOpClassQueue : public OpQueue<Request, Client> {
    using queue_t = mClockQueue<Request, osd_op_type_t>;

    queue_t queue;

    double cost_factor;

  public:

    mClockOpClassQueue(CephContext *cct) :
      queue(&op_class_client_info_f),
      cost_factor(cct->_conf->osd_op_queue_mclock_cost_factor)
    {
      // manage the singleton
      if (!mclock_op_tags) {
	mclock_op_tags.reset(new mclock_op_tags_t(cct));
      }
    }


    inline unsigned length() const override final {
      return queue.length();
    }


    // Ops will be removed f evaluates to true, f may have sideeffects
    inline void remove_by_filter(std::function<bool(Request)> f) override final {
      queue.remove_by_filter(f);
    }


    // Ops of this priority should be deleted immediately
    inline void remove_by_class(Client cl,
				std::list<Request> *out) override final {
      queue.remove_by_filter(
	[&] (const Request& r) ->bool { return cl == r.second.get_owner(); },
	out);
    }


    inline void enqueue_strict(Client cl,
			       unsigned priority,
			       Request item) override final {
      queue.enqueue_strict(get_osd_op_type(item), 0, item);
    }


    // Enqueue op in the front of the strict queue
    inline void enqueue_strict_front(Client cl,
				     unsigned priority,
				     Request item) override final {
      queue.enqueue_strict_front(get_osd_op_type(item), priority, item);
    }


    // Enqueue op in the back of the regular queue
    inline void enqueue(Client cl,
			unsigned priority,
			unsigned cost,
			Request item) override final {
      queue.enqueue(get_osd_op_type(item), priority, cost, item);
    }

    // Enqueue the op in the front of the regular queue
    inline void enqueue_front(Client cl,
			      unsigned priority,
			      unsigned cost,
			      Request item) override final {
      queue.enqueue_front(get_osd_op_type(item), priority, cost, item);
    }

    // Returns if the queue is empty
    inline bool empty() const override final {
      return queue.empty();
    }


    // Return an op to be dispatch
    inline Request dequeue() override final {
      return queue.dequeue();
    }


    // Formatted output of the queue
    inline void dump(ceph::Formatter *f) const override final {
      queue.dump(f);
    }

  protected:

    class pg_queueable_visitor_t : public boost::static_visitor<osd_op_type_t> {
    public:
      osd_op_type_t operator()(const OpRequestRef& o) const {
	// don't know if it's a client_op or a
        return osd_op_type_t::client_op;
      }

      osd_op_type_t operator()(const PGSnapTrim& o) const {
        return osd_op_type_t::bg_snaptrim;
      }

      osd_op_type_t operator()(const PGScrub& o) const {
        return osd_op_type_t::bg_scrub;
      }

      osd_op_type_t operator()(const PGRecovery& o) const {
        return osd_op_type_t::bg_recovery;
      }
    }; // class pg_queueable_visitor_t

    pg_queueable_visitor_t pg_queueable_visitor;

    osd_op_type_t get_osd_op_type(const Request& request) {
      osd_op_type_t type =
	boost::apply_visitor(pg_queueable_visitor, request.second.get_variant());

      // if we got client_op back then we need to distinguish between
      // a client op and an osd subop.
      
      if (osd_op_type_t::client_op != type) {
	return type;
      } else if (MSG_OSD_SUBOP ==
		 boost::get<OpRequestRef>(
		   request.second.get_variant())->get_req()->get_header().type) {
	return osd_op_type_t::osd_subop;
      } else {
	return osd_op_type_t::client_op;
      }
    }
  }; // class mClockOpClassQueue

} // namespace ceph
