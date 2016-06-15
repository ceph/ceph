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
    inline void remove_by_filter(
      std::function<bool (Request)> f) override final {
      return queue.remove_by_filter(f);
    }


    // Ops of this priority should be deleted immediately
    inline void remove_by_class(K k, std::list<T> *out) override final {
      // FIX THIS
    }


    inline void enqueue_strict(K cl, unsigned priority, T item) override final {
      // TODO FIX
      osd_op_type_t op_type = osd_op_type_t::client_op;
      queue.enqueue_strict(op_type, 0, item);
    }


    // Enqueue op in the front of the strict queue
    inline void enqueue_strict_front(K cl, unsigned priority, T item) override final {
      // FIX THIS
    }


    // Enqueue op in the back of the regular queue
    inline void enqueue(K cl, unsigned priority, unsigned cost, T item) override final {
      // FIX THIS
    }


    // Enqueue the op in the front of the regular queue
    inline void enqueue_front(K cl, unsigned priority, unsigned cost, T item) override final {
      // FIX THIS
    }


    // Returns if the queue is empty
    inline bool empty() const override final {
      return queue.empty();
    }


    // Return an op to be dispatch
    inline T dequeue() override final {
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

    osd_op_type_t get_osd_op_type(const T& request) {
      osd_op_type_t type =
	boost::apply_visitor(pg_queueable_visitor, request.second.get_variant());

      if (osd_op_type_t::client_op == type) {
	// TODO must distinguish between client_op and osd_subop
      }

      return type;
    }
  }; // class mClockOpClassQueue

} // namespace ceph
