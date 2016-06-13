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

#include "common/config.h"
#include "common/ceph_context.h"

#include "mClockPriorityQueue.h"


namespace ceph {

  namespace dmc = crimson::dmclock;

  enum class osd_op_type_t {
    client, osd_subop, bg_snaptrim, bg_recovery, bg_scrub };

  struct mclock_op_tags_t {
    const double client_op_res;
    const double client_op_wgt;
    const double client_op_lim;

    const double osd_subop_res;
    const double osd_subop_wgt;
    const double osd_subop_lim;

    const double snap_res;
    const double snap_wgt;
    const double snap_lim;

    const double recov_res;
    const double recov_wgt;
    const double recov_lim;

    const double scrub_res;
    const double scrub_wgt;
    const double scrub_lim;

    mclock_op_tags_t(CephContext *cct) :
      client_op_res(cct->_conf->osd_op_queue_mclock_client_op_res),
      client_op_wgt(cct->_conf->osd_op_queue_mclock_client_op_wgt),
      client_op_lim(cct->_conf->osd_op_queue_mclock_client_op_lim),

      osd_subop_res(cct->_conf->osd_op_queue_mclock_osd_subop_res),
      osd_subop_wgt(cct->_conf->osd_op_queue_mclock_osd_subop_wgt),
      osd_subop_lim(cct->_conf->osd_op_queue_mclock_osd_subop_lim),

      snap_res(cct->_conf->osd_op_queue_mclock_snap_res),
      snap_wgt(cct->_conf->osd_op_queue_mclock_snap_wgt),
      snap_lim(cct->_conf->osd_op_queue_mclock_snap_lim),

      recov_res(cct->_conf->osd_op_queue_mclock_recov_res),
      recov_wgt(cct->_conf->osd_op_queue_mclock_recov_wgt),
      recov_lim(cct->_conf->osd_op_queue_mclock_recov_lim),

      scrub_res(cct->_conf->osd_op_queue_mclock_scrub_res),
      scrub_wgt(cct->_conf->osd_op_queue_mclock_scrub_wgt),
      scrub_lim(cct->_conf->osd_op_queue_mclock_scrub_lim)
    {
      // empty
    }
  };


  extern std::unique_ptr<mclock_op_tags_t> mclock_op_tags;

  dmc::ClientInfo op_class_client_info_f(const osd_op_type_t& op_type);


#if 0
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
  template<typename T, typename K>
  class mClockOpClassQueue : public OpQueue<T, K> {
    using queue_t = mClockQueue<T, osd_op_type_t>;

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
      std::function<bool (T)> f) override final {
      // FIX THIS
    }


    // Ops of this priority should be deleted immediately
    inline void remove_by_class(K k, std::list<T> *out) override final {
      // FIX THIS
    }


    inline void enqueue_strict(K cl, unsigned priority, T item) override final {
      // TODO FIX
      osd_op_type_t op_type = osd_op_type_t::client;
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
    }


    // Don't leak resources on destruction
  }; // class mClockOpClassQueue
  
} // namespace ceph
