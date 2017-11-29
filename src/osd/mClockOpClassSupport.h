// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <bitset>

#include "dmclock/src/dmclock_server.h"
#include "osd/OpRequest.h"
#include "osd/OpQueueItem.h"


namespace ceph {
  namespace mclock {

    using op_item_type_t = OpQueueItem::OpQueueable::op_type_t;
    
    enum class osd_op_type_t {
      client_op, osd_rep_op, bg_snaptrim, bg_recovery, bg_scrub
    };

    class OpClassClientInfoMgr {
      crimson::dmclock::ClientInfo client_op;
      crimson::dmclock::ClientInfo osd_rep_op;
      crimson::dmclock::ClientInfo snaptrim;
      crimson::dmclock::ClientInfo recov;
      crimson::dmclock::ClientInfo scrub;

      static constexpr std::size_t rep_op_msg_bitset_size = 128;
      std::bitset<rep_op_msg_bitset_size> rep_op_msg_bitset;
      void add_rep_op_msg(int message_code);

    public:

      OpClassClientInfoMgr(CephContext *cct);

      double osd_op_queue_mclock_client_op_res;
      double osd_op_queue_mclock_client_op_wgt;
      double osd_op_queue_mclock_client_op_lim;
      double osd_op_queue_mclock_osd_rep_op_res;
      double osd_op_queue_mclock_osd_rep_op_wgt;
      double osd_op_queue_mclock_osd_rep_op_lim;
      double osd_op_queue_mclock_snap_res;
      double osd_op_queue_mclock_snap_wgt;
      double osd_op_queue_mclock_snap_lim;
      double osd_op_queue_mclock_recov_res;
      double osd_op_queue_mclock_recov_wgt;
      double osd_op_queue_mclock_recov_lim;
      double osd_op_queue_mclock_scrub_res;
      double osd_op_queue_mclock_scrub_wgt;
      double osd_op_queue_mclock_scrub_lim;

      inline const crimson::dmclock::ClientInfo*
      get_client_info(osd_op_type_t type) {
	switch(type) {
	case osd_op_type_t::client_op:
	  return &client_op;
	case osd_op_type_t::osd_rep_op:
	  return &osd_rep_op;
	case osd_op_type_t::bg_snaptrim:
	  return &snaptrim;
	case osd_op_type_t::bg_recovery:
	  return &recov;
	case osd_op_type_t::bg_scrub:
	  return &scrub;
	default:
	  ceph_abort();
	  return nullptr;
	}
      }

      // converts operation type from op queue internal to mclock
      // equivalent
      inline static osd_op_type_t convert_op_type(op_item_type_t t) {
	switch(t) {
	case op_item_type_t::client_op:
	  return osd_op_type_t::client_op;
	case op_item_type_t::bg_snaptrim:
	  return osd_op_type_t::bg_snaptrim;
	case op_item_type_t::bg_recovery:
	  return osd_op_type_t::bg_recovery;
	case op_item_type_t::bg_scrub:
	  return osd_op_type_t::bg_scrub;
	default:
	  ceph_abort();
	}
      }

      osd_op_type_t osd_op_type(const OpQueueItem&) const;

      // used for debugging since faster implementation can be done
      // with rep_op_msg_bitmap
      static bool is_rep_op(uint16_t);
    }; // OpClassClientInfoMgr
  } // namespace mclock
} // namespace ceph
