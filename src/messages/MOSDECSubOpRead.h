// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MOSDECSUBOPREAD_H
#define MOSDECSUBOPREAD_H

#include "MOSDFastDispatchOp.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpRead : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  ECSubRead op;
  uint64_t cost = 0;

  /**
    * Calculate the cost of the SubOp read operation for mClock scheduler.
    *
    * @param CephContext*
    * @param pair<int, int>&: subchunk_count and subchunk_size
    */
  void compute_cost(CephContext *cct, std::pair<int, int> &subchunk_info) {
    if (cct->_conf->osd_op_queue == "mclock_scheduler") {
      cost = op.cost(subchunk_info);
    }
  }
  int get_cost() const override {
    return cost;
  }
  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDECSubOpRead()
    : MOSDFastDispatchOp{MSG_OSD_EC_READ, HEAD_VERSION, COMPAT_VERSION}
    {}

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(op, p);
    if (header.version >= 3) {
      decode(min_epoch, p);
      decode_trace(p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 4) {
      decode(cost, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(op, payload, features);
    encode(min_epoch, payload);
    encode_trace(payload, features);
    if (header.version >= 4) {
      encode(cost, payload);
    }
  }

  std::string_view get_type_name() const override { return "MOSDECSubOpRead"; }

  void print(std::ostream& out) const override {
    out << "MOSDECSubOpRead(" << pgid
	<< " " << map_epoch << "/" << min_epoch
	<< " " << op;
    out << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
