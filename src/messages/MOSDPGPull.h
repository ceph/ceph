// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef MOSDPGPULL_H
#define MOSDPGPULL_H

#include "MOSDFastDispatchOp.h"

class MOSDPGPull : public MessageInstance<MOSDPGPull, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 2;

  vector<PullOp> pulls;

public:
  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  uint64_t cost;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  void take_pulls(vector<PullOp> *outpulls) {
    outpulls->swap(pulls);
  }
  void set_pulls(vector<PullOp> *inpulls) {
    inpulls->swap(pulls);
  }

  MOSDPGPull()
    : MessageInstance(MSG_OSD_PG_PULL, HEAD_VERSION, COMPAT_VERSION),
      cost(0)
    {}

  void compute_cost(CephContext *cct) {
    cost = 0;
    for (vector<PullOp>::iterator i = pulls.begin();
	 i != pulls.end();
	 ++i) {
      cost += i->cost(cct);
    }
  }

  int get_cost() const override {
    return cost;
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pgid.pgid, p);
    decode(map_epoch, p);
    decode(pulls, p);
    decode(cost, p);
    decode(pgid.shard, p);
    decode(from, p);
    if (header.version >= 3) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid.pgid, payload);
    encode(map_epoch, payload);
    encode(pulls, payload, features);
    encode(cost, payload);
    encode(pgid.shard, payload);
    encode(from, payload);
    encode(min_epoch, payload);
  }

  std::string_view get_type_name() const override { return "MOSDPGPull"; }

  void print(ostream& out) const override {
    out << "MOSDPGPull(" << pgid
	<< " e" << map_epoch << "/" << min_epoch
	<< " cost " << cost
	<< ")";
  }
};

#endif
