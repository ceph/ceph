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

#ifndef MOSDPGPUSH_H
#define MOSDPGPUSH_H

#include "MOSDFastDispatchOp.h"

class MOSDPGPush : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 2;

public:
  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  std::vector<PushOp> pushes;
  bool is_repair = false;

private:
  uint64_t cost = 0;

public:
  void compute_cost(CephContext *cct) {
    cost = 0;
    for (auto i = pushes.begin(); i != pushes.end(); ++i) {
      cost += i->cost(cct);
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

  void set_cost(uint64_t c) {
    cost = c;
  }

  MOSDPGPush()
    : MOSDFastDispatchOp{MSG_OSD_PG_PUSH, HEAD_VERSION, COMPAT_VERSION}
  {}

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid.pgid, p);
    decode(map_epoch, p);
    decode(pushes, p);
    decode(cost, p);
    decode(pgid.shard, p);
    decode(from, p);
    if (header.version >= 3) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 4) {
      decode(is_repair, p);
    } else {
      is_repair = false;
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid.pgid, payload);
    encode(map_epoch, payload);
    encode(pushes, payload, features);
    encode(cost, payload);
    encode(pgid.shard, payload);
    encode(from, payload);
    encode(min_epoch, payload);
    encode(is_repair, payload);
  }

  std::string_view get_type_name() const override { return "MOSDPGPush"; }

  void print(std::ostream& out) const override {
    out << "MOSDPGPush(" << pgid
	<< " " << map_epoch << "/" << min_epoch
	<< " " << pushes;
    out << ")";
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
