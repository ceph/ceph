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

#ifndef MOSDPGPUSHREPLY_H
#define MOSDPGPUSHREPLY_H

#include "MOSDFastDispatchOp.h"

class MOSDPGPushReply : public MessageInstance<MOSDPGPushReply, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 2;

public:
  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  vector<PushReplyOp> replies;
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

  MOSDPGPushReply()
    : MessageInstance(MSG_OSD_PG_PUSH_REPLY, HEAD_VERSION, COMPAT_VERSION),
      cost(0)
    {}

  void compute_cost(CephContext *cct) {
    cost = 0;
    for (vector<PushReplyOp>::iterator i = replies.begin();
	 i != replies.end();
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
    decode(replies, p);
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
    encode(replies, payload);
    encode(cost, payload);
    encode(pgid.shard, payload);
    encode(from, payload);
    encode(min_epoch, payload);
  }

  void print(ostream& out) const override {
    out << "MOSDPGPushReply(" << pgid
	<< " " << map_epoch << "/" << min_epoch
	<< " " << replies;
    out << ")";
  }

  std::string_view get_type_name() const override { return "MOSDPGPushReply"; }
};

#endif
