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

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGPush : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;


public:
  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch;
  vector<PushOp> pushes;
  uint64_t cost;

  void compute_cost(CephContext *cct) {
    cost = 0;
    for (vector<PushOp>::iterator i = pushes.begin();
	 i != pushes.end();
	 ++i) {
      cost += i->cost(cct);
    }
  }

  int get_cost() const {
    return cost;
  }

  MOSDPGPush() :
    Message(MSG_OSD_PG_PUSH, HEAD_VERSION, COMPAT_VERSION),
    cost(0)
    {}

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid.pgid, p);
    ::decode(map_epoch, p);
    ::decode(pushes, p);
    ::decode(cost, p);
    if (header.version >= 2) {
      ::decode(pgid.shard, p);
      ::decode(from, p);
    } else {
      pgid.shard = shard_id_t::NO_SHARD;
      from = pg_shard_t(get_source().num(), shard_id_t::NO_SHARD);
    }
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(pgid.pgid, payload);
    ::encode(map_epoch, payload);
    ::encode(pushes, payload);
    ::encode(cost, payload);
    ::encode(pgid.shard, payload);
    ::encode(from, payload);
  }

  const char *get_type_name() const { return "MOSDPGPush"; }

  void print(ostream& out) const {
    out << "MOSDPGPush(" << pgid
	<< " " << map_epoch
	<< " " << pushes;
    out << ")";
  }
};

#endif
