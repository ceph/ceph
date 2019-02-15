// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MMDSLoadTargets_H
#define CEPH_MMDSLoadTargets_H

#include "msg/Message.h"
#include "mds/mdstypes.h"
#include "messages/PaxosServiceMessage.h"
#include "include/types.h"

#include <map>
using std::map;

class MMDSLoadTargets : public MessageInstance<MMDSLoadTargets, PaxosServiceMessage> {
public:
  friend factory;

  mds_gid_t global_id;
  set<mds_rank_t> targets;

protected:
  MMDSLoadTargets() : MessageInstance(MSG_MDS_OFFLOAD_TARGETS, 0) {}
  MMDSLoadTargets(mds_gid_t g, set<mds_rank_t>& mds_targets) :
    MessageInstance(MSG_MDS_OFFLOAD_TARGETS, 0),
    global_id(g), targets(mds_targets) {}
  ~MMDSLoadTargets() override {}

public:
  std::string_view get_type_name() const override { return "mds_load_targets"; }
  void print(ostream& o) const override {
    o << "mds_load_targets(" << global_id << " " << targets << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(global_id, p);
    decode(targets, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(global_id, payload);
    encode(targets, payload);
  }
};

#endif
