// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MPOOLOP_H
#define CEPH_MPOOLOP_H

#include "messages/PaxosServiceMessage.h"


class MPoolOp : public MessageInstance<MPoolOp, PaxosServiceMessage> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 2;

public:
  uuid_d fsid;
  __u32 pool = 0;
  string name;
  __u32 op = 0;
  snapid_t snapid;
  __s16 crush_rule = 0;

  MPoolOp()
    : MessageInstance(CEPH_MSG_POOLOP, 0, HEAD_VERSION, COMPAT_VERSION) { }
  MPoolOp(const uuid_d& f, ceph_tid_t t, int p, string& n, int o, version_t v)
    : MessageInstance(CEPH_MSG_POOLOP, v, HEAD_VERSION, COMPAT_VERSION),
      fsid(f), pool(p), name(n), op(o),
      snapid(0), crush_rule(0) {
    set_tid(t);
  }

private:
  ~MPoolOp() override {}

public:
  std::string_view get_type_name() const override { return "poolop"; }
  void print(ostream& out) const override {
    out << "pool_op(" << ceph_pool_op_name(op) << " pool " << pool
	<< " tid " << get_tid()
	<< " name " << name
	<< " v" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(pool, payload);
    encode(op, payload);
    encode((uint64_t)0, payload);
    encode(snapid, payload);
    encode(name, payload);
    __u8 pad = 0;
    encode(pad, payload);  /* for v3->v4 encoding change */
    encode(crush_rule, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(pool, p);
    if (header.version < 2)
      decode(name, p);
    decode(op, p);
    uint64_t old_auid;
    decode(old_auid, p);
    decode(snapid, p);
    if (header.version >= 2)
      decode(name, p);

    if (header.version >= 3) {
      __u8 pad;
      decode(pad, p);
      if (header.version >= 4)
	decode(crush_rule, p);
      else
	crush_rule = pad;
    } else
      crush_rule = -1;
  }
};

#endif
