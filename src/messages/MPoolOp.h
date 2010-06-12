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


class MPoolOp : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  __u32 pool;
  string name;
  __u32 op;
  uint64_t auid;
  snapid_t snapid;
  __u8 crush_rule;

  MPoolOp() : PaxosServiceMessage(CEPH_MSG_POOLOP, 0) {}
  MPoolOp(const ceph_fsid_t& f, tid_t t, int p, string& n, int o, version_t v) :
    PaxosServiceMessage(CEPH_MSG_POOLOP, v), fsid(f), pool(p), name(n), op(o),
    auid(0), snapid(0), crush_rule(0) {
    set_tid(t);
  }
  MPoolOp(const ceph_fsid_t& f, tid_t t, int p, string& n,
	  int o, uint64_t uid, version_t v) :
    PaxosServiceMessage(CEPH_MSG_POOLOP, v), fsid(f), pool(p), name(n), op(o),
    auid(uid), snapid(0), crush_rule(0) {
    set_tid(t);
  }

private:
  ~MPoolOp() {}

public:
  const char *get_type_name() { return "poolop"; }
  void print(ostream& out) {
    out << "poolop(" << ceph_pool_op_name(op) << ",pool " << pool
	<< ",auid " << auid
	<< ", tid" << get_tid() << " " << name << " v" << version << ")";
  }

  void encode_payload() {
    if (crush_rule)
      header.version = 3;
    else header.version = 2;
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(pool, payload);
    ::encode(op, payload);
    ::encode(auid, payload);
    ::encode(snapid, payload);
    ::encode(name, payload);
    ::encode(crush_rule, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(pool, p);
    if (header.version < 2)
      ::decode(name, p);
    ::decode(op, p);
    ::decode(auid, p);
    ::decode(snapid, p);
    if (header.version >= 2)
      ::decode(name, p);
    if (header.version >= 3)
      ::decode(crush_rule, p);
    else crush_rule = 0;
  }
};

#endif
