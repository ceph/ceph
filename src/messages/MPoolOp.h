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

#ifndef __MPOOLOP_H
#define __MPOOLOP_H

#include "messages/PaxosServiceMessage.h"


enum {
  POOL_OP_CREATE,
  POOL_OP_DELETE,
  POOL_OP_CREATE_SNAP,
  POOL_OP_DELETE_SNAP,
  POOL_OP_CREATE_UNMANAGED_SNAP,
  POOL_OP_DELETE_UNMANAGED_SNAP,
  POOL_OP_AUID_CHANGE
};

static const char *get_pool_op_name(int op) {
  switch (op) {
  case POOL_OP_CREATE:
    return "create pool";
  case POOL_OP_DELETE:
    return "delete pool";
  case POOL_OP_CREATE_SNAP:
    return "create snap";
  case POOL_OP_DELETE_SNAP:
    return "delete snap";
  case POOL_OP_AUID_CHANGE:
    return "change auid";
  default:
    return "unknown";
  }
}

class MPoolOp : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  int pool;
  string name;
  int op;
  __u64 auid;
  snapid_t snapid;

  MPoolOp() : PaxosServiceMessage(MSG_POOLOP, 0) {}
  MPoolOp(const ceph_fsid_t& f, tid_t t, int p, string& n, int o, version_t v) :
    PaxosServiceMessage(MSG_POOLOP, v), fsid(f), pool(p), name(n), op(o),
    auid(0), snapid(0) {
    set_tid(t);
  }
  MPoolOp(const ceph_fsid_t& f, tid_t t, int p, string& n,
	  int o, __u64 uid, version_t v) :
    PaxosServiceMessage(MSG_POOLOP, v), fsid(f), pool(p), name(n), op(o),
    auid(uid), snapid(0) {
    set_tid(t);
  }

private:
  ~MPoolOp() {}

public:
  const char *get_type_name() { return "poolop"; }
  void print(ostream& out) {
    out << "poolop(" << get_pool_op_name(op) << ",pool " << pool
	<< ",auid " << auid
	<< ", tid" << get_tid() << " " << name << " v" << version << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(pool, payload);
    ::encode(name, payload);
    ::encode(op, payload);
    ::encode(auid, payload);
    ::encode(snapid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(pool, p);
    ::decode(name, p);
    ::decode(op, p);
    ::decode(auid, p);
    ::decode(snapid, p);
  }
};

#endif
