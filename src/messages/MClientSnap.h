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

#ifndef __MCLIENTSNAP_H
#define __MCLIENTSNAP_H

#include "msg/Message.h"

struct MClientSnap : public Message {

  static const char *get_opname(int o) {
    switch (o) {
    case CEPH_SNAP_OP_UPDATE: return "update";
    case CEPH_SNAP_OP_SPLIT: return "split";
    default: return "???";
    }
  }
  
  __u32 op;
  inodeno_t realm;

  // new snap state
  snapid_t snap_created, snap_highwater;
  vector<snapid_t> snaps;

  // (for split only)
  inodeno_t split_parent;
  list<inodeno_t> split_inos;
  
  MClientSnap() : Message(CEPH_MSG_CLIENT_SNAP) {}
  MClientSnap(int o, inodeno_t r) : 
    Message(CEPH_MSG_CLIENT_SNAP),
    op(o), realm(r),
    split_parent(0) {} 
  
  const char *get_type_name() { return "Csnap"; }
  void print(ostream& out) {
    out << "client_snap(" << get_opname(op) << " " << realm
	<< " " << snaps;
    if (split_parent)
      out << " split_parent=" << split_parent;
    out << ")";
  }

  void encode_payload() {
    ::encode(op, payload);
    ::encode(realm, payload);
    ::encode(snap_highwater, payload);
    ::encode(snaps, payload);
    ::encode(split_parent, payload);
    ::encode(split_inos, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(realm, p);
    ::decode(snap_highwater, p);
    ::decode(snaps, p);
    ::decode(split_parent, p);
    ::decode(split_inos, p);
    assert(p.end());
  }

};

#endif
