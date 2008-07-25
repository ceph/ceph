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
  __u32 op;
  bufferlist bl;

  // (for split only)
  inodeno_t split;
  list<inodeno_t> split_inos;
  
  MClientSnap(int o=0) : 
    Message(CEPH_MSG_CLIENT_SNAP),
    op(o), split(0) {} 
  
  const char *get_type_name() { return "client_snap"; }
  void print(ostream& out) {
    out << "client_snap(" << ceph_snap_op_name(op);
    if (split)
      out << " split=" << split;
    out << ")";
  }

  void encode_payload() {
    ::encode(op, payload);
    ::encode(bl, payload);
    ::encode(split, payload);
    ::encode(split_inos, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(bl, p);
    ::decode(split, p);
    ::decode(split_inos, p);
    assert(p.end());
  }

};

#endif
