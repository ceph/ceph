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
  
  bufferlist bl;

  // (for split only)
  inodeno_t split;
  list<inodeno_t> split_inos;
  
  MClientSnap() : 
    Message(CEPH_MSG_CLIENT_SNAP),
    split(0) {} 
  
  const char *get_type_name() { return "Csnap"; }
  void print(ostream& out) {
    out << "client_snap(";
    if (split)
      out << "split=" << split;
    out << ")";
  }

  void encode_payload() {
    ::encode(bl, payload);
    ::encode(split, payload);
    ::encode(split_inos, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(bl, p);
    ::decode(split, p);
    ::decode(split_inos, p);
    assert(p.end());
  }

};

#endif
