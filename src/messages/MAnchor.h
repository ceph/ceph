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


#ifndef __MANCHORREQUEST_H
#define __MANCHORREQUEST_H

#include <vector>

#include "msg/Message.h"
#include "mds/Anchor.h"


class MAnchor : public Message {
  int op;
  inodeno_t ino;
  vector<Anchor> trace;
  version_t atid;  // anchor table version.

 public:
  MAnchor() {}
  MAnchor(int o, inodeno_t i, version_t v=0) : 
    Message(MSG_MDS_ANCHOR),
    op(o), ino(i), atid(v) { }
  
  virtual const char *get_type_name() { return "anchor"; }
  void print(ostream& o) {
    o << "anchor(" << get_anchor_opname(op);
    if (ino) o << " " << ino;
    if (atid) o << " atid " << atid;
    if (!trace.empty()) o << ' ' << trace;
    o << ")";
  }

  void set_trace(vector<Anchor>& trace) { 
    this->trace = trace; 
  }

  int get_op() { return op; }
  inodeno_t get_ino() { return ino; }
  vector<Anchor>& get_trace() { return trace; }
  version_t get_atid() { return atid; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(atid), (char*)&atid);
    off += sizeof(atid);
    ::_decode(trace, payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&op, sizeof(op));
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&atid, sizeof(atid));
    ::_encode(trace, payload);
  }
};

#endif
