// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "mds/AnchorTable.h"


class MAnchor : public Message {
  int op;
  inodeno_t ino;
  vector<Anchor> trace;

 public:
  MAnchor() {}
  MAnchor(int o, inodeno_t i) : 
	Message(MSG_MDS_ANCHOR),
	op(o), ino(i) { }

  
  virtual char *get_type_name() { return "anchor"; }
  void print(ostream& o) {
    o << "anchor(" << get_anchor_opname(op) << " " << ino;
    for (unsigned i=0; i<trace.size(); i++) {
      o << ' ' << trace[i];
    }
    o << ")";
  }

  void set_trace(vector<Anchor>& trace) { 
    this->trace = trace; 
  }

  int get_op() { return op; }
  inodeno_t get_ino() { return ino; }
  vector<Anchor>& get_trace() { return trace; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    ::_decode(trace, payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&op, sizeof(op));
    payload.append((char*)&ino, sizeof(ino));
    ::_encode(trace, payload);
    /*
    int n = trace.size();
    payload.append((char*)&n, sizeof(int));
    for (int i=0; i<n; i++) 
      trace[i]->_encode(payload);
    */
  }
};

#endif
