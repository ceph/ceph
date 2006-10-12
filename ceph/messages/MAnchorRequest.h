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

#define ANCHOR_OP_CREATE   1
#define ANCHOR_OP_DESTROY  2
#define ANCHOR_OP_LOOKUP   3
#define ANCHOR_OP_UPDATE   4

class MAnchorRequest : public Message {
  int op;
  inodeno_t ino;
  vector<Anchor*> trace;

 public:
  MAnchorRequest() {}
  MAnchorRequest(int op, inodeno_t ino) : Message(MSG_MDS_ANCHORREQUEST) {
    this->op = op;
    this->ino = ino;
  }
  ~MAnchorRequest() {
    for (unsigned i=0; i<trace.size(); i++) delete trace[i];
  }
  virtual char *get_type_name() { return "areq"; }

  void set_trace(vector<Anchor*>& trace) { this->trace = trace; }

  int get_op() { return op; }
  inodeno_t get_ino() { return ino; }
  vector<Anchor*>& get_trace() { return trace; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    int n;
    payload.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      Anchor *a = new Anchor;
      a->_decode(payload, off);
      trace.push_back(a);
    }
  }

  virtual void encode_payload() {
    payload.append((char*)&op, sizeof(op));
    payload.append((char*)&ino, sizeof(ino));
    int n = trace.size();
    payload.append((char*)&n, sizeof(int));
    for (int i=0; i<n; i++) 
      trace[i]->_encode(payload);
  }
};

#endif
