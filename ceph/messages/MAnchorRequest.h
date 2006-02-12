// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(op), (char*)&op);
	off += sizeof(op);
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	int n;
	s.copy(off, sizeof(int), (char*)&n);
	off += sizeof(int);
	for (int i=0; i<n; i++) {
	  Anchor *a = new Anchor;
	  a->_unrope(s, off);
	  trace.push_back(a);
	}
  }

  virtual void encode_payload(crope& r) {
	r.append((char*)&op, sizeof(op));
	r.append((char*)&ino, sizeof(ino));
	int n = trace.size();
	r.append((char*)&n, sizeof(int));
	for (int i=0; i<n; i++) 
	  trace[i]->_rope(r);
  }
};

#endif
