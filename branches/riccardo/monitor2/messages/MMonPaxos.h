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


#ifndef __MMONPAXOS_H
#define __MMONPAXOS_H

#include "msg/Message.h"

class MMonPaxos : public Message {
 public:
  // op types
  const static int OP_PROPOSE = 1;
  const static int OP_LAST = 2;
  const static int OP_ACCEPT = 3;
  const static int OP_COMMIT = 4;
  // ..
  // todo rf .. fill these in!

  int op;   
  int machine_id;
  version_t proposal;
  version_t n;
  bufferlist value;

  MMonPaxos() : Message(MSG_MON_PAXOS) {}
  MMonPaxos(int o, int mid, 
	    version_t pn, version_t v) : Message(MSG_MON_PAXOS),
					     op(o), machine_id(mid), 
					     proposal(pn), n(v) {}
  MMonPaxos(int o, int mid, 
	    version_t pn, version_t v, 
	    bufferlist& b) : Message(MSG_MON_PAXOS),
			     op(o), machine_id(mid),
			     proposal(pn), n(v), 
			     value(b) {}
  
  virtual char *get_type_name() { return "paxos"; }
  
  void print(ostream& out) {
    out << "paxos(op " << op
	<< ", machine " << machine_id
	<< ", proposal " << proposal 
	<< ", state " << n 
	<< ", " << value.length() << " bytes)";
  }

  void encode_payload() {
    payload.append((char*)&op, sizeof(op));
    payload.append((char*)&machine_id, sizeof(machine_id));
    payload.append((char*)&proposal, sizeof(proposal));
    payload.append((char*)&n, sizeof(n));
    ::_encode(value, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    payload.copy(off, sizeof(machine_id), (char*)&machine_id);
    off += sizeof(machine_id);
    payload.copy(off, sizeof(proposal), (char*)&proposal);
    off += sizeof(proposal);
    payload.copy(off, sizeof(n), (char*)&n);
    off += sizeof(n);
    ::_decode(value, payload, off);
  }
};

#endif
