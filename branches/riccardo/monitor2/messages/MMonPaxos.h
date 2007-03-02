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
  const static int OP_COLLECT = 1;   // proposer: propose round
  const static int OP_LAST = 2;		 // voter:    accept proposed round
  const static int OP_BEGIN = 4;	 // proposer: value proposed for this round
  const static int OP_ACCEPT = 5;	 // voter:    accept propsed value
  const static int OP_COMMIT = 7;   // proposer: notify learners of agreed value

  // which state machine?
  int op;   
  int machine_id;
  
  version_t last_committed;  // i've committed to
  version_t pn_from;         // i promise to accept after
  version_t pn;              // with with proposal
  map<version_t,bufferlist> values;
  version_t old_accepted_pn;     // previous pn, if we are a LAST with an uncommitted value

  MMonPaxos() : Message(MSG_MON_PAXOS) {}
  MMonPaxos(int o, int mid) : Message(MSG_MON_PAXOS),
			      op(o), machine_id(mid) {}
  
  virtual char *get_type_name() { return "paxos"; }
  
  void print(ostream& out) {
    out << "paxos(op " << op
	<< ", machine " << machine_id << ")";
  }

  void encode_payload() {
    payload.append((char*)&op, sizeof(op));
    payload.append((char*)&machine_id, sizeof(machine_id));
    payload.append((char*)&last_committed, sizeof(last_committed));
    payload.append((char*)&old_accepted_pn, sizeof(old_accepted_pn));
    ::_encode(values, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    payload.copy(off, sizeof(machine_id), (char*)&machine_id);
    off += sizeof(machine_id);
    payload.copy(off, sizeof(last_committed), (char*)&last_committed);
    off += sizeof(last_committed);
    payload.copy(off, sizeof(old_accepted_pn), (char*)&old_accepted_pn);
    off += sizeof(old_accepted_pn);
    ::_decode(values, payload, off);
  }
};

#endif
