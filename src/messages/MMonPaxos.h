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


#ifndef __MMONPAXOS_H
#define __MMONPAXOS_H

#include "msg/Message.h"
#include "mon/mon_types.h"

class MMonPaxos : public Message {
 public:
  // op types
  const static int OP_COLLECT =   1; // proposer: propose round
  const static int OP_LAST =      2; // voter:    accept proposed round
  const static int OP_BEGIN =     3; // proposer: value proposed for this round
  const static int OP_ACCEPT =    4; // voter:    accept propsed value
  const static int OP_COMMIT =    5; // proposer: notify learners of agreed value
  const static int OP_LEASE =     6; // leader: extend peon lease
  const static int OP_LEASE_ACK = 7; // peon: lease ack
  const static char *get_opname(int op) {
    switch (op) {
    case OP_COLLECT: return "collect";
    case OP_LAST: return "last";
    case OP_BEGIN: return "begin";
    case OP_ACCEPT: return "accept";
    case OP_COMMIT: return "commit";
    case OP_LEASE: return "lease";
    case OP_LEASE_ACK: return "lease_ack";
    default: assert(0); return 0;
    }
  }

  epoch_t epoch;   // monitor epoch
  int op;          // paxos op
  int machine_id;  // which state machine?

  version_t last_committed;  // i've committed to
  version_t pn_from;         // i promise to accept after
  version_t pn;              // with with proposal
  version_t uncommitted_pn;     // previous pn, if we are a LAST with an uncommitted value
  utime_t lease_expire;

  map<version_t,bufferlist> values;

  MMonPaxos() : Message(MSG_MON_PAXOS) {}
  MMonPaxos(epoch_t e, int o, int mid) : 
    Message(MSG_MON_PAXOS),
    epoch(e),
    op(o), machine_id(mid),
    last_committed(0), pn_from(0), pn(0), uncommitted_pn(0) { }
  
  const char *get_type_name() { return "paxos"; }
  
  void print(ostream& out) {
    out << "paxos(" << get_paxos_name(machine_id)
	<< " " << get_opname(op) << " lc " << last_committed
	<< " pn " << pn << " opn " << uncommitted_pn
	<< ")";
  }

  void encode_payload() {
    ::_encode(epoch, payload);
    ::_encode(op, payload);
    ::_encode(machine_id, payload);
    ::_encode(last_committed, payload);
    ::_encode(pn_from, payload);
    ::_encode(pn, payload);
    ::_encode(uncommitted_pn, payload);
    ::_encode(lease_expire, payload);
    ::_encode(values, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(epoch, payload, off);
    ::_decode(op, payload, off);
    ::_decode(machine_id, payload, off);
    ::_decode(last_committed, payload, off);
    ::_decode(pn_from, payload, off);   
    ::_decode(pn, payload, off);   
    ::_decode(uncommitted_pn, payload, off);
    ::_decode(lease_expire, payload, off);
    ::_decode(values, payload, off);
  }
};

#endif
