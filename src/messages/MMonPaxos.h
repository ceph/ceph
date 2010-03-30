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

#include "messages/PaxosServiceMessage.h"
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
  __s32 op;          // paxos op
  __s32 machine_id;  // which state machine?

  version_t first_committed;  // i've committed to
  version_t last_committed;  // i've committed to
  version_t pn_from;         // i promise to accept after
  version_t pn;              // with with proposal
  version_t uncommitted_pn;     // previous pn, if we are a LAST with an uncommitted value
  utime_t lease_expire;

  version_t latest_version;
  bufferlist latest_value;

  map<version_t,bufferlist> values;

  MMonPaxos() : Message(MSG_MON_PAXOS) {}
  MMonPaxos(epoch_t e, int o, int mid) : 
    Message(MSG_MON_PAXOS),
    epoch(e),
    op(o), machine_id(mid),
    first_committed(0), last_committed(0), pn_from(0), pn(0), uncommitted_pn(0),
    latest_version(0) { }

private:
  ~MMonPaxos() {}

public:  
  const char *get_type_name() { return "paxos"; }
  
  void print(ostream& out) {
    out << "paxos(" << get_paxos_name(machine_id)
	<< " " << get_opname(op) 
	<< " lc " << last_committed
	<< " fc " << first_committed
	<< " pn " << pn << " opn " << uncommitted_pn;
    if (latest_version)
      out << " latest " << latest_version << " (" << latest_value.length() << " bytes)";
    out <<  ")";
  }

  void encode_payload() {
    ::encode(epoch, payload);
    ::encode(op, payload);
    ::encode(machine_id, payload);
    ::encode(first_committed, payload);
    ::encode(last_committed, payload);
    ::encode(pn_from, payload);
    ::encode(pn, payload);
    ::encode(uncommitted_pn, payload);
    ::encode(lease_expire, payload);
    ::encode(latest_version, payload);
    ::encode(latest_value, payload);
    ::encode(values, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(op, p);
    ::decode(machine_id, p);
    ::decode(first_committed, p);
    ::decode(last_committed, p);
    ::decode(pn_from, p);   
    ::decode(pn, p);   
    ::decode(uncommitted_pn, p);
    ::decode(lease_expire, p);
    ::decode(latest_version, p);
    ::decode(latest_value, p);
    ::decode(values, p);
  }
};

#endif
