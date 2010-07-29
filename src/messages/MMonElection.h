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


#ifndef CEPH_MMONELECTION_H
#define CEPH_MMONELECTION_H

#include "msg/Message.h"
#include "mon/MonMap.h"

class MMonElection : public Message {
public:
  static const int OP_PROPOSE = 1;
  static const int OP_ACK     = 2;
  static const int OP_NAK     = 3;
  static const int OP_VICTORY = 4;
  static const char *get_opname(int o) {
    switch (o) {
    case OP_PROPOSE: return "propose";
    case OP_ACK: return "ack";
    case OP_NAK: return "nak";
    case OP_VICTORY: return "victory";
    default: assert(0); return 0;
    }
  }
  
  int32_t op;
  epoch_t epoch;
  bufferlist monmap_bl;
  set<int> quorum;
  
  MMonElection() : Message(MSG_MON_ELECTION) {}
  MMonElection(int o, epoch_t e, MonMap *m) : 
    Message(MSG_MON_ELECTION), 
    op(o), epoch(e) {
    m->encode(monmap_bl);
  }
private:
  ~MMonElection() {}

public:  
  const char *get_type_name() { return "election"; }
  void print(ostream& out) {
    out << "election(" << get_opname(op) << " " << epoch << ")";
  }
  
  void encode_payload() {
    ::encode(op, payload);
    ::encode(epoch, payload);
    ::encode(monmap_bl, payload);
    ::encode(quorum, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(epoch, p);
    ::decode(monmap_bl, p);
    ::decode(quorum, p);
  }
  
};

#endif
