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


#ifndef __MMONELECTIONSTATUS_H
#define __MMONELECTIONSTATUS_H

#include "msg/Message.h"

#include "mon/Elector.h"

class MMonElectionStatus : public Message {
 public:
  int q;
  int read_num;
  map<int,Elector::State> registry;

  MMonElectionStatus() {}
  MMonElectionStatus(int _q, int r, map<int,Elector::State> reg) :
    Message(MSG_MON_ELECTION_STATUS),
    q(_q), read_num(r), registry(reg) {}
 
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(q), (char*)&q);
    off += sizeof(q);
    payload.copy(off, sizeof(read_num), (char*)&read_num);
    off += sizeof(read_num);
    ::_decode(registry, payload, off);
  }
  void encode_payload() {
    payload.append((char*)&q, sizeof(q));
    payload.append((char*)&read_num, sizeof(read_num));
    ::_encode(registry, payload);
  }

  virtual char *get_type_name() { return "MonElStatus"; }
};

#endif
