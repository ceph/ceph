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


#ifndef __MMONELECTIONREFRESH_H
#define __MMONELECTIONREFRESH_H

#include "msg/Message.h"

#include "mon/Elector.h"

class MMonElectionRefresh : public Message {
 public:
  int p;
  Elector::State state;
  int refresh_num;

  MMonElectionRefresh() {}
  MMonElectionRefresh(int _p, Elector::State& s, int r) :
    Message(MSG_MON_ELECTION_REFRESH),
    p(_p), state(s), refresh_num(r) {}
 
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(p), (char*)&p);
    off += sizeof(p);
    payload.copy(off, sizeof(state), (char*)&state);
    off += sizeof(state);
    payload.copy(off, sizeof(refresh_num), (char*)&refresh_num);
    off += sizeof(refresh_num);
  }
  void encode_payload() {
    payload.append((char*)&p, sizeof(p));
    payload.append((char*)&state, sizeof(state));
    payload.append((char*)&refresh_num, sizeof(refresh_num));
  }

  virtual char *get_type_name() { return "MonElRefresh"; }
};

#endif
