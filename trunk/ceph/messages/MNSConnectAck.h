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


#ifndef __MNSCONNECTACK_H
#define __MNSCONNECTACK_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSConnectAck : public Message {
  int rank;
  int inst;

 public:
  MNSConnectAck() {}
  MNSConnectAck(int r, int g=0) : 
    Message(MSG_NS_CONNECTACK) { 
    rank = r;
    inst = g;
  }
  
  char *get_type_name() { return "NSConA"; }

  int get_rank() { return rank; }
  int get_inst() { return inst; }

  void encode_payload() {
    payload.append((char*)&rank, sizeof(rank));
    payload.append((char*)&inst, sizeof(inst));
  }
  void decode_payload() {
    unsigned off = 0;
    payload.copy(off, sizeof(rank), (char*)&rank);
    off += sizeof(rank);
    payload.copy(off, sizeof(inst), (char*)&inst);
    off += sizeof(inst);
  }
};


#endif

