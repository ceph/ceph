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


#ifndef __MNSREGISTER_H
#define __MNSREGISTER_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSRegister : public Message {
  entity_name_t addr;
  int rank;
  long tid;

 public:
  MNSRegister() {}
  MNSRegister(entity_name_t a, int r, int ti) : 
    Message(MSG_NS_REGISTER) { 
    addr = a;
    rank = r;
    tid = ti;
  }
  
  char *get_type_name() { return "NSReg"; }

  entity_name_t get_entity() { return addr; }
  int get_rank() { return rank; }
  long get_tid() { return tid; }

  void encode_payload() {
    payload.append((char*)&addr, sizeof(addr));
    payload.append((char*)&rank, sizeof(rank));
    payload.append((char*)&tid, sizeof(tid));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(addr), (char*)&addr);
    off += sizeof(addr);
    payload.copy(off, sizeof(rank), (char*)&rank);
    off += sizeof(rank);
    payload.copy(off, sizeof(tid), (char*)&tid);
    off += sizeof(tid);
  }
};


#endif

