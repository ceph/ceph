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


#ifndef __MNSREGISTERACK_H
#define __MNSREGISTERACK_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSRegisterAck : public Message {
  entity_name_t entity;
  long tid;

 public:
  MNSRegisterAck() {}
  MNSRegisterAck(long t, entity_name_t e) : 
    Message(MSG_NS_REGISTERACK) { 
    entity = e;
    tid = t;
  }
  
  char *get_type_name() { return "NSRegA"; }

  entity_name_t get_entity() { return entity; }
  long get_tid() { return tid; }

  void encode_payload() {
    payload.append((char*)&entity, sizeof(entity));
    payload.append((char*)&tid, sizeof(tid));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(entity), (char*)&entity);
    off += sizeof(entity);
    payload.copy(off, sizeof(tid), (char*)&tid);
    off += sizeof(tid);
  }
};


#endif

