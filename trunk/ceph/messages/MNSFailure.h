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


#ifndef __MNSFAILURE_H
#define __MNSFAILURE_H

#include "msg/Message.h"
#include "msg/tcp.h"

class MNSFailure : public Message {
  //msg_addr_t    entity;
  entity_inst_t inst;

 public:
  MNSFailure() {}
  MNSFailure(entity_inst_t& i) :
    Message(MSG_NS_FAILURE),
    //entity(w), 
    inst(i) {}
  
  char *get_type_name() { return "NSFail"; }

  //msg_addr_t &get_entity() { return entity; }
  entity_inst_t &get_inst() { return inst; }

  void encode_payload() {
    //payload.append((char*)&entity, sizeof(entity));
    payload.append((char*)&inst, sizeof(inst));
  }
  void decode_payload() {
    unsigned off = 0;
    //payload.copy(off, sizeof(entity), (char*)&entity);
    //off += sizeof(entity);
    payload.copy(off, sizeof(inst), (char*)&inst);
    off += sizeof(inst);
  }
};


#endif

