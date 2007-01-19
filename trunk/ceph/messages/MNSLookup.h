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


#ifndef __MNSLOOKUP_H
#define __MNSLOOKUP_H

#include "msg/Message.h"

class MNSLookup : public Message {
  msg_addr_t entity;

 public:
  MNSLookup() {}
  MNSLookup(msg_addr_t e) :
    Message(MSG_NS_LOOKUP) {
    entity = e;
  }
  
  char *get_type_name() { return "NSLook"; }

  msg_addr_t get_entity() { return entity; }

  void encode_payload() {
    payload.append((char*)&entity, sizeof(entity));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(entity), (char*)&entity);
    off += sizeof(entity);
  }
};


#endif

