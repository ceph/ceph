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


#ifndef __MNSLOOKUPREPLY_H
#define __MNSLOOKUPREPLY_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSLookupReply : public Message {
 public:
  map<entity_name_t, entity_inst_t> entity_map;  

 public:
  MNSLookupReply() {}
  MNSLookupReply(MNSLookup *m) : 
    Message(MSG_NS_LOOKUPREPLY) { 
  }
  
  char *get_type_name() { return "NSLookR"; }

  void encode_payload() {
    ::_encode(entity_map, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(entity_map, payload, off);
  }
};


#endif

