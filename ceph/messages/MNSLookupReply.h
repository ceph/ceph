// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  map<msg_addr_t, int> entity_rank;  // e -> rank
  map<msg_addr_t, int> entity_gen;  // e -> gen
  map<int, tcpaddr_t>  rank_addr;   // rank -> addr

 public:
  MNSLookupReply() {}
  MNSLookupReply(MNSLookup *m) : 
	Message(MSG_NS_LOOKUPREPLY) { 
  }
  
  char *get_type_name() { return "NSLookR"; }

  void encode_payload() {
	::_encode(entity_rank, payload);
	::_encode(entity_gen, payload);
	::_encode(rank_addr, payload);
  }
  void decode_payload() {
	int off = 0;
	::_decode(entity_rank, payload, off);
	::_decode(entity_gen, payload, off);
	::_decode(rank_addr, payload, off);
  }
};


#endif

