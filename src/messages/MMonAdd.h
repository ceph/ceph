// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MMONADD_H
#define __MMONADD_H

#include "msg/Message.h"
#include "include/types.h"

#include "mon/MonMap.h"
#include "config.h"

class MMonAdd : public Message {
 public:
  entity_addr_t address;

  MMonAdd() : Message(MSG_MON_ADD) {}
  MMonAdd(const char *ip) : Message(MSG_MON_ADD) {
    parse_ip_port(ip, address);}
  MMonAdd(const struct entity_addr_t& addr) :
    Message(MSG_MON_ADD), address(addr) {}

  const char* get_type_name() { return "mon_add"; }
  void print (ostream& o) { o << "mon_add(" << address << ")"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(address, p);
  }

  void encode_payload() {
    ::encode(address, payload);
  }
};

#endif
