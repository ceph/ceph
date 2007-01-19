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


#ifndef __MNSCONNECT_H
#define __MNSCONNECT_H

#include "msg/Message.h"
#include "msg/tcp.h"

class MNSConnect : public Message {
  tcpaddr_t tcpaddr;

 public:
  MNSConnect() {}
  MNSConnect(tcpaddr_t t) :
    Message(MSG_NS_CONNECT) { 
    tcpaddr = t;
  }
  
  char *get_type_name() { return "NSCon"; }

  tcpaddr_t& get_addr() { return tcpaddr; }

  void encode_payload() {
    payload.append((char*)&tcpaddr, sizeof(tcpaddr));
  }
  void decode_payload() {
    payload.copy(0, sizeof(tcpaddr), (char*)&tcpaddr);
  }
};


#endif

