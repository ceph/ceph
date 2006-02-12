// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef __MNSCONNECT_H
#define __MNSCONNECT_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

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

