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

#ifndef __MNSREGISTERACK_H
#define __MNSREGISTERACK_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSRegisterAck : public Message {
  msg_addr_t entity;
  long tid;

 public:
  MNSRegisterAck() {}
  MNSRegisterAck(long t, msg_addr_t e) : 
	Message(MSG_NS_REGISTERACK) { 
	entity = e;
	tid = t;
  }
  
  char *get_type_name() { return "NSRegA"; }

  msg_addr_t get_entity() { return entity; }
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

