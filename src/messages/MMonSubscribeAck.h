// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef __MMONSUBSCRIBEACK_H
#define __MMONSUBSCRIBEACK_H

#include "msg/Message.h"

struct MMonSubscribeAck : public Message {
  __u32 interval;
  ceph_fsid fsid;
  
  MMonSubscribeAck(int i = 0) : Message(CEPH_MSG_MON_SUBSCRIBE_ACK),
				interval(i) {}
  
  const char *get_type_name() { return "mon_subscribe_ack"; }
  void print(ostream& o) {
    o << "mon_subscribe_ack(" << interval << "s)";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(interval, p);
    ::decode(fsid, p);
  }
  void encode_payload() {
    ::encode(interval, payload);
    ::encode(fsid, payload);
  }
};

#endif
