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

#ifndef CEPH_MMONSUBSCRIBEACK_H
#define CEPH_MMONSUBSCRIBEACK_H

#include "msg/Message.h"

struct MMonSubscribeAck : public Message {
  __u32 interval;
  uuid_d fsid;
  
  MMonSubscribeAck() : Message(CEPH_MSG_MON_SUBSCRIBE_ACK),
		       interval(0) {
    memset(&fsid, 0, sizeof(fsid));
  }
  MMonSubscribeAck(uuid_d& f, int i) : Message(CEPH_MSG_MON_SUBSCRIBE_ACK),
				       interval(i), fsid(f) { }
private:
  ~MMonSubscribeAck() {}

public:
  const char *get_type_name() const { return "mon_subscribe_ack"; }
  void print(ostream& o) const {
    o << "mon_subscribe_ack(" << interval << "s)";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(interval, p);
    ::decode(fsid, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(interval, payload);
    ::encode(fsid, payload);
  }
};

#endif
