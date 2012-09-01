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



#ifndef CEPH_MROUTE_H
#define CEPH_MROUTE_H

#include "msg/Message.h"
#include "include/encoding.h"

struct MRoute : public Message {
  uint64_t session_mon_tid;
  Message *msg;
  entity_inst_t dest;
  
  MRoute() : Message(MSG_ROUTE), msg(NULL) {}
  MRoute(uint64_t t, Message *m) :
    Message(MSG_ROUTE), session_mon_tid(t), msg(m) {}
  MRoute(bufferlist bl, entity_inst_t i) :
    Message(MSG_ROUTE), session_mon_tid(0), dest(i) {
    bufferlist::iterator p = bl.begin();
    msg = decode_message(NULL, p);
  }
private:
  ~MRoute() {
    if (msg) msg->put();
  }

public:
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(session_mon_tid, p);
    ::decode(dest, p);
    if (!p.end())
      msg = decode_message(NULL, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(session_mon_tid, payload);
    ::encode(dest, payload);
    if (msg)
      encode_message(msg, features, payload);
  }

  const char *get_type_name() const { return "route"; }
  void print(ostream& o) const {
    if (msg)
      o << "route(" << *msg;
    else
      o << "route(no-reply";
    if (session_mon_tid)
      o << " tid " << session_mon_tid << ")";
    else
      o << " to " << dest << ")";
  }
};

#endif
