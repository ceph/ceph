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

  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 2;

  uint64_t session_mon_tid;
  Message *msg;
  entity_inst_t dest;
  epoch_t send_osdmap_first;
  
  MRoute() : Message(MSG_ROUTE, HEAD_VERSION, COMPAT_VERSION),
	     session_mon_tid(0),
	     msg(NULL),
	     send_osdmap_first(0) {}
  MRoute(uint64_t t, Message *m)
    : Message(MSG_ROUTE, HEAD_VERSION, COMPAT_VERSION),
      session_mon_tid(t),
      msg(m),
      send_osdmap_first(0) {}
  MRoute(bufferlist bl, const entity_inst_t& i)
    : Message(MSG_ROUTE, HEAD_VERSION, COMPAT_VERSION),
      session_mon_tid(0),
      dest(i),
      send_osdmap_first(0) {
    bufferlist::iterator p = bl.begin();
    msg = decode_message(NULL, 0, p);
  }
private:
  ~MRoute() {
    if (msg)
      msg->put();
  }

public:
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(session_mon_tid, p);
    ::decode(dest, p);
    if (header.version >= 2) {
      bool m;
      ::decode(m, p);
      if (m)
	msg = decode_message(NULL, 0, p);
    } else {
      msg = decode_message(NULL, 0, p);
    }
    if (header.version >= 3) {
      ::decode(send_osdmap_first, p);
    }
  }
  void encode_payload(uint64_t features) {
    ::encode(session_mon_tid, payload);
    ::encode(dest, payload);
    bool m = msg ? true : false;
    ::encode(m, payload);
    if (msg)
      encode_message(msg, features, payload);
    ::encode(send_osdmap_first, payload);
  }

  const char *get_type_name() const { return "route"; }
  void print(ostream& o) const {
    if (msg)
      o << "route(" << *msg;
    else
      o << "route(no-reply";
    if (send_osdmap_first)
      o << " send_osdmap_first " << send_osdmap_first;
    if (session_mon_tid)
      o << " tid " << session_mon_tid << ")";
    else
      o << " to " << dest << ")";
  }
};

#endif
