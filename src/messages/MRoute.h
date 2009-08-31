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



#ifndef __MROUTE_H
#define __MROUTE_H

#include "msg/Message.h"
#include "include/encoding.h"

struct MRoute : public Message {
  Message *msg;
  entity_inst_t dest;
  
  MRoute() : Message(MSG_ROUTE), msg(NULL) {}
  MRoute(Message *m, entity_inst_t i) : Message(MSG_ROUTE), msg(m), dest(i) {}
  ~MRoute() {
    delete msg;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dest, p);
    ceph_msg_header h;
    ceph_msg_footer f;
    bufferlist fr, mi, da;
    ::decode(h, p);
    ::decode(f, p);
    ::decode(fr, p);
    ::decode(mi, p);
    ::decode(da, p);
    msg = decode_message(h, f, fr, mi, da);
  }
  void encode_payload() {
    ::encode(dest, payload);
    bufferlist front, middle, data;
    msg->encode();
    ::encode(msg->get_header(), payload);
    ::encode(msg->get_footer(), payload);
    ::encode(msg->get_payload(), payload);
    ::encode(msg->get_middle(), payload);
    ::encode(msg->get_data(), payload);
  }

  const char *get_type_name() { return "route"; }
  void print(ostream& o) {
    if (msg)
      o << "route(" << *msg << " to " << dest << ")";
    else
      o << "route(??? to " << dest << ")";
  }
};

#endif
