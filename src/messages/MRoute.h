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

class MRoute : public MessageInstance<MRoute> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 3;

  uint64_t session_mon_tid;
  Message *msg;
  epoch_t send_osdmap_first;
  
  MRoute() : MessageInstance(MSG_ROUTE, HEAD_VERSION, COMPAT_VERSION),
	     session_mon_tid(0),
	     msg(NULL),
	     send_osdmap_first(0) {}
  MRoute(uint64_t t, Message *m)
    : MessageInstance(MSG_ROUTE, HEAD_VERSION, COMPAT_VERSION),
      session_mon_tid(t),
      msg(m),
      send_osdmap_first(0) {}
private:
  ~MRoute() override {
    if (msg)
      msg->put();
  }

public:
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(session_mon_tid, p);
    entity_inst_t dest_unused;
    decode(dest_unused, p);
    bool m;
    decode(m, p);
    if (m)
      msg = decode_message(NULL, 0, p);
    decode(send_osdmap_first, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(session_mon_tid, payload);
    entity_inst_t dest_unused;
    encode(dest_unused, payload, features);
    bool m = msg ? true : false;
    encode(m, payload);
    if (msg)
      encode_message(msg, features, payload);
    encode(send_osdmap_first, payload);
  }

  std::string_view get_type_name() const override { return "route"; }
  void print(ostream& o) const override {
    if (msg)
      o << "route(" << *msg;
    else
      o << "route(no-reply";
    if (send_osdmap_first)
      o << " send_osdmap_first " << send_osdmap_first;
    if (session_mon_tid)
      o << " tid " << session_mon_tid << ")";
    else
      o << " tid (none)";
  }
};

#endif
