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

#ifndef __MCLIENTSESSION_H
#define __MCLIENTSESSION_H

#include "msg/Message.h"

class MClientSession : public Message {
public:

  int32_t op;
  version_t seq;  // used when requesting close, declaring stale
  utime_t stamp;

  MClientSession() : Message(CEPH_MSG_CLIENT_SESSION) { }
  MClientSession(int o, version_t s=0) : 
    Message(CEPH_MSG_CLIENT_SESSION),
    op(o), seq(s) { }
  MClientSession(int o, utime_t st) : 
    Message(CEPH_MSG_CLIENT_SESSION),
    op(o), seq(0), stamp(st) { }

  const char *get_type_name() { return "client_session"; }
  void print(ostream& out) {
    out << "client_session(" << ceph_session_op_name(op);
    if (seq) out << " seq " << seq;
    out << ")";
  }

  void decode_payload() { 
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(seq, p);
    ::decode(stamp, p);
  }
  void encode_payload() { 
    ::encode(op, payload);
    ::encode(seq, payload);
    ::encode(stamp, payload);
  }
};

#endif
