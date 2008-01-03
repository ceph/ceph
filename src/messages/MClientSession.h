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
  static const char *get_opname(int o) {
    switch (o) {
    case CEPH_SESSION_REQUEST_OPEN: return "request_open";
    case CEPH_SESSION_OPEN: return "open";
    case CEPH_SESSION_REQUEST_CLOSE: return "request_close";
    case CEPH_SESSION_CLOSE: return "close";
    case CEPH_SESSION_REQUEST_RENEWCAPS: return "request_renewcaps";
    case CEPH_SESSION_RENEWCAPS: return "renewcaps";
    default: assert(0); return 0;
    }
  }

  int32_t op;
  version_t seq;

  MClientSession() : Message(CEPH_MSG_CLIENT_SESSION) { }
  MClientSession(int o, version_t s=0) : 
    Message(CEPH_MSG_CLIENT_SESSION),
    op(o), seq(s) { }

  const char *get_type_name() { return "client_session"; }
  void print(ostream& out) {
    out << "client_session(" << get_opname(op);
    if (seq) out << " seq " << seq;
    out << ")";
  }

  void decode_payload() { 
    int off = 0;
    ::_decode(op, payload, off);
    ::_decode(seq, payload, off);
  }
  void encode_payload() { 
    ::_encode(op, payload);
    ::_encode(seq, payload);
  }
};

#endif
