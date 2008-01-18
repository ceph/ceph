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
    case CEPH_SESSION_STALE: return "stale";
    case CEPH_SESSION_REQUEST_RESUME: return "request_resume";
    case CEPH_SESSION_RESUME: return "resume";
    default: assert(0); return 0;
    }
  }

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
    out << "client_session(" << get_opname(op);
    if (seq) out << " seq " << seq;
    out << ")";
  }

  void decode_payload() { 
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(op, p);
    ::_decode_simple(seq, p);
    ::_decode_simple(stamp, p);
  }
  void encode_payload() { 
    ::_encode_simple(op, payload);
    ::_encode_simple(seq, payload);
    ::_encode_simple(stamp, payload);
  }
};

#endif
