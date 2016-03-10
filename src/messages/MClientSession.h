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

#ifndef CEPH_MCLIENTSESSION_H
#define CEPH_MCLIENTSESSION_H

#include "msg/Message.h"

class MClientSession : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

public:
  ceph_mds_session_head head;

  std::map<std::string, std::string> client_meta;

  int get_op() const { return head.op; }
  version_t get_seq() const { return head.seq; }
  utime_t get_stamp() const { return utime_t(head.stamp); }
  int get_max_caps() const { return head.max_caps; }
  int get_max_leases() const { return head.max_leases; }

  MClientSession() : Message(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) { }
  MClientSession(int o, version_t s=0) : 
    Message(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = s;
  }
  MClientSession(int o, utime_t st) : 
    Message(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = 0;
    st.encode_timeval(&head.stamp);
  }
private:
  ~MClientSession() {}

public:
  const char *get_type_name() const { return "client_session"; }
  void print(ostream& out) const {
    out << "client_session(" << ceph_session_op_name(get_op());
    if (get_seq())
      out << " seq " << get_seq();
    if (get_op() == CEPH_SESSION_RECALL_STATE)
      out << " max_caps " << head.max_caps << " max_leases " << head.max_leases;
    out << ")";
  }

  void decode_payload() { 
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    if (header.version >= 2) {
      ::decode(client_meta, p);
    }
  }
  void encode_payload(uint64_t features) { 
    ::encode(head, payload);
    if (client_meta.empty()) {
      // If we're not trying to send any metadata (always the case if
      // we are a server) then send older-format message to avoid upsetting
      // old kernel clients.
      header.version = 1;
    } else {
      ::encode(client_meta, payload);
      header.version = HEAD_VERSION;
    }

  }
};
REGISTER_MESSAGE(MClientSession, CEPH_MSG_CLIENT_SESSION);
#endif
