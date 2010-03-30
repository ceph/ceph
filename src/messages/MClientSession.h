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
  ceph_mds_session_head head;

  int get_op() { return head.op; }
  version_t get_seq() { return head.seq; }
  utime_t get_stamp() { return utime_t(head.stamp); }
  int get_max_caps() { return head.max_caps; }
  int get_max_leases() { return head.max_leases; }

  MClientSession() : Message(CEPH_MSG_CLIENT_SESSION) { }
  MClientSession(int o, version_t s=0) : 
    Message(CEPH_MSG_CLIENT_SESSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = s;
  }
  MClientSession(int o, utime_t st) : 
    Message(CEPH_MSG_CLIENT_SESSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = 0;
    st.encode_timeval(&head.stamp);
  }
private:
  ~MClientSession() {}

public:
  const char *get_type_name() { return "client_session"; }
  void print(ostream& out) {
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
  }
  void encode_payload() { 
    ::encode(head, payload);
  }
};

#endif
