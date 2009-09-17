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

#ifndef __MAUTH_H
#define __MAUTH_H

#include "include/AuthLibrary.h"
#include "messages/PaxosServiceMessage.h"

enum {
   AUTH_NOOP = 0,
   AUTH_GET,
   AUTH_SET,
   AUTH_RESPONSE,
};

class MAuthMon : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  deque<AuthLibEntry> info;
  deque<bool> add;
  version_t last;
  __s32 action;


  MAuthMon() : PaxosServiceMessage(MSG_AUTHMON, 0) {}
  MAuthMon(const ceph_fsid_t& f, version_t l) : PaxosServiceMessage(MSG_AUTHMON, 0), fsid(f), last(l) {}
  MAuthMon(const ceph_fsid_t& f, version_t l, version_t paxos_version) :
    PaxosServiceMessage(MSG_AUTHMON, paxos_version), fsid(f), last(l) {}

  const char *get_type_name() { return "class"; }
  void print(ostream& out) {
    out << "class(";
    switch (action) {
    case AUTH_NOOP:
       out << "NOOP, ";
       break;
    case AUTH_GET:
       out << "GET, ";
       break;
    case AUTH_SET:
       out << "SET, ";
       break;
    case AUTH_RESPONSE:
       out << "SET, ";
       break;
    default:
       out << "Unknown op, ";
       break;
    }
    if (info.size())
      out << info.size() << " entries";
    if (last)
      out << "last " << last;
    out << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(info, payload);
    ::encode(add, payload);
    ::encode(last, payload);
    ::encode(action, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(info, p);
    ::decode(add, p);
    ::decode(last, p);
    ::decode(action, p);
  }
};

#endif
