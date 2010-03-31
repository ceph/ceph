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

#ifndef __MCLASS_H
#define __MCLASS_H

#include "include/ClassLibrary.h"
#include "messages/PaxosServiceMessage.h"

enum {
   CLASS_NOOP = 0,
   CLASS_GET,
   CLASS_SET,
   CLASS_RESPONSE,
};

class MClass : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  deque<ClassInfo> info;
  deque<ClassImpl> impl;
  deque<bool> add;
  version_t last;
  __s32 action;


  MClass() : PaxosServiceMessage(MSG_CLASS, 0) {}
#if 0
  MClass(const ceph_fsid_t& f, deque<ClassLibraryIncremental>& e) :
    PaxosServiceMessage(MSG_CLASS, 0),
    fsid(f), entries(e), last(0), action(0) {}
#endif
  MClass(const ceph_fsid_t& f, version_t l) : PaxosServiceMessage(MSG_CLASS, 0), fsid(f), last(l) {}
  MClass(const ceph_fsid_t& f, version_t l, version_t paxos_version) :
    PaxosServiceMessage(MSG_CLASS, paxos_version), fsid(f), last(l) {}
private:
  ~MClass() {}

public:
  const char *get_type_name() { return "class"; }
  void print(ostream& out) {
    out << "class(";
    switch (action) {
    case CLASS_NOOP:
       out << "NOOP, ";
       break;
    case CLASS_GET:
       out << "GET, ";
       break;
    case CLASS_SET:
       out << "SET, ";
       break;
    case CLASS_RESPONSE:
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
    ::encode(impl, payload);
    ::encode(add, payload);
    ::encode(last, payload);
    ::encode(action, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(info, p);
    ::decode(impl, p);
    ::decode(add, p);
    ::decode(last, p);
    ::decode(action, p);
  }
};

#endif
