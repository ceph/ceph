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

#ifndef CEPH_MMONOBSERVE_H
#define CEPH_MMONOBSERVE_H

#include "msg/Message.h"

#include <vector>
using std::vector;

class MMonObserve : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;
  uint32_t machine_id;
  version_t ver;

  MMonObserve() : PaxosServiceMessage(MSG_MON_OBSERVE, 0) {}
  MMonObserve(ceph_fsid_t &f, int mid, version_t v) : 
    PaxosServiceMessage(MSG_MON_OBSERVE, v),
    fsid(f), machine_id(mid), ver(v) { }
private:
  ~MMonObserve() {}
  
public:
  const char *get_type_name() { return "mon_observe"; }
  void print(ostream& o) {
    o << "observe(" << machine_id << " v" << ver << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(machine_id, payload);
    ::encode(ver, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(machine_id, p);
    ::decode(ver, p);
  }
};

#endif
