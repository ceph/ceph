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


#ifndef __MGETPOOLSTATS_H
#define __MGETPOOLSTATS_H

#include "messages/PaxosServiceMessage.h"

class MGetPoolStats : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  tid_t tid;
  vector<string> pools;

  MGetPoolStats() : PaxosServiceMessage(MSG_GETPOOLSTATS, 0) {}
  MGetPoolStats(ceph_fsid_t& f, tid_t t, vector<string>& ls, version_t l) :
    PaxosServiceMessage(MSG_GETPOOLSTATS, l),
    fsid(f), tid(t), pools(ls) { }

  const char *get_type_name() { return "getpoolstats"; }
  void print(ostream& out) {
    out << "getpoolstats(" << tid << " " << pools << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(tid, payload);
    ::encode(pools, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(tid, p);
    ::decode(pools, p);
  }
};

#endif
