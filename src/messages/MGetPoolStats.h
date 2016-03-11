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


#ifndef CEPH_MGETPOOLSTATS_H
#define CEPH_MGETPOOLSTATS_H

#include "messages/PaxosServiceMessage.h"

class MGetPoolStats : public PaxosServiceMessage {
public:
  uuid_d fsid;
  list<string> pools;

  MGetPoolStats() : PaxosServiceMessage(MSG_GETPOOLSTATS, 0) {}
  MGetPoolStats(const uuid_d& f, ceph_tid_t t, list<string>& ls, version_t l) :
    PaxosServiceMessage(MSG_GETPOOLSTATS, l),
    fsid(f), pools(ls) {
    set_tid(t);
  }

private:
  ~MGetPoolStats() {}

public:
  const char *get_type_name() const { return "getpoolstats"; }
  void print(ostream& out) const {
    out << "getpoolstats(" << get_tid() << " " << pools << " v" << version << ")";
  }

  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(pools, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(pools, p);
  }
};
REGISTER_MESSAGE(MGetPoolStats, MSG_GETPOOLSTATS);
#endif
