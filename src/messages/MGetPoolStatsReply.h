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


#ifndef __MGETPOOLSTATSREPLY_H
#define __MGETPOOLSTATSREPLY_H

class MGetPoolStatsReply : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  tid_t tid;
  map<string,pool_stat_t> pool_stats;

  MGetPoolStatsReply() : PaxosServiceMessage(MSG_GETPOOLSTATSREPLY, 0) {}
  MGetPoolStatsReply(ceph_fsid_t& f, tid_t t, version_t v) :
    PaxosServiceMessage(MSG_GETPOOLSTATSREPLY, v),
    fsid(f), tid(t) { }

  const char *get_type_name() { return "getpoolstats"; }
  void print(ostream& out) {
    out << "getpoolstatsreply(" << tid << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(tid, payload);
    ::encode(pool_stats, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(tid, p);
    ::decode(pool_stats, p);
  }
};

#endif
