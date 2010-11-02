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

#ifndef CEPH_MPGSTATS_H
#define CEPH_MPGSTATS_H

#include "osd/osd_types.h"
#include "messages/PaxosServiceMessage.h"

class MPGStats : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  map<pg_t,pg_stat_t> pg_stat;
  osd_stat_t osd_stat;
  epoch_t epoch;
  utime_t had_map_for;
  
  MPGStats() : PaxosServiceMessage(MSG_PGSTATS, 0) {}
  MPGStats(const ceph_fsid_t& f, epoch_t e, utime_t had) : 
    PaxosServiceMessage(MSG_PGSTATS, e), fsid(f), epoch(e), had_map_for(had) {}

private:
  ~MPGStats() {};

public:
  const char *get_type_name() { return "pg_stats"; }
  void print(ostream& out) {
    out << "pg_stats(" << pg_stat.size() << " pgs v " << version << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(osd_stat, payload);
    ::encode(pg_stat, payload);
    ::encode(epoch, payload);
    ::encode(had_map_for, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(osd_stat, p);
    ::decode(pg_stat, p);
    ::decode(epoch, p);
    ::decode(had_map_for, p);
  }
};

#endif
