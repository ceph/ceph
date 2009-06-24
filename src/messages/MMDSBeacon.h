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

#ifndef __MMDSBEACON_H
#define __MMDSBEACON_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

#include "mds/MDSMap.h"

class MMDSBeacon : public PaxosServiceMessage {
  ceph_fsid_t fsid;
  string name;
  epoch_t last_epoch_seen;  // include last mdsmap epoch mds has seen to avoid race with monitor decree
  __u32 state;
  version_t seq;
  __s32 standby_for_rank;
  string standby_for_name;

 public:
  MMDSBeacon() : PaxosServiceMessage(MSG_MDS_BEACON, 0) {}
  MMDSBeacon(ceph_fsid_t &f, string& n, epoch_t les, int st, version_t se) : 
    PaxosServiceMessage(MSG_MDS_BEACON, se), 
    fsid(f), name(n), last_epoch_seen(les), state(st), seq(se),
    standby_for_rank(-1) { }

  ceph_fsid_t& get_fsid() { return fsid; }
  string& get_name() { return name; }
  epoch_t get_last_epoch_seen() { return last_epoch_seen; }
  int get_state() { return state; }
  version_t get_seq() { return seq; }
  const char *get_type_name() { return "mdsbeacon"; }
  int get_standby_for_rank() { return standby_for_rank; }
  const string& get_standby_for_name() { return standby_for_name; }

  void set_standby_for_rank(int r) { standby_for_rank = r; }
  void set_standby_for_name(string& n) { standby_for_name = n; }

  void print(ostream& out) {
    out << "mdsbeacon(" << name << " " << ceph_mds_state_name(state) 
	<< " seq " << seq << "v " << version << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(last_epoch_seen, payload);
    ::encode(state, payload);
    ::encode(seq, payload);
    ::encode(name, payload);
    ::encode(standby_for_rank, payload);
    ::encode(standby_for_name, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(last_epoch_seen, p);
    ::decode(state, p);
    ::decode(seq, p);
    ::decode(name, p);
    ::decode(standby_for_rank, p);
    ::decode(standby_for_name, p);
  }
};

#endif
