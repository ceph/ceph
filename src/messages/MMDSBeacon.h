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

#ifndef CEPH_MMDSBEACON_H
#define CEPH_MMDSBEACON_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

#include "mds/MDSMap.h"

class MMDSBeacon : public PaxosServiceMessage {
  ceph_fsid_t fsid;
  uint64_t global_id;
  string name;

  __u32 state;
  version_t seq;
  __s32 standby_for_rank;
  string standby_for_name;

  CompatSet compat;

 public:
  MMDSBeacon() : PaxosServiceMessage(MSG_MDS_BEACON, 0) {}
  MMDSBeacon(const ceph_fsid_t &f, uint64_t g, string& n, epoch_t les, int st, version_t se) : 
    PaxosServiceMessage(MSG_MDS_BEACON, les), 
    fsid(f), global_id(g), name(n), state(st), seq(se),
    standby_for_rank(-1) { }
private:
  ~MMDSBeacon() {}

public:
  ceph_fsid_t& get_fsid() { return fsid; }
  uint64_t get_global_id() { return global_id; }
  string& get_name() { return name; }
  epoch_t get_last_epoch_seen() { return version; }
  int get_state() { return state; }
  version_t get_seq() { return seq; }
  const char *get_type_name() { return "mdsbeacon"; }
  int get_standby_for_rank() { return standby_for_rank; }
  const string& get_standby_for_name() { return standby_for_name; }

  CompatSet& get_compat() { return compat; }
  void set_compat(CompatSet& c) { compat = c; }

  void set_standby_for_rank(int r) { standby_for_rank = r; }
  void set_standby_for_name(string& n) { standby_for_name = n; }

  void print(ostream& out) {
    out << "mdsbeacon(" << global_id << "/" << name << " " << ceph_mds_state_name(state) 
	<< " seq " << seq << " v" << version << ")";
  }

  void encode_payload() {
    header.version = 2;
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(global_id, payload);
    ::encode(state, payload);
    ::encode(seq, payload);
    ::encode(name, payload);
    ::encode(standby_for_rank, payload);
    ::encode(standby_for_name, payload);
    ::encode(compat, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(global_id, p);
    ::decode(state, p);
    ::decode(seq, p);
    ::decode(name, p);
    ::decode(standby_for_rank, p);
    ::decode(standby_for_name, p);
    if (header.version >= 2)
      ::decode(compat, p);
  }
};

#endif
