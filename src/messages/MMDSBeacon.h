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

#include "msg/Message.h"

#include "include/types.h"

#include "mds/MDSMap.h"

class MMDSBeacon : public Message {
  ceph_fsid_t fsid;
  epoch_t last_epoch_seen;  // include last mdsmap epoch mds has seen to avoid race with monitor decree
  __u32 state;
  version_t seq;
  __s32 want_rank;

 public:
  MMDSBeacon() : Message(MSG_MDS_BEACON) {}
  MMDSBeacon(ceph_fsid_t &f, epoch_t les, int st, version_t se, int wr) : 
    Message(MSG_MDS_BEACON), 
    fsid(f), last_epoch_seen(les), state(st), seq(se), want_rank(wr) { }

  ceph_fsid_t& get_fsid() { return fsid; }
  epoch_t get_last_epoch_seen() { return last_epoch_seen; }
  int get_state() { return state; }
  version_t get_seq() { return seq; }
  const char *get_type_name() { return "mdsbeacon"; }
  int get_want_rank() { return want_rank; }

  void print(ostream& out) {
    out << "mdsbeacon(" << MDSMap::get_state_name(state) 
	<< " seq " << seq << ")";
  }
  
  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(last_epoch_seen, payload);
    ::encode(state, payload);
    ::encode(seq, payload);
    ::encode(want_rank, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(last_epoch_seen, p);
    ::decode(state, p);
    ::decode(seq, p);
    ::decode(want_rank, p);
  }
};

#endif
