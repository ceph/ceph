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
  entity_inst_t inst;
  epoch_t last_epoch_seen;  // include last mdsmap epoch mds has seen to avoid race with monitor decree
  int state;
  version_t seq;
  int want_rank;

 public:
  MMDSBeacon() : Message(MSG_MDS_BEACON) {}
  MMDSBeacon(entity_inst_t i, epoch_t les, int st, version_t se, int wr) : 
    Message(MSG_MDS_BEACON), 
    inst(i), last_epoch_seen(les), state(st), seq(se), want_rank(wr) { }

  entity_inst_t& get_mds_inst() { return inst; }
  epoch_t get_last_epoch_seen() { return last_epoch_seen; }
  int get_state() { return state; }
  version_t get_seq() { return seq; }
  const char *get_type_name() { return "mdsbeacon"; }
  int get_want_rank() { return want_rank; }

  void print(ostream& out) {
    out << "mdsbeacon(" << inst
	<< " " << MDSMap::get_state_name(state) 
	<< " seq " << seq << ")";
  }
  
  void encode_payload() {
    ::_encode(inst, payload);
    ::_encode(last_epoch_seen, payload);
    ::_encode(state, payload);
    ::_encode(seq, payload);
    ::_encode(want_rank, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inst, payload, off);
    ::_decode(last_epoch_seen, payload, off);
    ::_decode(state, payload, off);
    ::_decode(seq, payload, off);
    ::_decode(want_rank, payload, off);
  }
};

#endif
