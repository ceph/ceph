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
  int state;
  version_t seq;

 public:
  MMDSBeacon() : Message(MSG_MDS_BEACON) {}
  MMDSBeacon(entity_inst_t i, int st, version_t se) : 
    Message(MSG_MDS_BEACON), 
    inst(i), state(st), seq(se) { }

  entity_inst_t& get_mds_inst() { return inst; }
  int get_state() { return state; }
  version_t get_seq() { return seq; }
  char *get_type_name() { return "mdsbeacon"; }

  void print(ostream& out) {
    out << "mdsbeacon(" << inst
	<< " " << MDSMap::get_state_name(state) 
	<< " seq " << seq << ")";
  }
  
  void encode_payload() {
    ::_encode(inst, payload);
    ::_encode(state, payload);
    ::_encode(seq, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inst, payload, off);
    ::_decode(state, payload, off);
    ::_decode(seq, payload, off);
  }
};

#endif
