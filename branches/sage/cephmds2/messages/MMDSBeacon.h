// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  int state;
  version_t seq;

 public:
  MMDSBeacon() : Message(MSG_MDS_BEACON) {}
  MMDSBeacon(int st, version_t se) : Message(MSG_MDS_BEACON), 
				     state(st), seq(se) { }

  int get_state() { return state; }
  version_t get_seq() { return seq; }
  char *get_type_name() { return "mdsbeacon"; }

  void print(ostream& out) {
    out << "mdsbeacon(" << MDSMap::get_state_name(state) 
	<< " seq " << seq << ")";
  }
  
  void encode_payload() {
    payload.append((char*)&state, sizeof(state));
    payload.append((char*)&seq, sizeof(seq));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(state), (char*)&state);
    off += sizeof(state);
    payload.copy(off, sizeof(seq), (char*)&seq);
    off += sizeof(seq);
  }
};

#endif
