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


#ifndef __MOSDPGPEERACK_H
#define __MOSDPGPEERACK_H

#include "msg/Message.h"
#include "osd/OSD.h"

class MOSDPGPeerAck : public Message {
  version_t       map_version;

 public:
  list<pg_t>                pg_dne;   // pg dne
  map<pg_t, PGReplicaInfo > pg_state; // state, lists, etc.

  version_t get_version() { return map_version; }

  MOSDPGPeerAck() {}
  MOSDPGPeerAck(version_t v) :
    Message(MSG_OSD_PG_PEERACK) {
    this->map_version = v;
  }
  
  char *get_type_name() { return "PGPeer"; }

  void encode_payload() {
    ::encode(map_version, payload);
    ::encode(pg_dne, payload);
    ::encode(pg_state, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_version, payload);
    ::decode(pg_dne, payload);
    ::decode(pg_state, payload);
  }
};

#endif
