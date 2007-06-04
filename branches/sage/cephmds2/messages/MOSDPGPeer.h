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


#ifndef __MOSDPGPEER_H
#define __MOSDPGPEER_H

#include "msg/Message.h"


class MOSDPGPeer : public Message {
  uint64_t       map_version;
  list<pg_t> pg_list;

  bool complete;

 public:
  uint64_t get_version() { return map_version; }
  list<pg_t>& get_pg_list() { return pg_list; }
  bool get_complete() { return complete; }

  MOSDPGPeer() {}
  MOSDPGPeer(uint64_t v, list<pg_t>& l, bool c=false) :
    Message(MSG_OSD_PG_PEER) {
    this->map_version = v;
    this->complete = c;
    pg_list.splice(pg_list.begin(), l);
  }
  
  char *get_type_name() { return "PGPeer"; }

  void encode_payload() {
    payload.append((char*)&map_version, sizeof(map_version));
    payload.append((char*)&complete, sizeof(complete));
    _encode(pg_list, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(map_version), (char*)&map_version);
    off += sizeof(map_version);
    payload.copy(off, sizeof(complete), (char*)&complete);
    off += sizeof(complete);
    _decode(pg_list, payload, off);
  }
};

#endif
