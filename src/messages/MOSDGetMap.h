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

#ifndef __MOSDGETMAP_H
#define __MOSDGETMAP_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

class MOSDGetMap : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;
  epoch_t start;  // this is the first incremental the sender wants (he has start-1)

  MOSDGetMap() : PaxosServiceMessage(CEPH_MSG_OSD_GETMAP, 0) {}
  MOSDGetMap(const ceph_fsid_t& f, epoch_t s=0) : 
    PaxosServiceMessage(CEPH_MSG_OSD_GETMAP, s>0 ? s-1 : 0),
    fsid(f), start(s) { }

  epoch_t get_start_epoch() { return start; }

  const char *get_type_name() { return "get_osd_map"; }
  void print(ostream& out) {
    out << "get_osd_map(start " << start << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(start, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(start, p);
  }
};

#endif
