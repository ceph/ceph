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

#ifndef __MMDSGETMAP_H
#define __MMDSGETMAP_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

class MMDSGetMap : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;

  MMDSGetMap() : PaxosServiceMessage(CEPH_MSG_MDS_GETMAP, 0) {}
  MMDSGetMap(const ceph_fsid_t &f, epoch_t have=0) : 
    PaxosServiceMessage(CEPH_MSG_MDS_GETMAP, have), 
    fsid(f) { }

  const char *get_type_name() { return "mds_getmap"; }
  void print(ostream& out) {
    out << "mds_getmap(have v" << version << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
  }
};

#endif
