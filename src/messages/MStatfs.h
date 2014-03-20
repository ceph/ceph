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


#ifndef CEPH_MSTATFS_H
#define CEPH_MSTATFS_H

#include <sys/statvfs.h>    /* or <sys/statfs.h> */
#include "messages/PaxosServiceMessage.h"

class MStatfs : public PaxosServiceMessage {
public:
  uuid_d fsid;

  MStatfs() : PaxosServiceMessage(CEPH_MSG_STATFS, 0) {}
  MStatfs(const uuid_d& f, ceph_tid_t t, version_t v) :
    PaxosServiceMessage(CEPH_MSG_STATFS, v), fsid(f) {
    set_tid(t);
  }

private:
  ~MStatfs() {}

public:
  const char *get_type_name() const { return "statfs"; }
  void print(ostream& out) const {
    out << "statfs(" << get_tid() << " v" << version << ")";
  }

  void encode_payload(uint64_t features) {
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
