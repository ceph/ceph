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


#ifndef CEPH_MFSMAP_H
#define CEPH_MFSMAP_H

#include "msg/Message.h"
#include "mds/FSMap.h"
#include "include/ceph_features.h"

class MFSMap : public Message {
 public:
  epoch_t epoch;
  bufferlist encoded;

  version_t get_epoch() const { return epoch; }
  bufferlist& get_encoded() { return encoded; }

  MFSMap() : 
    Message(CEPH_MSG_FS_MAP), epoch(0) {}
  MFSMap(const uuid_d &f, FSMap *fsmap) :
    Message(CEPH_MSG_FS_MAP), epoch(fsmap->get_epoch())
  {
    fsmap->encode(encoded, -1);
  }
private:
  ~MFSMap() {}

public:
  const char *get_type_name() const { return "mdsmap"; }
  void print(ostream& out) const {
    out << "fsmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(encoded, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    ::encode(encoded, payload);
  }
};

#endif
