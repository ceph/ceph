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


#ifndef CEPH_MMDSMAP_H
#define CEPH_MMDSMAP_H

#include "msg/Message.h"
#include "mds/MDSMap.h"
#include "include/ceph_features.h"

class MMDSMap : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;
public:

  uuid_d fsid;
  epoch_t epoch = 0;
  bufferlist encoded;

  version_t get_epoch() const { return epoch; }
  bufferlist& get_encoded() { return encoded; }

  MMDSMap() : 
    Message(CEPH_MSG_MDS_MAP, HEAD_VERSION, COMPAT_VERSION) {}
  MMDSMap(const uuid_d &f, MDSMap *mm) :
    Message(CEPH_MSG_MDS_MAP, HEAD_VERSION, COMPAT_VERSION),
    fsid(f) {
    epoch = mm->get_epoch();
    mm->encode(encoded, -1);  // we will reencode with fewer features as necessary
  }
private:
  ~MMDSMap() override {}

public:
  const char *get_type_name() const override { return "mdsmap"; }
  void print(ostream& out) const override {
    out << "mdsmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(fsid, p);
    decode(epoch, p);
    decode(encoded, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(epoch, payload);
    if ((features & CEPH_FEATURE_PGID64) == 0 ||
	(features & CEPH_FEATURE_MDSENC) == 0 ||
	(features & CEPH_FEATURE_MSG_ADDR2) == 0) {
      // reencode for old clients.
      MDSMap m;
      m.decode(encoded);
      encoded.clear();
      m.encode(encoded, features);
    }
    encode(encoded, payload);
  }
};

#endif
