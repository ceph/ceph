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

class MFSMap : public MessageInstance<MFSMap> {
public:
  friend factory;

  epoch_t epoch;
  bufferlist encoded;

  version_t get_epoch() const { return epoch; }
  const FSMap& get_fsmap() const {return fsmap;}

  MFSMap() : 
    MessageInstance(CEPH_MSG_FS_MAP), epoch(0) {}
  MFSMap(const uuid_d &f, const FSMap &fsmap_) :
    MessageInstance(CEPH_MSG_FS_MAP), epoch(fsmap_.get_epoch())
  {
    fsmap = fsmap_;
  }
private:
  FSMap fsmap;

  ~MFSMap() override {}

public:
  std::string_view get_type_name() const override { return "fsmap"; }
  void print(ostream& out) const override {
    out << "fsmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(fsmap, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(fsmap, payload, features);
  }
};

#endif
