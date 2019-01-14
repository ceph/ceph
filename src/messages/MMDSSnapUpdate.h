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

#ifndef CEPH_MMDSSNAPUPDATE_H
#define CEPH_MMDSSNAPUPDATE_H

#include "msg/Message.h"

class MMDSSnapUpdate : public MessageInstance<MMDSSnapUpdate> {
public:
  friend factory;
private:

  inodeno_t ino;
  __s16 snap_op;

public:
  inodeno_t get_ino() const { return ino; }
  int get_snap_op() const { return snap_op; }

  bufferlist snap_blob;

protected:
  MMDSSnapUpdate() : MessageInstance(MSG_MDS_SNAPUPDATE) {}
  MMDSSnapUpdate(inodeno_t i, version_t tid, int op) :
    MessageInstance(MSG_MDS_SNAPUPDATE), ino(i), snap_op(op) {
      set_tid(tid);
    }
  ~MMDSSnapUpdate() override {}

public:
  std::string_view get_type_name() const override { return "snap_update"; }
  void print(ostream& o) const override {
    o << "snap_update(" << ino << " table_tid " << get_tid() << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(snap_op, payload);
    encode(snap_blob, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(snap_op, p);
    decode(snap_blob, p);
  }
};

#endif
