// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MDSFINDINO_H
#define CEPH_MDSFINDINO_H

#include "msg/Message.h"
#include "include/filepath.h"

class MMDSFindIno : public MessageInstance<MMDSFindIno> {
public:
  friend factory;

  ceph_tid_t tid {0};
  inodeno_t ino;

protected:
  MMDSFindIno() : MessageInstance(MSG_MDS_FINDINO) {}
  MMDSFindIno(ceph_tid_t t, inodeno_t i) : MessageInstance(MSG_MDS_FINDINO), tid(t), ino(i) {}
  ~MMDSFindIno() override {}

public:
  std::string_view get_type_name() const override { return "findino"; }
  void print(ostream &out) const override {
    out << "findino(" << tid << " " << ino << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(tid, payload);
    encode(ino, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(tid, p);
    decode(ino, p);
  }
};

#endif
