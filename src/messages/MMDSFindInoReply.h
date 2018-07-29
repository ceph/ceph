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

#ifndef CEPH_MDSFINDINOREPLY_H
#define CEPH_MDSFINDINOREPLY_H

#include "msg/Message.h"
#include "include/filepath.h"

class MMDSFindInoReply : public Message {
public:
  typedef boost::intrusive_ptr<MMDSFindInoReply> ref;
  typedef boost::intrusive_ptr<MMDSFindInoReply const> const_ref;
  ceph_tid_t tid = 0;
  filepath path;

  MMDSFindInoReply() : Message(MSG_MDS_FINDINOREPLY) {}
  MMDSFindInoReply(ceph_tid_t t) : Message(MSG_MDS_FINDINOREPLY), tid(t) {}

  const char *get_type_name() const override { return "findinoreply"; }
  void print(ostream &out) const override {
    out << "findinoreply(" << tid << " " << path << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(tid, payload);
    encode(path, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(tid, p);
    decode(path, p);
  }
};

#endif
