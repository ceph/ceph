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

#ifndef CEPH_MMDSFRAGMENTNOTIFYAck_H
#define CEPH_MMDSFRAGMENTNOTIFYAck_H

#include "msg/Message.h"

class MMDSFragmentNotifyAck : public MessageInstance<MMDSFragmentNotifyAck> {
public:
  friend factory;
private:
  dirfrag_t base_dirfrag;
  int8_t bits = 0;

 public:
  dirfrag_t get_base_dirfrag() const { return base_dirfrag; }
  int get_bits() const { return bits; }

  bufferlist basebl;

protected:
  MMDSFragmentNotifyAck() : MessageInstance(MSG_MDS_FRAGMENTNOTIFYACK) {}
  MMDSFragmentNotifyAck(dirfrag_t df, int b, uint64_t tid) :
    MessageInstance(MSG_MDS_FRAGMENTNOTIFYACK),
    base_dirfrag(df), bits(b) {
    set_tid(tid);
  }
  ~MMDSFragmentNotifyAck() override {}

public:
  std::string_view get_type_name() const override { return "fragment_notify_ack"; }
  void print(ostream& o) const override {
    o << "fragment_notify_ack(" << base_dirfrag << " " << (int)bits << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(base_dirfrag, payload);
    encode(bits, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(base_dirfrag, p);
    decode(bits, p);
  }
};

#endif
