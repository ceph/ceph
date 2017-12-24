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

#ifndef CEPH_MMONGLOBALID_H
#define CEPH_MMONGLOBALID_H

#include "messages/PaxosServiceMessage.h"

struct MMonGlobalID : public PaxosServiceMessage {
  uint64_t old_max_id;
  MMonGlobalID() : PaxosServiceMessage(MSG_MON_GLOBAL_ID, 0), old_max_id(0) { }
private:
  ~MMonGlobalID() override {}

public:
  const char *get_type_name() const override { return "global_id"; }
  void print(ostream& out) const override {
    out << "global_id  (" << old_max_id << ")";
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    decode(old_max_id, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(old_max_id, payload);
  }
};

#endif
