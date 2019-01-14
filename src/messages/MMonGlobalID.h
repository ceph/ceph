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

class MMonGlobalID : public MessageInstance<MMonGlobalID, PaxosServiceMessage> {
public:
  friend factory;

  uint64_t old_max_id;
  MMonGlobalID() : MessageInstance(MSG_MON_GLOBAL_ID, 0), old_max_id(0) { }
private:
  ~MMonGlobalID() override {}

public:
  std::string_view get_type_name() const override { return "global_id"; }
  void print(ostream& out) const override {
    out << "global_id  (" << old_max_id << ")";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
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
