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



#ifndef CEPH_MOSDALIVE_H
#define CEPH_MOSDALIVE_H

#include "messages/PaxosServiceMessage.h"

class MOSDAlive : public MessageInstance<MOSDAlive, PaxosServiceMessage> {
public:
  friend factory;

  epoch_t want = 0;

  MOSDAlive(epoch_t h, epoch_t w) : MessageInstance(MSG_OSD_ALIVE, h), want(w) { }
  MOSDAlive() : MessageInstance(MSG_OSD_ALIVE, 0) {}
private:
  ~MOSDAlive() override {}

public:
  void encode_payload(uint64_t features) override {
    paxos_encode();
    using ceph::encode;
    encode(want, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(want, p);
  }

  std::string_view get_type_name() const override { return "osd_alive"; }
  void print(ostream &out) const override {
    out << "osd_alive(want up_thru " << want << " have " << version << ")";
  }
  
};

#endif
