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

#ifndef CEPH_MAUTH_H
#define CEPH_MAUTH_H

#include <string_view>

#include "include/encoding.h"

#include "msg/Message.h"
#include "msg/MessageRef.h"
#include "messages/PaxosServiceMessage.h"

class MAuth final : public PaxosServiceMessage {
public:
  __u32 protocol;
  ceph::buffer::list auth_payload;
  epoch_t monmap_epoch;

  /* if protocol == 0, then auth_payload is a set<__u32> listing protocols the client supports */

  MAuth() : PaxosServiceMessage{CEPH_MSG_AUTH, 0}, protocol(0), monmap_epoch(0) { }
private:
  ~MAuth() final {}

public:
  std::string_view get_type_name() const override { return "auth"; }
  void print(std::ostream& out) const override {
    out << "auth(proto " << protocol << " " << auth_payload.length() << " bytes"
	<< " epoch " << monmap_epoch << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(protocol, p);
    decode(auth_payload, p);
    if (!p.end())
      decode(monmap_epoch, p);
    else
      monmap_epoch = 0;
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(protocol, payload);
    encode(auth_payload, payload);
    encode(monmap_epoch, payload);
  }
  ceph::buffer::list& get_auth_payload() { return auth_payload; }
};

#endif
