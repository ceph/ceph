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

#ifndef CEPH_MMONCOMMANDACK_H
#define CEPH_MMONCOMMANDACK_H

#include "messages/PaxosServiceMessage.h"

class MMonCommandAck : public MessageInstance<MMonCommandAck, PaxosServiceMessage> {
public:
  friend factory;

  std::vector<std::string> cmd;
  errorcode32_t r;
  std::string rs;

  MMonCommandAck() : MessageInstance(MSG_MON_COMMAND_ACK, 0) {}
  MMonCommandAck(std::vector<std::string>& c, int _r, std::string s, version_t v) : 
    MessageInstance(MSG_MON_COMMAND_ACK, v),
    cmd(c), r(_r), rs(s) { }
private:
  ~MMonCommandAck() override {}

public:
  std::string_view get_type_name() const override { return "mon_command"; }
  void print(std::ostream& o) const override {
    o << "mon_command_ack(" << cmd << "=" << r << " " << rs << " v" << version << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(r, payload);
    encode(rs, payload);
    encode(cmd, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(r, p);
    decode(rs, p);
    decode(cmd, p);
  }
};

#endif
