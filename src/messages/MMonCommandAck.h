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

#include "common/cmdparse.h"
#include "messages/PaxosServiceMessage.h"

class MMonCommandAck : public MessageInstance<MMonCommandAck, PaxosServiceMessage> {
public:
  friend factory;

  vector<string> cmd;
  errorcode32_t r;
  string rs;
  
  MMonCommandAck() : MessageInstance(MSG_MON_COMMAND_ACK, 0) {}
  MMonCommandAck(vector<string>& c, int _r, string s, version_t v) : 
    MessageInstance(MSG_MON_COMMAND_ACK, v),
    cmd(c), r(_r), rs(s) { }
private:
  ~MMonCommandAck() override {}

public:
  std::string_view get_type_name() const override { return "mon_command"; }
  void print(ostream& o) const override {
    cmdmap_t cmdmap;
    stringstream ss;
    string prefix;
    cmdmap_from_json(cmd, &cmdmap, ss);
    cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
    // Some config values contain sensitive data, so don't log them
    o << "mon_command_ack(";
    if (prefix == "config set") {
      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      o << "[{prefix=" << prefix
        << ", name=" << name << "}]"
        << "=" << r << " " << rs << " v" << version << ")";
    } else if (prefix == "config-key set") {
      string key;
      cmd_getval(g_ceph_context, cmdmap, "key", key);
      o << "[{prefix=" << prefix << ", key=" << key << "}]"
        << "=" << r << " " << rs << " v" << version << ")";
    } else {
      o << cmd;
    }
    o << "=" << r << " " << rs << " v" << version << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(r, payload);
    encode(rs, payload);
    encode(cmd, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(r, p);
    decode(rs, p);
    decode(cmd, p);
  }
};

#endif
