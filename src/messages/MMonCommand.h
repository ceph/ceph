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

#ifndef CEPH_MMONCOMMAND_H
#define CEPH_MMONCOMMAND_H

#include "common/cmdparse.h"
#include "messages/PaxosServiceMessage.h"

#include <vector>
#include <string>

class MMonCommand : public MessageInstance<MMonCommand, PaxosServiceMessage> {
public:
  friend factory;

  uuid_d fsid;
  std::vector<std::string> cmd;

  MMonCommand() : MessageInstance(MSG_MON_COMMAND, 0) {}
  MMonCommand(const uuid_d &f)
    : MessageInstance(MSG_MON_COMMAND, 0),
      fsid(f)
  { }

private:
  ~MMonCommand() override {}

public:  
  std::string_view get_type_name() const override { return "mon_command"; }
  void print(ostream& o) const override {
    cmdmap_t cmdmap;
    stringstream ss;
    string prefix;
    cmdmap_from_json(cmd, &cmdmap, ss);
    cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
    // Some config values contain sensitive data, so don't log them
    o << "mon_command(";
    if (prefix == "config set") {
      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      o << "[{prefix=" << prefix << ", name=" << name << "}]";
    } else if (prefix == "config-key set") {
      string key;
      cmd_getval(g_ceph_context, cmdmap, "key", key);
      o << "[{prefix=" << prefix << ", key=" << key << "}]";
    } else {
      for (unsigned i=0; i<cmd.size(); i++) {
        if (i) o << ' ';
        o << cmd[i];
      }
    }
    o << " v " << version << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(cmd, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(cmd, p);
  }
};

#endif
