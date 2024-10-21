// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "MMonCommand.h"

#include <sstream>

#include "common/cmdparse.h" // for cmdmap_from_json()
#include "include/container_ios.h"
#include "include/encoding_string.h"
#include "include/encoding_vector.h"

using ceph::common::cmdmap_from_json;
using ceph::common::cmd_getval;

void MMonCommand::print(std::ostream& o) const {
  cmdmap_t cmdmap;
  std::ostringstream ss;
  std::string prefix;
  cmdmap_from_json(cmd, &cmdmap, ss);
  cmd_getval(cmdmap, "prefix", prefix);
  // Some config values contain sensitive data, so don't log them
  o << "mon_command(";
  if (prefix == "config set") {
    std::string name;
    cmd_getval(cmdmap, "name", name);
    o << "[{prefix=" << prefix << ", name=" << name << "}]";
  } else if (prefix == "config-key set") {
    std::string key;
    cmd_getval(cmdmap, "key", key);
    o << "[{prefix=" << prefix << ", key=" << key << "}]";
  } else {
    for (unsigned i=0; i<cmd.size(); i++) {
      if (i) o << ' ';
      o << cmd[i];
    }
  }
  o << " v " << version << ")";
}

void MMonCommand::encode_payload(uint64_t features) {
  using ceph::encode;
  paxos_encode();
  encode(fsid, payload);
  encode(cmd, payload);
}

void MMonCommand::decode_payload() {
  using ceph::decode;
  auto p = payload.cbegin();
  paxos_decode(p);
  decode(fsid, p);
  decode(cmd, p);
}
