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

#ifndef __MMONCOMMAND_H
#define __MMONCOMMAND_H

#include "messages/PaxosServiceMessage.h"

#include <vector>
using std::vector;

class MMonCommand : public PaxosServiceMessage {
 public:
  ceph_fsid_t fsid;
  vector<string> cmd;

  MMonCommand() : PaxosServiceMessage(MSG_MON_COMMAND, 0) {}
  MMonCommand(ceph_fsid_t &f, version_t version) : 
    PaxosServiceMessage(MSG_MON_COMMAND, version),
    fsid(f) { }

private:
  ~MMonCommand() {}

public:  
  const char *get_type_name() { return "mon_command"; }
  void print(ostream& o) {
    o << "mon_command(";
    for (unsigned i=0; i<cmd.size(); i++) {
      if (i) o << ' ';
      o << cmd[i];
    }
    o << " v " << version << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(cmd, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(cmd, p);
  }
};

#endif
