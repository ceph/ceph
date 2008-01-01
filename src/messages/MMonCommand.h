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

#include "msg/Message.h"

#include <vector>
using std::vector;

class MMonCommand : public Message {
 public:
  entity_inst_t inst;
  vector<string> cmd;

  MMonCommand() : Message(MSG_MON_COMMAND) {}
  MMonCommand(entity_inst_t i) : 
    Message(MSG_MON_COMMAND),
    inst(i) { }
  
  const char *get_type_name() { return "mon_command"; }
  void print(ostream& o) {
    o << "mon_command(";
    for (unsigned i=0; i<cmd.size(); i++) {
      if (i) o << ' ';
      o << cmd[i];
    }
    o << ")";
  }
  
  void encode_payload() {
    ::_encode(inst, payload);
    ::_encode(cmd, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(inst, payload, off);
    ::_decode(cmd, payload, off);
  }
};

#endif
