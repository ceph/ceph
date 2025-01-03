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

#ifndef CEPH_MCOMMAND_H
#define CEPH_MCOMMAND_H

#include <vector>

#include "msg/Message.h"

class MCommand final : public Message {
public:
  uuid_d fsid;
  std::vector<std::string> cmd;

  MCommand()
    : Message{MSG_COMMAND} {}
  MCommand(const uuid_d &f)
    : Message{MSG_COMMAND},
      fsid(f) { }

private:
  ~MCommand() final {}

public:
  std::string_view get_type_name() const override { return "command"; }
  void print(std::ostream& o) const override {
    o << "command(tid " << get_tid() << ": ";
    for (unsigned i=0; i<cmd.size(); i++) {
      if (i) o << ' ';
      o << cmd[i];
    }
    o << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(cmd, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(cmd, p);
  }
};

#endif
