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

#ifndef CEPH_MCOMMANDREPLY_H
#define CEPH_MCOMMANDREPLY_H

#include <string_view>

#include "msg/Message.h"
#include "MCommand.h"

class MCommandReply : public Message {
 public:
  errorcode32_t r;
  string rs;
  
  MCommandReply()
    : Message(MSG_COMMAND_REPLY) {}
  MCommandReply(MCommand *m, int _r)
    : Message(MSG_COMMAND_REPLY), r(_r) {
    header.tid = m->get_tid();
  }
  MCommandReply(int _r, std::string_view s)
    : Message(MSG_COMMAND_REPLY),
      r(_r), rs(s) { }
private:
  ~MCommandReply() override {}

public:
  const char *get_type_name() const override { return "command_reply"; }
  void print(ostream& o) const override {
    o << "command_reply(tid " << get_tid() << ": " << r << " " << rs << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(r, payload);
    encode(rs, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(r, p);
    decode(rs, p);
  }
};

#endif
