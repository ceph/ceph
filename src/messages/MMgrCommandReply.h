// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string_view>

#include "msg/Message.h"
#include "MMgrCommand.h"

class MMgrCommandReply : public Message {
public:
  errorcode32_t r;
  std::string rs;

  MMgrCommandReply()
    : Message{MSG_MGR_COMMAND_REPLY} {}
  MMgrCommandReply(MMgrCommand *m, int _r)
    : Message{MSG_MGR_COMMAND_REPLY}, r(_r) {
    header.tid = m->get_tid();
  }
  MMgrCommandReply(int _r, std::string_view s)
    : Message{MSG_MGR_COMMAND_REPLY},
      r(_r), rs(s) { }
private:
  ~MMgrCommandReply() override {}

public:
  std::string_view get_type_name() const override { return "mgr_command_reply"; }
  void print(std::ostream& o) const override {
    o << "mgr_command_reply(tid " << get_tid() << ": " << r << " " << rs << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(r, payload);
    encode(rs, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(r, p);
    decode(rs, p);
  }
};
