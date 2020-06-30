// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>

#include "msg/Message.h"

class MMgrCommand : public Message {
public:
  uuid_d fsid;
  std::vector<std::string> cmd;

  MMgrCommand()
    : Message{MSG_MGR_COMMAND} {}
  MMgrCommand(const uuid_d &f)
    : Message{MSG_MGR_COMMAND},
      fsid(f) { }

private:
  ~MMgrCommand() override {}

public:
  std::string_view get_type_name() const override { return "mgr_command"; }
  void print(std::ostream& o) const override {
    o << "mgr_command(tid " << get_tid() << ": ";
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
