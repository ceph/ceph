// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

struct MConfig : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  map<string,string> config;

  MConfig() : Message(MSG_CONFIG, HEAD_VERSION, COMPAT_VERSION) { }
  MConfig(const map<string,string>& c)
    : Message(MSG_CONFIG, HEAD_VERSION, COMPAT_VERSION),
      config(c) {}

  const char *get_type_name() const override {
    return "config";
  }
  void print(ostream& o) const override {
    o << "config(" << config.size() << " keys" << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    bufferlist::iterator p = payload.begin();
    decode(config, p);
  }

  void encode_payload(uint64_t) override {
    using ceph::encode;
    encode(config, payload);
  }

};
