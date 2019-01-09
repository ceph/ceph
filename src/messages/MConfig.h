// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

class MConfig : public MessageInstance<MConfig> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  map<string,string> config;

  MConfig() : MessageInstance(MSG_CONFIG, HEAD_VERSION, COMPAT_VERSION) { }
  MConfig(const map<string,string>& c)
    : MessageInstance(MSG_CONFIG, HEAD_VERSION, COMPAT_VERSION),
      config(c) {}

  std::string_view get_type_name() const override {
    return "config";
  }
  void print(ostream& o) const override {
    o << "config(" << config.size() << " keys" << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(config, p);
  }

  void encode_payload(uint64_t) override {
    using ceph::encode;
    encode(config, payload);
  }

};
