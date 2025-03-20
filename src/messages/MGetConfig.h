// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

class MGetConfig : public Message {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  EntityName name;  ///< e.g., mon.a, client.foo
  std::string host;      ///< our hostname
  std::string device_class;

  MGetConfig() : Message{MSG_GET_CONFIG, HEAD_VERSION, COMPAT_VERSION} { }
  MGetConfig(const EntityName& n, const std::string& h)
    : Message{MSG_GET_CONFIG, HEAD_VERSION, COMPAT_VERSION},
      name(n),
      host(h) {}

  std::string_view get_type_name() const override {
    return "get_config";
  }
  void print(std::ostream& o) const override {
    o << "get_config(" << name << "@" << host;
    if (device_class.size()) {
      o << " device_class " << device_class;
    }
    o << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(name, p);
    decode(host, p);
    decode(device_class, p);
  }

  void encode_payload(uint64_t) override {
    using ceph::encode;
    encode(name, payload);
    encode(host, payload);
    encode(device_class, payload);
  }
};
