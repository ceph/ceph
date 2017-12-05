// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

struct MGetConfig : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  EntityName name;  ///< e.g., mon.a, client.foo
  string host;      ///< our hostname
  string device_class;

  MGetConfig() : Message(MSG_GET_CONFIG, HEAD_VERSION, COMPAT_VERSION) { }
  MGetConfig(const EntityName& n, const string& h)
    : Message(MSG_GET_CONFIG, HEAD_VERSION, COMPAT_VERSION),
      name(n),
      host(h) {}

  const char *get_type_name() const override {
    return "get_config";
  }
  void print(ostream& o) const override {
    o << "get_config(" << name << "@" << host;
    if (device_class.size()) {
      o << " device_class " << device_class;
    }
    o << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    bufferlist::iterator p = payload.begin();
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
