// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

class MKVData : public Message {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  version_t version;
  std::string prefix;
  bool incremental = false;

  // use transparent comparator so we can lookup in it by std::string_view keys
  std::map<std::string,std::optional<bufferlist>,std::less<>> data;

  MKVData() : Message{MSG_KV_DATA, HEAD_VERSION, COMPAT_VERSION} { }

  std::string_view get_type_name() const override {
    return "kv_data";
  }
  void print(std::ostream& o) const override {
    o << "kv_data(v" << version
      << " prefix " << prefix << ", "
      << (incremental ? "incremental, " : "full, ")
      << data.size() << " keys" << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(version, p);
    decode(prefix, p);
    decode(incremental, p);
    decode(data, p);
  }

  void encode_payload(uint64_t) override {
    using ceph::encode;
    encode(version, payload);
    encode(prefix, payload);
    encode(incremental, payload);
    encode(data, payload);
  }
};
