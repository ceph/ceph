// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

class MMgrClose : public Message {
private:

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  void decode_payload() override
  {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(daemon_name, p);
    decode(service_name, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(daemon_name, payload);
    encode(service_name, payload);
  }

  std::string_view get_type_name() const override { return "mgrclose"; }
  void print(std::ostream& out) const override {
    out << get_type_name() << "(";
    if (service_name.length()) {
      out << service_name;
    } else {
      out << ceph_entity_type_name(get_source().type());
    }
    out << "." << daemon_name;
    out << ")";
  }

  MMgrClose()
    : Message{MSG_MGR_CLOSE, HEAD_VERSION, COMPAT_VERSION}
  {}

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
