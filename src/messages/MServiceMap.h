// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "mgr/ServiceMap.h"

class MServiceMap final : public Message {
public:
  ServiceMap service_map;

  MServiceMap() : Message{MSG_SERVICE_MAP} { }
  explicit MServiceMap(const ServiceMap& sm)
    : Message{MSG_SERVICE_MAP},
      service_map(sm) {
  }
private:
  ~MServiceMap() final {}

public:
  std::string_view get_type_name() const override { return "service_map"; }
  void print(std::ostream& out) const override {
    out << "service_map(e" << service_map.epoch << " "
	<< service_map.services.size() << " svc)";
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(service_map, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(service_map, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
