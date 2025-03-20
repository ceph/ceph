// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Prashant D <pdhange@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef CEPH_MMGRUPDATE_H_
#define CEPH_MMGRUPDATE_H_

#include "msg/Message.h"

class MMgrUpdate : public Message {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:

  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  std::map<std::string,std::string> daemon_metadata;
  std::map<std::string,std::string> daemon_status;

  bool need_metadata_update = false;

  void decode_payload() override
  {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(daemon_name, p);
    if (header.version >= 2) {
      decode(service_name, p);
      decode(need_metadata_update, p);
      if (need_metadata_update) {
	decode(daemon_metadata, p);
	decode(daemon_status, p);
      }
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(daemon_name, payload);
    encode(service_name, payload);
    encode(need_metadata_update, payload);
    if (need_metadata_update) {
      encode(daemon_metadata, payload);
      encode(daemon_status, payload);
    }
  }

  std::string_view get_type_name() const override { return "mgrupdate"; }
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

private:
  MMgrUpdate()
    : Message{MSG_MGR_UPDATE, HEAD_VERSION, COMPAT_VERSION}
  {}
  using RefCountedObject::put;
  using RefCountedObject::get;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif

