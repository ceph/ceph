// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef CEPH_MMGROPEN_H_
#define CEPH_MMGROPEN_H_

#include "msg/Message.h"

class MMgrOpen : public MessageInstance<MMgrOpen> {
public:
  friend factory;
private:

  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

public:

  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  bool service_daemon = false;
  std::map<std::string,std::string> daemon_metadata;
  std::map<std::string,std::string> daemon_status;

  // encode map<string,map<int32_t,string>> of current config
  bufferlist config_bl;

  // encode map<string,string> of compiled-in defaults
  bufferlist config_defaults_bl;

  void decode_payload() override
  {
    auto p = payload.cbegin();
    decode(daemon_name, p);
    if (header.version >= 2) {
      decode(service_name, p);
      decode(service_daemon, p);
      if (service_daemon) {
	decode(daemon_metadata, p);
	decode(daemon_status, p);
      }
    }
    if (header.version >= 3) {
      decode(config_bl, p);
      decode(config_defaults_bl, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(daemon_name, payload);
    encode(service_name, payload);
    encode(service_daemon, payload);
    if (service_daemon) {
      encode(daemon_metadata, payload);
      encode(daemon_status, payload);
    }
    encode(config_bl, payload);
    encode(config_defaults_bl, payload);
  }

  std::string_view get_type_name() const override { return "mgropen"; }
  void print(ostream& out) const override {
    out << get_type_name() << "(";
    if (service_name.length()) {
      out << service_name;
    } else {
      out << ceph_entity_type_name(get_source().type());
    }
    out << "." << daemon_name;
    if (service_daemon) {
      out << " daemon";
    }
    out << ")";
  }

  MMgrOpen()
    : MessageInstance(MSG_MGR_OPEN, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

