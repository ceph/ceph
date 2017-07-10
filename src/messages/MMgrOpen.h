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

class MMgrOpen : public Message
{
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

public:

  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  bool service_daemon = false;
  std::map<std::string,std::string> daemon_metadata;
  std::map<std::string,std::string> daemon_status;

  void decode_payload() override
  {
    bufferlist::iterator p = payload.begin();
    ::decode(daemon_name, p);
    if (header.version >= 2) {
      ::decode(service_name, p);
      ::decode(service_daemon, p);
      if (service_daemon) {
	::decode(daemon_metadata, p);
	::decode(daemon_status, p);
      }
    }
  }

  void encode_payload(uint64_t features) override {
    ::encode(daemon_name, payload);
    ::encode(service_name, payload);
    ::encode(service_daemon, payload);
    if (service_daemon) {
      ::encode(daemon_metadata, payload);
      ::encode(daemon_status, payload);
    }
  }

  const char *get_type_name() const override { return "mgropen"; }
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
    : Message(MSG_MGR_OPEN, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

