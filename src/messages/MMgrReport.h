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


#ifndef CEPH_MMGRREPORT_H_
#define CEPH_MMGRREPORT_H_

#include <boost/optional.hpp>

#include "msg/Message.h"

#include "common/perf_counters.h"

class PerfCounterType
{
public:
  std::string path;
  std::string description;
  std::string nick;
  enum perfcounter_type_d type;

  void encode(bufferlist &bl) const
  {
    // TODO: decide whether to drop the per-type
    // encoding here, we could rely on the MgrReport
    // verisoning instead.
    ENCODE_START(1, 1, bl);
    ::encode(path, bl);
    ::encode(description, bl);
    ::encode(nick, bl);
    static_assert(sizeof(type) == 1, "perfcounter_type_d must be one byte");
    ::encode((uint8_t)type, bl);
    ENCODE_FINISH(bl);
  }
  
  void decode(bufferlist::iterator &p)
  {
    DECODE_START(1, p);
    ::decode(path, p);
    ::decode(description, p);
    ::decode(nick, p);
    ::decode((uint8_t&)type, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(PerfCounterType)

class MMgrReport : public Message
{
  static const int HEAD_VERSION = 4;
  static const int COMPAT_VERSION = 1;

public:
  /**
   * Client is responsible for remembering whether it has introduced
   * each perf counter to the server.  When first sending a particular
   * counter, it must inline the counter's schema here.
   */
  std::vector<PerfCounterType> declare_types;
  std::vector<std::string> undeclare_types;

  // For all counters present, sorted by idx, output
  // as many bytes as are needed to represent them

  // Decode: iterate over the types we know about, sorted by idx,
  // and use the current type's type to decide how to decode
  // the next bytes from the bufferlist.
  bufferlist packed;

  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  // for service registration
  boost::optional<std::map<std::string,std::string>> daemon_status;

  void decode_payload() override
  {
    bufferlist::iterator p = payload.begin();
    ::decode(daemon_name, p);
    ::decode(declare_types, p);
    ::decode(packed, p);
    if (header.version >= 2)
      ::decode(undeclare_types, p);
    if (header.version >= 3) {
      ::decode(service_name, p);
      ::decode(daemon_status, p);
    }
  }

  void encode_payload(uint64_t features) override {
    ::encode(daemon_name, payload);
    ::encode(declare_types, payload);
    ::encode(packed, payload);
    ::encode(undeclare_types, payload);
    ::encode(service_name, payload);
    ::encode(daemon_status, payload);
  }

  const char *get_type_name() const override { return "mgrreport"; }
  void print(ostream& out) const override {
    out << get_type_name() << "(";
    if (service_name.length()) {
      out << service_name;
    } else {
      out << ceph_entity_type_name(get_source().type());
    }
    out << "." << daemon_name
	<< " +" << declare_types.size()
	<< "-" << undeclare_types.size()
        << " packed " << packed.length();
    if (daemon_status) {
      out << " status=" << daemon_status->size();
    }
    out << ")";
  }

  MMgrReport()
    : Message(MSG_MGR_REPORT, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

