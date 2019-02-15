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
#include "mgr/OSDPerfMetricTypes.h"

#include "common/perf_counters.h"
#include "mgr/DaemonHealthMetric.h"

class PerfCounterType
{
public:
  std::string path;
  std::string description;
  std::string nick;
  enum perfcounter_type_d type;

  // For older clients that did not send priority, pretend everything
  // is "useful" so that mgr plugins filtering on prio will get some
  // data (albeit probably more than they wanted)
  uint8_t priority = PerfCountersBuilder::PRIO_USEFUL;
  enum unit_t unit;

  void encode(bufferlist &bl) const
  {
    // TODO: decide whether to drop the per-type
    // encoding here, we could rely on the MgrReport
    // verisoning instead.
    ENCODE_START(3, 1, bl);
    encode(path, bl);
    encode(description, bl);
    encode(nick, bl);
    static_assert(sizeof(type) == 1, "perfcounter_type_d must be one byte");
    encode((uint8_t)type, bl);
    encode(priority, bl);
    encode((uint8_t)unit, bl);
    ENCODE_FINISH(bl);
  }
  
  void decode(bufferlist::const_iterator &p)
  {
    DECODE_START(3, p);
    decode(path, p);
    decode(description, p);
    decode(nick, p);
    decode((uint8_t&)type, p);
    if (struct_v >= 2) {
      decode(priority, p);
    }
    if (struct_v >= 3) {
      decode((uint8_t&)unit, p);
    }
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(PerfCounterType)

class MMgrReport : public MessageInstance<MMgrReport> {
public:
  friend factory;
private:

  static constexpr int HEAD_VERSION = 7;
  static constexpr int COMPAT_VERSION = 1;

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

  std::vector<DaemonHealthMetric> daemon_health_metrics;

  // encode map<string,map<int32_t,string>> of current config
  bufferlist config_bl;

  std::map<OSDPerfMetricQuery, OSDPerfMetricReport>  osd_perf_metric_reports;

  void decode_payload() override
  {
    auto p = payload.cbegin();
    decode(daemon_name, p);
    decode(declare_types, p);
    decode(packed, p);
    if (header.version >= 2)
      decode(undeclare_types, p);
    if (header.version >= 3) {
      decode(service_name, p);
      decode(daemon_status, p);
    }
    if (header.version >= 5) {
      decode(daemon_health_metrics, p);
    }
    if (header.version >= 6) {
      decode(config_bl, p);
    }
    if (header.version >= 7) {
      decode(osd_perf_metric_reports, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(daemon_name, payload);
    encode(declare_types, payload);
    encode(packed, payload);
    encode(undeclare_types, payload);
    encode(service_name, payload);
    encode(daemon_status, payload);
    encode(daemon_health_metrics, payload);
    encode(config_bl, payload);
    encode(osd_perf_metric_reports, payload);
  }

  std::string_view get_type_name() const override { return "mgrreport"; }
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
    if (!daemon_health_metrics.empty()) {
      out << " daemon_metrics=" << daemon_health_metrics.size();
    }
    out << ")";
  }

  MMgrReport()
    : MessageInstance(MSG_MGR_REPORT, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

