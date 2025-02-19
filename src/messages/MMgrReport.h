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
#include "mgr/MetricTypes.h"
#include "mgr/OSDPerfMetricTypes.h"

#include "common/perf_counters.h"
#include "include/common_fwd.h"
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

  void encode(ceph::buffer::list &bl) const
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
  
  void decode(ceph::buffer::list::const_iterator &p)
  {
    DECODE_START(3, p);
    decode(path, p);
    decode(description, p);
    decode(nick, p);
    uint8_t raw_type;
    decode(raw_type, p);
    type = (enum perfcounter_type_d)raw_type;
    if (struct_v >= 2) {
      decode(priority, p);
    }
    if (struct_v >= 3) {
      uint8_t raw_unit;
      decode(raw_unit, p);
      unit = (enum unit_t)raw_unit;
    }
    DECODE_FINISH(p);
  }

  void dump(ceph::Formatter *f) const
  {
    f->dump_string("path", path);
    f->dump_string("description", description);
    f->dump_string("nick", nick);
    f->dump_int("type", type);
    f->dump_int("priority", priority);
    f->dump_int("unit", unit);
  }
  static void generate_test_instances(std::list<PerfCounterType*>& ls)
  {
    ls.push_back(new PerfCounterType);
    ls.push_back(new PerfCounterType);
    ls.back()->path = "mycounter";
    ls.back()->description = "mycounter description";
    ls.back()->nick = "mycounter nick";
    ls.back()->type = PERFCOUNTER_COUNTER;
    ls.back()->priority = PerfCountersBuilder::PRIO_CRITICAL;
    ls.back()->unit = UNIT_BYTES;
  }
};
WRITE_CLASS_ENCODER(PerfCounterType)

class MMgrReport : public Message {
private:
  static constexpr int HEAD_VERSION = 9;
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
  // the next bytes from the ceph::buffer::list.
  ceph::buffer::list packed;

  std::string daemon_name;
  std::string service_name;  // optional; otherwise infer from entity type

  // for service registration
  boost::optional<std::map<std::string,std::string>> daemon_status;
  boost::optional<std::map<std::string,std::string>> task_status;

  std::vector<DaemonHealthMetric> daemon_health_metrics;

  // encode map<string,map<int32_t,string>> of current config
  ceph::buffer::list config_bl;

  std::map<OSDPerfMetricQuery, OSDPerfMetricReport>  osd_perf_metric_reports;

  boost::optional<MetricReportMessage> metric_report_message;

  void decode_payload() override
  {
    using ceph::decode;
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
    if (header.version >= 8) {
      decode(task_status, p);
    }
    if (header.version >= 9) {
      decode(metric_report_message, p);
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
    encode(task_status, payload);
    if (metric_report_message && metric_report_message->should_encode(features)) {
      encode(metric_report_message, payload);
    } else {
      boost::optional<MetricReportMessage> empty;
      encode(empty, payload);
    }
  }

  std::string_view get_type_name() const override { return "mgrreport"; }
  void print(std::ostream& out) const override {
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
    if (task_status) {
      out << " task_status=" << task_status->size();
    }
    out << ")";
  }

private:
  MMgrReport()
    : Message{MSG_MGR_REPORT, HEAD_VERSION, COMPAT_VERSION}
  {}
  using RefCountedObject::put;
  using RefCountedObject::get;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
