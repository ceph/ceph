// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mgr/OSDPerfMetricTypes.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/container_ios.h"
#include <ostream>

using ceph::bufferlist;

void OSDPerfMetricSubKeyDescriptor::dump(ceph::Formatter *f) const {
  f->dump_unsigned("type", static_cast<uint8_t>(type));
  f->dump_string("regex", regex_str);
}

std::list<OSDPerfMetricSubKeyDescriptor> OSDPerfMetricSubKeyDescriptor::generate_test_instances() {
  std::list<OSDPerfMetricSubKeyDescriptor> o;
  o.push_back(OSDPerfMetricSubKeyDescriptor());
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::CLIENT_ID, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::CLIENT_ADDRESS, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::POOL_ID, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::NAMESPACE, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::OSD_ID, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::PG_ID, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::OBJECT_NAME, ".*"));
  o.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::SNAP_ID, ".*"));
  return o;
}

std::ostream& operator<<(std::ostream& os,
                         const OSDPerfMetricSubKeyDescriptor &d) {
  switch(d.type) {
  case OSDPerfMetricSubKeyType::CLIENT_ID:
    os << "client_id";
    break;
  case OSDPerfMetricSubKeyType::CLIENT_ADDRESS:
    os << "client_address";
    break;
  case OSDPerfMetricSubKeyType::POOL_ID:
    os << "pool_id";
    break;
  case OSDPerfMetricSubKeyType::NAMESPACE:
    os << "namespace";
    break;
  case OSDPerfMetricSubKeyType::OSD_ID:
    os << "osd_id";
    break;
  case OSDPerfMetricSubKeyType::PG_ID:
    os << "pg_id";
    break;
  case OSDPerfMetricSubKeyType::OBJECT_NAME:
    os << "object_name";
    break;
  case OSDPerfMetricSubKeyType::SNAP_ID:
    os << "snap_id";
    break;
  default:
    os << "unknown (" << static_cast<int>(d.type) << ")";
  }
  return os << "~/" << d.regex_str << "/";
}

void denc_traits<OSDPerfMetricKeyDescriptor>::decode(OSDPerfMetricKeyDescriptor& v,
						     ceph::buffer::ptr::const_iterator& p) {
  unsigned num;
  denc_varint(num, p);
  v.clear();
  v.reserve(num);
  for (unsigned i=0; i < num; ++i) {
    OSDPerfMetricSubKeyDescriptor d;
    denc(d, p);
    if (!d.is_supported()) {
      v.clear();
      return;
    }
    try {
      d.regex = d.regex_str.c_str();
    } catch (const std::regex_error& e) {
      v.clear();
      return;
    }
    if (d.regex.mark_count() == 0) {
      v.clear();
      return;
    }
    v.push_back(std::move(d));
  }
}

void PerformanceCounterDescriptor::dump(ceph::Formatter *f) const {
  f->dump_unsigned("type", static_cast<uint8_t>(type));
}

std::list<PerformanceCounterDescriptor> PerformanceCounterDescriptor::generate_test_instances() {
  std::list<PerformanceCounterDescriptor> o;
  o.push_back(PerformanceCounterDescriptor());
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::OPS));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::WRITE_OPS));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::READ_OPS));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::BYTES));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::WRITE_BYTES));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::READ_BYTES));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::LATENCY));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::WRITE_LATENCY));
  o.push_back(PerformanceCounterDescriptor(PerformanceCounterType::READ_LATENCY));
  return o;
}

void PerformanceCounterDescriptor::pack_counter(const PerformanceCounter &c,
                                                bufferlist *bl) const {
  using ceph::encode;
  encode(c.first, *bl);
  switch(type) {
  case PerformanceCounterType::OPS:
  case PerformanceCounterType::WRITE_OPS:
  case PerformanceCounterType::READ_OPS:
  case PerformanceCounterType::BYTES:
  case PerformanceCounterType::WRITE_BYTES:
  case PerformanceCounterType::READ_BYTES:
    break;
  case PerformanceCounterType::LATENCY:
  case PerformanceCounterType::WRITE_LATENCY:
  case PerformanceCounterType::READ_LATENCY:
    encode(c.second, *bl);
    break;
  default:
    ceph_abort_msg("unknown counter type");
  }
}

void PerformanceCounterDescriptor::unpack_counter(
    bufferlist::const_iterator& bl, PerformanceCounter *c) const {
  using ceph::decode;
  decode(c->first, bl);
  switch(type) {
  case PerformanceCounterType::OPS:
  case PerformanceCounterType::WRITE_OPS:
  case PerformanceCounterType::READ_OPS:
  case PerformanceCounterType::BYTES:
  case PerformanceCounterType::WRITE_BYTES:
  case PerformanceCounterType::READ_BYTES:
    break;
  case PerformanceCounterType::LATENCY:
  case PerformanceCounterType::WRITE_LATENCY:
  case PerformanceCounterType::READ_LATENCY:
    decode(c->second, bl);
    break;
  default:
    ceph_abort_msg("unknown counter type");
  }
}

std::ostream& operator<<(std::ostream& os,
                         const PerformanceCounterDescriptor &d) {
  switch(d.type) {
  case PerformanceCounterType::OPS:
    return os << "ops";
  case PerformanceCounterType::WRITE_OPS:
    return os << "write ops";
  case PerformanceCounterType::READ_OPS:
    return os << "read ops";
  case PerformanceCounterType::BYTES:
    return os << "bytes";
  case PerformanceCounterType::WRITE_BYTES:
    return os << "write bytes";
  case PerformanceCounterType::READ_BYTES:
    return os << "read bytes";
  case PerformanceCounterType::LATENCY:
    return os << "latency";
  case PerformanceCounterType::WRITE_LATENCY:
    return os << "write latency";
  case PerformanceCounterType::READ_LATENCY:
    return os << "read latency";
  default:
    return os << "unknown (" << static_cast<int>(d.type) << ")";
  }
}

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricLimit &limit) {
  return os << "{order_by=" << limit.order_by << ", max_count="
            << limit.max_count << "}";
}

void OSDPerfMetricQuery::dump(ceph::Formatter *f) const {
  encode_json("key_descriptor", key_descriptor, f);
  encode_json("performance_counter_descriptors",
	      performance_counter_descriptors, f);
}

std::list<OSDPerfMetricQuery> OSDPerfMetricQuery::generate_test_instances() {
  std::list<OSDPerfMetricQuery> o;
  o.push_back(OSDPerfMetricQuery());
  o.push_back(OSDPerfMetricQuery(OSDPerfMetricKeyDescriptor(),
				 PerformanceCounterDescriptors()));
  o.push_back(OSDPerfMetricQuery(OSDPerfMetricKeyDescriptor(),
				 PerformanceCounterDescriptors{
				   PerformanceCounterType::WRITE_OPS,
				   PerformanceCounterType::READ_OPS,
				   PerformanceCounterType::BYTES,
				   PerformanceCounterType::WRITE_BYTES,
				   PerformanceCounterType::READ_BYTES,
				   PerformanceCounterType::LATENCY,
				   PerformanceCounterType::WRITE_LATENCY,
				   PerformanceCounterType::READ_LATENCY}));
  return o;
}

void OSDPerfMetricQuery::pack_counters(const PerformanceCounters &counters,
                                       bufferlist *bl) const {
  auto it = counters.begin();
  for (auto &descriptor : performance_counter_descriptors) {
    if (it == counters.end()) {
      descriptor.pack_counter(PerformanceCounter(), bl);
    } else {
      descriptor.pack_counter(*it, bl);
      it++;
    }
  }
}

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query) {
  return os << "{key=" << query.key_descriptor << ", counters="
            << query.performance_counter_descriptors << "}";
}

void OSDPerfMetricReport::dump(ceph::Formatter *f) const {
  encode_json("performance_counter_descriptors",
	      performance_counter_descriptors, f);
  encode_json("group_packed_performance_counters",
	      group_packed_performance_counters, f);
}

std::list<OSDPerfMetricReport> OSDPerfMetricReport::generate_test_instances() {
  std::list<OSDPerfMetricReport> o;
  o.emplace_back();
  o.emplace_back();
  o.back().performance_counter_descriptors.push_back(
    PerformanceCounterDescriptor(PerformanceCounterType::OPS));
  o.back().performance_counter_descriptors.push_back(
    PerformanceCounterDescriptor(PerformanceCounterType::WRITE_OPS));
  o.back().performance_counter_descriptors.push_back(
    PerformanceCounterDescriptor(PerformanceCounterType::READ_OPS));
  return o;
}
