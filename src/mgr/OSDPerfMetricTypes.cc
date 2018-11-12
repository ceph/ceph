// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/OSDPerfMetricTypes.h"

#include <ostream>

std::ostream& operator<<(std::ostream& os,
                         const OSDPerfMetricSubKeyDescriptor &d) {
  switch(d.type) {
  case OSDPerfMetricSubKeyType::CLIENT_ID:
    os << "client_id";
    break;
  case OSDPerfMetricSubKeyType::POOL_ID:
    os << "pool_id";
    break;
  case OSDPerfMetricSubKeyType::OBJECT_NAME:
    os << "object_name";
    break;
  default:
    os << "unknown (" << static_cast<int>(d.type) << ")";
  }
  return os << "~/" << d.regex_str << "/";
}

void PerformanceCounterDescriptor::pack_counter(const PerformanceCounter &c,
                                                bufferlist *bl) const {
  using ceph::encode;
  encode(c.first, *bl);
  switch(type) {
  case PerformanceCounterType::WRITE_OPS:
  case PerformanceCounterType::READ_OPS:
    break;
  case PerformanceCounterType::WRITE_BYTES:
  case PerformanceCounterType::READ_BYTES:
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
  case PerformanceCounterType::WRITE_OPS:
  case PerformanceCounterType::READ_OPS:
    break;
  case PerformanceCounterType::WRITE_BYTES:
  case PerformanceCounterType::READ_BYTES:
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
  case PerformanceCounterType::WRITE_OPS:
    return os << "write ops";
  case PerformanceCounterType::READ_OPS:
    return os << "read ops";
  case PerformanceCounterType::WRITE_BYTES:
    return os << "write bytes";
  case PerformanceCounterType::READ_BYTES:
    return os << "read bytes";
  case PerformanceCounterType::WRITE_LATENCY:
    return os << "write latency";
  case PerformanceCounterType::READ_LATENCY:
    return os << "read latency";
  default:
    return os << "unknown (" << static_cast<int>(d.type) << ")";
  }
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
