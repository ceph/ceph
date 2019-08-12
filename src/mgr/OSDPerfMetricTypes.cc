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
