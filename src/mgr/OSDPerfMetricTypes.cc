// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/OSDPerfMetricTypes.h"
#include "messages/MOSDOp.h"
#include "osd/OpRequest.h"

void PerformanceCounterDescriptor::update_counter(
    const OpRequest& op, uint64_t inb, uint64_t outb, const utime_t &now,
    PerformanceCounter *c) const {
  switch(type) {
  case PerformanceCounterType::WRITE_OPS:
    if (op.may_write() || op.may_cache()) {
      c->first++;
    }
    return;
  case PerformanceCounterType::READ_OPS:
    if (op.may_read()) {
      c->first++;
    }
    return;
  case PerformanceCounterType::WRITE_BYTES:
    if (op.may_write() || op.may_cache()) {
      c->first += inb;
      c->second++;
    }
    return;
  case PerformanceCounterType::READ_BYTES:
    if (op.may_read()) {
      c->first += outb;
      c->second++;
    }
    return;
  case PerformanceCounterType::WRITE_LATENCY:
    if (op.may_write() || op.may_cache()) {
      const MOSDOp* const m = static_cast<const MOSDOp*>(op.get_req());
      c->first += (now - m->get_recv_stamp()).to_nsec();
      c->second++;
    }
    return;
  case PerformanceCounterType::READ_LATENCY:
    if (op.may_read()) {
      const MOSDOp* const m = static_cast<const MOSDOp*>(op.get_req());
      c->first += (now - m->get_recv_stamp()).to_nsec();
      c->second++;
    }
    return;
  default:
    ceph_abort_msg("unknown counter type");
  }
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
    return os << "unknown (" << d.type << ")";
  }
}

bool OSDPerfMetricQuery::get_key(const OpRequest& op, std::string *key) const {
  const MOSDOp* const m = static_cast<const MOSDOp*>(op.get_req());

  *key = stringify(m->get_reqid().name);
  return true;
}

void OSDPerfMetricQuery::update_counters(const OpRequest& op, uint64_t inb,
                                         uint64_t outb, const utime_t &now,
                                         PerformanceCounters *counters) const {
  auto it = counters->begin();
  for (auto &descriptor : performance_counter_descriptors) {
    // TODO: optimize
    if (it == counters->end()) {
      counters->push_back(PerformanceCounter());
      it = std::prev(counters->end());
    }
    descriptor.update_counter(op, inb, outb, now, &(*it));
    it++;
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
  return os << "simple";
}
