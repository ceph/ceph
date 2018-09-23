// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_H_
#define OSD_PERF_METRIC_H_

#include "include/denc.h"
#include "include/stringify.h"

#include <ostream>

class OpRequest;
class utime_t;

typedef std::pair<uint64_t,uint64_t> PerformanceCounter;
typedef std::vector<PerformanceCounter> PerformanceCounters;

enum class PerformanceCounterType : uint8_t {
  WRITE_OPS = 0,
  READ_OPS = 1,
  WRITE_BYTES = 2,
  READ_BYTES = 3,
  WRITE_LATENCY = 4,
  READ_LATENCY = 5,
};

struct PerformanceCounterDescriptor {
  PerformanceCounterType type = static_cast<PerformanceCounterType>(-1);

  PerformanceCounterDescriptor() {
  }

  PerformanceCounterDescriptor(PerformanceCounterType type) : type(type) {
  }

  DENC(PerformanceCounterDescriptor, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    DENC_FINISH(p);
  }

  void update_counter(const OpRequest& op, uint64_t inb, uint64_t outb,
                      const utime_t &now, PerformanceCounter *c) const;
  void pack_counter(const PerformanceCounter &c, bufferlist *bl) const;
  void unpack_counter(bufferlist::const_iterator& bl,
                      PerformanceCounter *c) const;
};
WRITE_CLASS_DENC(PerformanceCounterDescriptor)

std::ostream& operator<<(std::ostream& os,
                         const PerformanceCounterDescriptor &d);

typedef int OSDPerfMetricQueryID;

struct OSDPerfMetricQuery {
  bool operator<(const OSDPerfMetricQuery &other) const {
    return false;
  }

  bool get_key(const OpRequest& op, std::string *key) const;

  DENC(OSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    denc(v.performance_counter_descriptors, p);
    DENC_FINISH(p);
  }

  void get_performance_counter_descriptors(
      std::vector<PerformanceCounterDescriptor> *descriptors) const {
    *descriptors = performance_counter_descriptors;
  }

  void update_counters(const OpRequest& op, uint64_t inb, uint64_t outb,
                       const utime_t &now, PerformanceCounters *counters) const;
  void pack_counters(const PerformanceCounters &counters, bufferlist *bl) const;

  std::vector<PerformanceCounterDescriptor> performance_counter_descriptors = {
    {PerformanceCounterType::WRITE_OPS},
    {PerformanceCounterType::READ_OPS},
    {PerformanceCounterType::WRITE_BYTES},
    {PerformanceCounterType::READ_BYTES},
    {PerformanceCounterType::WRITE_LATENCY},
    {PerformanceCounterType::READ_LATENCY},
  };
};
WRITE_CLASS_DENC(OSDPerfMetricQuery)

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query);

struct OSDPerfMetricReport {
  std::vector<PerformanceCounterDescriptor> performance_counter_descriptors;
  std::map<std::string, bufferlist> group_packed_performance_counters;

  DENC(OSDPerfMetricReport, v, p) {
    DENC_START(1, 1, p);
    denc(v.performance_counter_descriptors, p);
    denc(v.group_packed_performance_counters, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricReport)

#endif // OSD_PERF_METRIC_H_

