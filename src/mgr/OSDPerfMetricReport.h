// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef OSD_PERF_METRIC_REPORT_H_
#define OSD_PERF_METRIC_REPORT_H_
#include "include/denc.h"

#include "common/perf_counters.h"

struct PerformanceCounterDescriptor {
  std::string name;
  perfcounter_type_d type;
};


struct OSDPerfMetricReport {
  std::vector<PerformanceCounterDescriptor> performance_counter_descriptors;
  std::map<std::string, bufferlist> group_packed_performance_counters;

  DENC(OSDPerfMetricReport, v, p) {
      DENC_START(1, 1, p);
//      denc(v.performance_counter_descriptors, p);
      DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricReport)

#endif // OSD_PERF_METRIC_REPORT_H_
