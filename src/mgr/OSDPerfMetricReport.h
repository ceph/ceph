// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef OSD_PERF_METRIC_REPORT_H_
#define OSD_PERF_METRIC_REPORT_H_
#include "include/denc.h"
//#include "mgr/OSDPerfMetricQuery"

typedef int OSDPerfMetricQueryID;  //Temporary; to be moved in mgr/OSDPerfMetricQuery.h ?

struct OSDPerfMetricReport
{
  OSDPerfMetricQueryID query_id; 

  // Gather from OSD::PerfCounters ?
  //  client reads/s
  //  client writes/s
  //  client read bytes
  //  client write bytes
  //  client read latency
  //  client write latency

  DENC(OSDPerfMetricReport, v, p) {
      DENC_START(1, 1, p);
      denc(v.query_id, p);
      DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricReport)
#endif // OSD_PERF_METRIC_REPORT_H_
