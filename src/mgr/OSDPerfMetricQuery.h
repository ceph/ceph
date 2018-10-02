// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_QUERY_H_
#define OSD_PERF_METRIC_QUERY_H_

#include "include/denc.h"

typedef int OSDPerfMetricQueryID;

struct OSDPerfMetricQuery
{
  OSDPerfMetricQueryID query_id;

  DENC(OSDPerfMetricQuery, v, p) {
      DENC_START(1, 1, p);
      denc(v.query_id, p);
      DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricQuery)

#endif // OSD_PERF_METRIC_QUERY_H_
