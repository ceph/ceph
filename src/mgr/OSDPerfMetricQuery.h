// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_QUERY_H_
#define OSD_PERF_METRIC_QUERY_H_

#include "include/denc.h"

typedef int OSDPerfMetricQueryID;

struct OSDPerfMetricQuery
{
  bool operator<(const OSDPerfMetricQuery &other) const {
    return false;
  }

  DENC(OSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricQuery)

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query);

#endif // OSD_PERF_METRIC_QUERY_H_
