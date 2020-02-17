// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_COLLECTOR_H_
#define OSD_PERF_METRIC_COLLECTOR_H_

#include "mgr/MetricCollector.h"
#include "mgr/OSDPerfMetricTypes.h"

/**
 * OSD performance query class.
 */
class OSDPerfMetricCollector
  : public MetricCollector<OSDPerfMetricQuery, OSDPerfMetricLimit, OSDPerfMetricKey,
                           OSDPerfMetricReport> {
public:
  OSDPerfMetricCollector(MetricListener &listener);

  void process_reports(const MetricPayload &payload) override;
};

#endif // OSD_PERF_METRIC_COLLECTOR_H_
