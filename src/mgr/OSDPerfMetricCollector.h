// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
  int get_counters(PerfCollector *collector) override;
};

#endif // OSD_PERF_METRIC_COLLECTOR_H_
