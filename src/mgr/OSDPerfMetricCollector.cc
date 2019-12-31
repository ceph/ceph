// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "messages/MMgrReport.h"
#include "OSDPerfMetricCollector.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.osd_perf_metric_collector " << __func__ << " "

OSDPerfMetricCollector::OSDPerfMetricCollector(MetricListener &listener)
  : MetricCollector<OSDPerfMetricQuery,
                    OSDPerfMetricLimit,
                    OSDPerfMetricKey,
                    OSDPerfMetricReport>(listener) {
}

void OSDPerfMetricCollector::process_reports(const MetricPayload &payload) {
  const std::map<OSDPerfMetricQuery, OSDPerfMetricReport> &reports =
    boost::get<OSDMetricPayload>(payload).report;

  std::lock_guard locker(lock);
  process_reports_generic(
    reports, [](PerformanceCounter *counter, const PerformanceCounter &update) {
      counter->first += update.first;
      counter->second += update.second;
    });
}
