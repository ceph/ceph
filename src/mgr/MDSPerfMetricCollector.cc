// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "messages/MMgrReport.h"
#include "mgr/MDSPerfMetricTypes.h"
#include "mgr/MDSPerfMetricCollector.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.mds_perf_metric_collector " << __func__ << " "

MDSPerfMetricCollector::MDSPerfMetricCollector(MetricListener &listener)
  : MetricCollector<MDSPerfMetricQuery,
                    MDSPerfMetricLimit,
                    MDSPerfMetricKey,
                    MDSPerfMetrics>(listener) {
}

void MDSPerfMetricCollector::process_reports(const MetricPayload &payload) {
  const MDSPerfMetricReport &metric_report = boost::get<MDSMetricPayload>(payload).metric_report;

  std::lock_guard locker(lock);
  process_reports_generic(
    metric_report.reports, [](PerformanceCounter *counter, const PerformanceCounter &update) {
      counter->first = update.first;
      counter->second = update.second;
    });

  // update delayed rank set
  delayed_ranks = metric_report.rank_metrics_delayed;
  dout(20) << ": delayed ranks=[" << delayed_ranks << "]" << dendl;
}
