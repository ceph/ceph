// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_MDS_PERF_COLLECTOR_H
#define CEPH_MGR_MDS_PERF_COLLECTOR_H

#include "mgr/MetricCollector.h"
#include "mgr/MDSPerfMetricTypes.h"

// MDS performance query class
class MDSPerfMetricCollector
  : public MetricCollector<MDSPerfMetricQuery, MDSPerfMetricLimit, MDSPerfMetricKey,
                           MDSPerfMetrics> {
private:
  std::set<mds_rank_t> delayed_ranks;
public:
  MDSPerfMetricCollector(MetricListener &listener);

  void process_reports(const MetricPayload &payload) override;
};

#endif // CEPH_MGR_MDS_PERF_COLLECTOR_H
