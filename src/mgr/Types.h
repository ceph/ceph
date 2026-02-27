// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MGR_TYPES_H
#define CEPH_MGR_TYPES_H

#include <vector>

typedef int MetricQueryID;

typedef std::pair<uint64_t,uint64_t> PerformanceCounter;
typedef std::vector<PerformanceCounter> PerformanceCounters;

struct MetricListener {
  virtual ~MetricListener() {
  }

  virtual void handle_query_updated() = 0;
};

struct PerfCollector {
  MetricQueryID query_id;
  PerfCollector(MetricQueryID query_id)
    : query_id(query_id) {
  }
};

#endif // CEPH_MGR_TYPES_H
