// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_TYPES_H
#define CEPH_MGR_TYPES_H

typedef int MetricQueryID;

typedef std::pair<uint64_t,uint64_t> PerformanceCounter;
typedef std::vector<PerformanceCounter> PerformanceCounters;

struct MetricListener {
  virtual ~MetricListener() {
  }

  virtual void handle_query_updated() = 0;
};

#endif // CEPH_MGR_TYPES_H
