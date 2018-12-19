// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_COLLECTOR_H_
#define OSD_PERF_METRIC_COLLECTOR_H_

#include "common/Mutex.h"

#include "mgr/OSDPerfMetricTypes.h"

#include <map>

/**
 * OSD performance query class.
 */
class OSDPerfMetricCollector {
public:
  struct Listener {
    virtual ~Listener() {
    }

    virtual void handle_query_updated() = 0;
  };

  OSDPerfMetricCollector(Listener &listener);

  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> get_queries() const;

  OSDPerfMetricQueryID add_query(
      const OSDPerfMetricQuery& query,
      const std::optional<OSDPerfMetricLimit> &limit);
  int remove_query(OSDPerfMetricQueryID query_id);
  void remove_all_queries();

  int get_counters(OSDPerfMetricQueryID query_id,
                   std::map<OSDPerfMetricKey, PerformanceCounters> *counters);

  void process_reports(
      const std::map<OSDPerfMetricQuery, OSDPerfMetricReport> &reports);

private:
  typedef std::optional<OSDPerfMetricLimit> OptionalLimit;
  typedef std::map<OSDPerfMetricQuery,
                   std::map<OSDPerfMetricQueryID, OptionalLimit>> Queries;
  typedef std::map<OSDPerfMetricQueryID,
                   std::map<OSDPerfMetricKey, PerformanceCounters>> Counters;

  Listener &listener;
  mutable Mutex lock;
  OSDPerfMetricQueryID next_query_id = 0;
  Queries queries;
  Counters counters;
};

#endif // OSD_PERF_METRIC_COLLECTOR_H_
