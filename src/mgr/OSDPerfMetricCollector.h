// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_COLLECTOR_H_
#define OSD_PERF_METRIC_COLLECTOR_H_

#include "common/Mutex.h"

#include "mgr/OSDPerfMetricQuery.h"

#include <list>
#include <set>

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

  std::list<OSDPerfMetricQuery> get_queries();

  OSDPerfMetricQueryID add_query(const OSDPerfMetricQuery& query);
  int remove_query(OSDPerfMetricQueryID query_id);
  void remove_all_queries();

private:
  typedef std::map<OSDPerfMetricQuery, std::set<OSDPerfMetricQueryID>> Queries;

  Listener &listener;
  mutable Mutex lock;
  OSDPerfMetricQueryID next_query_id = 0;
  Queries queries;
};

#endif // OSD_PERF_METRIC_COLLECTOR_H_
