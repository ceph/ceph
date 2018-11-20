// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DYNAMIC_PERF_STATS_H
#define DYNAMIC_PERF_STATS_H

#include "mgr/OSDPerfMetricTypes.h"

class DynamicPerfStats {
public:
  DynamicPerfStats() {
  }

  DynamicPerfStats(const std::list<OSDPerfMetricQuery> &queries) {
    for (auto &query : queries) {
      data[query];
    }
  }

  void set_queries(const std::list<OSDPerfMetricQuery> &queries) {
    std::map<OSDPerfMetricQuery,
             std::map<std::string, PerformanceCounters>> new_data;
    for (auto &query : queries) {
      std::swap(new_data[query], data[query]);
    }
    std::swap(data, new_data);
  }

  bool is_enabled() {
    return !data.empty();
  }

  void add(const OpRequest& op, uint64_t inb, uint64_t outb,
           const utime_t &now) {
    for (auto &it : data) {
      auto &query = it.first;
      std::string key;
      if (query.get_key(op, &key)) {
        query.update_counters(op, inb, outb, now, &it.second[key]);
      }
    }
  }

  void add_to_reports(
      std::map<OSDPerfMetricQuery, OSDPerfMetricReport> *reports) {
    for (auto &it : data) {
      auto &query = it.first;
      auto &report = (*reports)[query];

      query.get_performance_counter_descriptors(
          &report.performance_counter_descriptors);

      for (auto &it_counters : it.second) {
        auto &bl = report.group_packed_performance_counters[it_counters.first];
        query.pack_counters(it_counters.second, &bl);
      }
    }
  }

private:
  std::map<OSDPerfMetricQuery, std::map<std::string, PerformanceCounters>> data;
};

#endif // DYNAMIC_PERF_STATS_H
