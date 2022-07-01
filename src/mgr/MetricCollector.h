// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_METRIC_COLLECTOR_H
#define CEPH_MGR_METRIC_COLLECTOR_H

#include <map>
#include <set>
#include <tuple>
#include <vector>
#include <utility>
#include <algorithm>

#include "common/ceph_mutex.h"
#include "msg/Message.h"
#include "mgr/Types.h"
#include "mgr/MetricTypes.h"

class MMgrReport;

template <typename Query, typename Limit, typename Key, typename Report>
class MetricCollector {
public:
  virtual ~MetricCollector() {
  }

  using Limits = std::set<Limit>;

  MetricCollector(MetricListener &listener);

  MetricQueryID add_query(const Query &query, const std::optional<Limit> &limit);

  int remove_query(MetricQueryID query_id);

  void remove_all_queries();

  void reregister_queries();

  std::map<Query, Limits> get_queries() const {
    std::lock_guard locker(lock);

    std::map<Query, Limits> result;
    for (auto& [query, limits] : queries) {
      auto result_it = result.insert({query, {}}).first;
      if (is_limited(limits)) {
        for (auto& limit : limits) {
          if (limit.second) {
            result_it->second.insert(*limit.second);
          }
        }
      }
    }

    return result;
  }

  virtual void process_reports(const MetricPayload &payload) = 0;
  virtual int get_counters(PerfCollector *collector) = 0;

protected:
  typedef std::optional<Limit> OptionalLimit;
  typedef std::map<MetricQueryID, OptionalLimit> QueryIDLimit;
  typedef std::map<Query, QueryIDLimit> Queries;
  typedef std::map<MetricQueryID, std::map<Key, PerformanceCounters>> Counters;
  typedef std::function<void(PerformanceCounter *, const PerformanceCounter &)> UpdateCallback;

  mutable ceph::mutex lock = ceph::make_mutex("mgr::metric::collector::lock");

  Queries queries;
  Counters counters;

  void process_reports_generic(const std::map<Query, Report> &reports, UpdateCallback callback);
  int get_counters_generic(MetricQueryID query_id, std::map<Key, PerformanceCounters> *counters);

private:
  MetricListener &listener;
  MetricQueryID next_query_id = 0;

  bool is_limited(const std::map<MetricQueryID, OptionalLimit> &limits) const {
    return std::any_of(begin(limits), end(limits),
                       [](auto &limits) { return limits.second.has_value(); });
  }
};

#endif // CEPH_MGR_METRIC_COLLECTOR_H
