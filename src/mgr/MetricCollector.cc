// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "mgr/MetricCollector.h"
#include "mgr/OSDPerfMetricTypes.h"
#include "mgr/MDSPerfMetricTypes.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.metric_collector " << __func__ << ": "

template <typename Query, typename Limit, typename Key, typename Report>
MetricCollector<Query, Limit, Key, Report>::MetricCollector(MetricListener &listener)
  : listener(listener)
{
}

template <typename Query, typename Limit, typename Key, typename Report>
MetricQueryID MetricCollector<Query, Limit, Key, Report>::add_query(
    const Query &query,
    const std::optional<Limit> &limit) {
  dout(20) << "query=" << query << ", limit=" << limit << dendl;
  uint64_t query_id;
  bool notify = false;

  {
    std::lock_guard locker(lock);

    query_id = next_query_id++;
    auto it = queries.find(query);
    if (it == queries.end()) {
      it = queries.emplace(query, std::map<MetricQueryID, OptionalLimit>{}).first;
      notify = true;
    } else if (is_limited(it->second)) {
      notify = true;
    }

    it->second.emplace(query_id, limit);
    counters.emplace(query_id, std::map<Key, PerformanceCounters>{});
  }

  dout(10) << query << " " << (limit ? stringify(*limit) : "unlimited")
           << " query_id=" << query_id << dendl;

  if (notify) {
    listener.handle_query_updated();
  }

  return query_id;
}

template <typename Query, typename Limit, typename Key, typename Report>
int MetricCollector<Query, Limit, Key, Report>::remove_query(MetricQueryID query_id) {
  dout(20) << "query_id=" << query_id << dendl;
  bool found = false;
  bool notify = false;

  {
    std::lock_guard locker(lock);

    for (auto it = queries.begin() ; it != queries.end();) {
      auto iter = it->second.find(query_id);
      if (iter == it->second.end()) {
        ++it;
        continue;
      }

      it->second.erase(iter);
      if (it->second.empty()) {
        it = queries.erase(it);
        notify = true;
      } else if (is_limited(it->second)) {
        ++it;
        notify = true;
      }
      found = true;
      break;
    }
    counters.erase(query_id);
  }

  if (!found) {
    dout(10) << query_id << " not found" << dendl;
    return -ENOENT;
  }

  dout(10) << query_id << dendl;

  if (notify) {
    listener.handle_query_updated();
  }

  return 0;
}

template <typename Query, typename Limit, typename Key, typename Report>
void MetricCollector<Query, Limit, Key, Report>::remove_all_queries() {
  dout(20) << dendl;
  bool notify;

  {
    std::lock_guard locker(lock);

    notify = !queries.empty();
    queries.clear();
  }

  if (notify) {
    listener.handle_query_updated();
  }
}

template <typename Query, typename Limit, typename Key, typename Report>
int MetricCollector<Query, Limit, Key, Report>::get_counters(
    MetricQueryID query_id, std::map<Key, PerformanceCounters> *c) {
  dout(20) << dendl;

  std::lock_guard locker(lock);

  auto it = counters.find(query_id);
  if (it == counters.end()) {
    dout(10) << "counters for " << query_id << " not found" << dendl;
    return -ENOENT;
  }

  *c = std::move(it->second);
  it->second.clear();

  return 0;
}

template <typename Query, typename Limit, typename Key, typename Report>
void MetricCollector<Query, Limit, Key, Report>::process_reports_generic(
    const std::map<Query, Report> &reports, UpdateCallback callback) {
  ceph_assert(ceph_mutex_is_locked(lock));

  if (reports.empty()) {
    return;
  }

  for (auto& [query, report] : reports) {
    dout(10) << "report for " << query << " query: "
             << report.group_packed_performance_counters.size() << " records"
             << dendl;

    for (auto& [key, bl] : report.group_packed_performance_counters) {
      auto bl_it = bl.cbegin();

      for (auto& p : queries[query]) {
        auto &key_counters = counters[p.first][key];
        if (key_counters.empty()) {
          key_counters.resize(query.performance_counter_descriptors.size(),
                              {0, 0});
        }
      }

      auto desc_it = report.performance_counter_descriptors.begin();
      for (size_t i = 0; i < query.performance_counter_descriptors.size(); i++) {
        if (desc_it == report.performance_counter_descriptors.end()) {
          break;
        }
        if (*desc_it != query.performance_counter_descriptors[i]) {
          continue;
        }
        PerformanceCounter c;
        desc_it->unpack_counter(bl_it, &c);
        dout(20) << "counter " << key << " " << *desc_it << ": " << c << dendl;

        for (auto& p : queries[query]) {
          auto &key_counters = counters[p.first][key];
          callback(&key_counters[i], c);
        }
        desc_it++;
      }
    }
  }
}

template class
MetricCollector<OSDPerfMetricQuery, OSDPerfMetricLimit, OSDPerfMetricKey, OSDPerfMetricReport>;
template class
MetricCollector<MDSPerfMetricQuery, MDSPerfMetricLimit, MDSPerfMetricKey, MDSPerfMetrics>;
