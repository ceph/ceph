// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "OSDPerfMetricCollector.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.osd_perf_metric_collector " << __func__ << " "

namespace {

bool is_limited(const std::map<OSDPerfMetricQueryID,
                                std::optional<OSDPerfMetricLimit>> &limits) {
  for (auto &it : limits) {
    if (!it.second) {
      return false;
    }
  }
  return true;
}

} // anonymous namespace

OSDPerfMetricCollector::OSDPerfMetricCollector(Listener &listener)
  : listener(listener), lock("OSDPerfMetricCollector::lock") {
}

std::map<OSDPerfMetricQuery, OSDPerfMetricLimits>
OSDPerfMetricCollector::get_queries() const {
  std::lock_guard locker(lock);

  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> result;
  for (auto &it : queries) {
    auto &query = it.first;
    auto &limits = it.second;
    auto result_it = result.insert({query, {}}).first;
    if (is_limited(limits)) {
      for (auto &iter : limits) {
        result_it->second.insert(*iter.second);
      }
    }
  }

  return result;
}

OSDPerfMetricQueryID OSDPerfMetricCollector::add_query(
    const OSDPerfMetricQuery& query,
    const std::optional<OSDPerfMetricLimit> &limit) {
  uint64_t query_id;
  bool notify = false;

  {
    std::lock_guard locker(lock);

    query_id = next_query_id++;
    auto it = queries.find(query);
    if (it == queries.end()) {
      it = queries.insert({query, {}}).first;
      notify = true;
    } else if (is_limited(it->second)) {
      notify = true;
    }
    it->second.insert({query_id, limit});
    counters[query_id];
  }

  dout(10) << query << " " << (limit ? stringify(*limit) : "unlimited")
           << " query_id=" << query_id << dendl;

  if (notify) {
    listener.handle_query_updated();
  }

  return query_id;
}

int OSDPerfMetricCollector::remove_query(int query_id) {
  bool found = false;
  bool notify = false;

  {
    std::lock_guard locker(lock);

    for (auto it = queries.begin() ; it != queries.end(); it++) {
      auto iter = it->second.find(query_id);
      if (iter == it->second.end()) {
        continue;
      }

      it->second.erase(iter);
      if (it->second.empty()) {
        queries.erase(it);
        notify = true;
      } else if (is_limited(it->second)) {
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

void OSDPerfMetricCollector::remove_all_queries() {
  dout(10) << dendl;

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

int OSDPerfMetricCollector::get_counters(
    OSDPerfMetricQueryID query_id,
    std::map<OSDPerfMetricKey, PerformanceCounters> *c) {
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

void OSDPerfMetricCollector::process_reports(
    const std::map<OSDPerfMetricQuery, OSDPerfMetricReport> &reports) {

  if (reports.empty()) {
    return;
  }

  std::lock_guard locker(lock);

  for (auto &it : reports) {
    auto &query = it.first;
    auto &report = it.second;
    dout(10) << "report for " << query << " query: "
             << report.group_packed_performance_counters.size() << " records"
             << dendl;

    for (auto &it : report.group_packed_performance_counters) {
      auto &key = it.first;
      auto bl_it = it.second.cbegin();

      for (auto &queries_it : queries[query]) {
        auto query_id = queries_it.first;
        auto &key_counters = counters[query_id][key];
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

        for (auto &queries_it : queries[query]) {
          auto query_id = queries_it.first;
          auto &key_counters = counters[query_id][key];
          key_counters[i].first += c.first;
          key_counters[i].second += c.second;
        }
        desc_it++;
      }
    }
  }
}
