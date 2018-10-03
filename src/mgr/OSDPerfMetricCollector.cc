// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "OSDPerfMetricCollector.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.osd_perf_metric_collector " << __func__ << " "

OSDPerfMetricCollector::OSDPerfMetricCollector(Listener &listener)
  : listener(listener), lock("OSDPerfMetricCollector::lock") {
}

std::list<OSDPerfMetricQuery> OSDPerfMetricCollector::get_queries() {
  Mutex::Locker locker(lock);

  std::list<OSDPerfMetricQuery> query_list;
  for (auto &it : queries) {
    query_list.push_back(it.first);
  }

  return query_list;
}

int OSDPerfMetricCollector::add_query(const OSDPerfMetricQuery& query) {
  uint64_t query_id;
  bool notify = false;

  {
    Mutex::Locker locker(lock);

    query_id = next_query_id++;
    auto it = queries.find(query);
    if (it == queries.end()) {
      it = queries.insert({query, {}}).first;
      notify = true;
    }
    it->second.insert(query_id);
  }

  dout(10) << query << " query_id=" << query_id << dendl;

  if (notify) {
    listener.handle_query_updated();
  }

  return query_id;
}

int OSDPerfMetricCollector::remove_query(int query_id) {
  bool found = false;
  bool notify = false;

  {
    Mutex::Locker locker(lock);

    for (auto it = queries.begin() ; it != queries.end(); it++) {
      auto &ids = it->second;

      if (ids.erase(query_id) > 0) {
        if (ids.empty()) {
          queries.erase(it);
          notify = true;
        }
        found = true;
        break;
      }
    }
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
    Mutex::Locker locker(lock);

    notify = !queries.empty();
    queries.clear();
  }

  if (notify) {
    listener.handle_query_updated();
  }
}
