// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/perf_counters_collection.h"

namespace sync_counters {

enum {
  l_first = 805000,

  l_fetch,
  l_fetch_not_modified,
  l_fetch_err,

  l_poll,
  l_poll_err,

  l_last,
};

PerfCountersRef build(CephContext *cct, const std::string& name);

} // namespace sync_counters

const std::string rgw_sync_delta_counters_key = "rgw_sync_delta";

namespace sync_deltas {

enum {
  l_rgw_sync_delta_first = 806000,
  l_rgw_datalog_sync_delta,
  l_rgw_sync_delta_last,
};

class SyncDeltaCountersManager {
  std::unique_ptr<PerfCounters> sync_delta_counters;
  CephContext *cct;

public:
  SyncDeltaCountersManager(const std::string& name, CephContext *cct);

  void tset(int idx, ceph::timespan v);

  ~SyncDeltaCountersManager();
};

} // namespace sync_deltas
