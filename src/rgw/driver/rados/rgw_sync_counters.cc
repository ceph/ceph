// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/ceph_context.h"
#include "rgw_sync_counters.h"
#include "common/perf_counters_key.h"

namespace sync_counters {

PerfCountersRef build(CephContext *cct, const std::string& name)
{
  PerfCountersBuilder b(cct, name, l_first, l_last);

  // share these counters with ceph-mgr
  b.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  b.add_u64_avg(l_fetch, "fetch_bytes", "Number of object bytes replicated");
  b.add_u64_counter(l_fetch_not_modified, "fetch_not_modified", "Number of objects already replicated");
  b.add_u64_counter(l_fetch_err, "fetch_errors", "Number of object replication errors");

  b.add_time_avg(l_poll, "poll_latency", "Average latency of replication log requests");
  b.add_u64_counter(l_poll_err, "poll_errors", "Number of replication log request errors");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // namespace sync_counters

namespace sync_deltas {

void add_rgw_sync_delta_counters(PerfCountersBuilder *lpcb) {
  lpcb->set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  lpcb->add_time(l_rgw_datalog_sync_delta, "sync_delta", "Sync delta between data log shard in seconds");
}

SyncDeltaCountersManager::SyncDeltaCountersManager(const std::string& name, CephContext *cct)
    : cct(cct)
{
  std::string_view key = ceph::perf_counters::key_name(name);
  ceph_assert(rgw_sync_delta_counters_key == key);
  PerfCountersBuilder pcb(cct, name, l_rgw_sync_delta_first, l_rgw_sync_delta_last);
  add_rgw_sync_delta_counters(&pcb);
  sync_delta_counters = std::unique_ptr<PerfCounters>(pcb.create_perf_counters());
  cct->get_perfcounters_collection()->add(sync_delta_counters.get());
}

void SyncDeltaCountersManager::tset(int idx, ceph::timespan  v) {
  sync_delta_counters->tset(idx, v);
}

SyncDeltaCountersManager::~SyncDeltaCountersManager() {
  cct->get_perfcounters_collection()->remove(sync_delta_counters.get());
}

} // namespace sync_deltas
