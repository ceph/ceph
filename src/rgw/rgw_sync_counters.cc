// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw_sync_counters.h"

namespace sync_counters {

PerfCountersRef build(CephContext *cct, const std::string& name)
{
  PerfCountersBuilder b(cct, name, l_first, l_last);

  // share these counters with ceph-mgr
  b.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  b.add_u64_avg(l_fetch, "fetch bytes", "Number of object bytes replicated");
  b.add_u64_counter(l_fetch_not_modified, "fetch not modified", "Number of objects already replicated");
  b.add_u64_counter(l_fetch_err, "fetch errors", "Number of object replication errors");

  b.add_time_avg(l_poll, "poll latency", "Average latency of replication log requests");
  b.add_u64_counter(l_poll_err, "poll errors", "Number of replication log request errors");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // namespace sync_counters
