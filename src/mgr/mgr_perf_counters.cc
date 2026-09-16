// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "mgr_perf_counters.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;

int mgr_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, "mgr", l_mgr_first, l_mgr_last);
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  plb.add_u64_counter(l_mgr_cache_hit, "cache_hit", "Cache hits");
  plb.add_u64_counter(l_mgr_cache_miss, "cache_miss", "Cache miss");

  // Only GIL waits in Gil and without_gil_t are counted. A Python thread
  // taking the GIL back by itself (after I/O, sleep, a "with nogil" call
  // in the rados/cephfs/rbd bindings or a forced switch) is not, nor is
  // PyGILState_Ensure() in MgrMapCache.
  plb.add_time_avg(l_mgr_gil_acquire_lat, "gil_acquire_lat",
                   "Time to acquire the GIL in Gil");
  plb.add_time_avg(l_mgr_gil_reacquire_lat, "gil_reacquire_lat",
                   "Time to reacquire the GIL after a without_gil_t release");
  plb.add_u64_counter(l_mgr_gil_slow_waits, "gil_slow_waits",
                      "GIL acquisitions that waited longer than 5ms");

  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);
  return 0;
}

void mgr_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
}
