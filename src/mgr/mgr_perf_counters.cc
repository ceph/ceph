// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "mgr_perf_counters.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;

int mgr_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, "mgr", l_mgr_first, l_mgr_last);
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  plb.add_u64_counter(l_mgr_cache_hit, "cache_hit", "Cache hits");
  plb.add_u64_counter(l_mgr_cache_miss, "cache_miss", "Cache miss");

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
