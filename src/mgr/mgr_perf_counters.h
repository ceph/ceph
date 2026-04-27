// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once
#include "include/common_fwd.h"

extern PerfCounters* perfcounter;

extern int mgr_perf_start(CephContext* cct);
extern void mgr_perf_stop(CephContext* cct);

enum {
  l_mgr_first,

  l_mgr_cache_hit,
  l_mgr_cache_miss,

  l_mgr_last,
};

