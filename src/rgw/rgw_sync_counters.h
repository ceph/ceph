// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
