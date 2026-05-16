// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#ifdef WITH_SEASTORE_WAF_COUNTERS

#include "common/perf_counters.h"
#include "crimson/common/perf_counters_collection.h"
#include "include/common_fwd.h"

namespace crimson::os::seastore {

// Perf counter ids for the seastore_waf logger group.  The "WAF" name
// reflects the ratio device_written / user_written that this group is
// designed to expose; both raw counters are monotonic byte totals.
enum {
  l_seastore_waf_first = 1100000,
  l_seastore_bytes_user_written,
  l_seastore_bytes_device_written,
  l_seastore_waf_last,
};

// Build and register a seastore_waf PerfCounters logger with cct's
// PerfCountersCollection.  The caller owns the returned reference;
// when it is destroyed the counters are automatically removed from
// the collection (see PerfCountersDeleter).
PerfCountersRef build_seastore_waf_logger(CephContext *cct);

}  // namespace crimson::os::seastore

#endif  // WITH_SEASTORE_WAF_COUNTERS
