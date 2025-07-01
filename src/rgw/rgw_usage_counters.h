#pragma once

#include "common/perf_counters.h"
#include "common/ceph_context.h"

namespace rgw {

class UsageExporter {
public:
  void init() {}
  void start() {}
};

} // namespace rgw