#pragma once

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#define TOPNSPC crimson
#else
#define TOPNSPC ceph
#endif

namespace TOPNSPC::common {
  class CephContext;
  class PerfCounters;
  class PerfCountersBuilder;
  class PerfCountersCollectionImpl;
  class PerfGuard;
}
using TOPNSPC::common::CephContext;
using TOPNSPC::common::PerfCounters;
using TOPNSPC::common::PerfCountersBuilder;
using TOPNSPC::common::PerfCountersCollectionImpl;
using TOPNSPC::common::PerfGuard;
