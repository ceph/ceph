#pragma once

#ifdef WITH_CRIMSON
#define TOPNSPC crimson
#else
#define TOPNSPC ceph
#endif

namespace TOPNSPC::common {
  class CephContext;
  class PerfCounters;
  class PerfCountersBuilder;
  class PerfCountersCollection;
  class PerfCountersCollectionImpl;
  class PerfGuard;
  class RefCountedObject;
  class RefCountedObjectSafe;
  class RefCountedCond;
  class RefCountedWaitObject;
  class ConfigProxy;
}
using TOPNSPC::common::CephContext;
using TOPNSPC::common::PerfCounters;
using TOPNSPC::common::PerfCountersBuilder;
using TOPNSPC::common::PerfCountersCollection;
using TOPNSPC::common::PerfCountersCollectionImpl;
using TOPNSPC::common::PerfGuard;
using TOPNSPC::common::RefCountedObject;
using TOPNSPC::common::RefCountedObjectSafe;
using TOPNSPC::common::RefCountedCond;
using TOPNSPC::common::RefCountedWaitObject;
using TOPNSPC::common::ConfigProxy;
