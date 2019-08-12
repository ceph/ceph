#include "perf_counters_collection.h"

namespace ceph::common {
PerfCountersCollection::PerfCountersCollection()
{
  perf_collection = std::make_unique<PerfCountersCollectionImpl>();
}
PerfCountersCollection::~PerfCountersCollection()
{
  perf_collection->clear();
}

PerfCountersCollectionImpl* PerfCountersCollection:: get_perf_collection()
{
  return perf_collection.get();
}

PerfCountersCollection::ShardedPerfCountersCollection PerfCountersCollection::sharded_perf_coll;

}


