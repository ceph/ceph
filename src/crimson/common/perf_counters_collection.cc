// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "perf_counters_collection.h"

namespace crimson::common {
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


