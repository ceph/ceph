// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/perf_counters.h"
#include <seastar/core/sharded.hh>

using crimson::common::PerfCountersCollectionImpl;
namespace crimson::common {
class PerfCountersCollection: public seastar::sharded<PerfCountersCollection>
{
  using ShardedPerfCountersCollection = seastar::sharded<PerfCountersCollection>;

private:
  std::unique_ptr<PerfCountersCollectionImpl> perf_collection;
  static ShardedPerfCountersCollection sharded_perf_coll;
  friend PerfCountersCollection& local_perf_coll();
  friend ShardedPerfCountersCollection& sharded_perf_coll();

public:
  PerfCountersCollection();
  ~PerfCountersCollection();
  PerfCountersCollectionImpl* get_perf_collection();

};

inline PerfCountersCollection::ShardedPerfCountersCollection& sharded_perf_coll(){
  return PerfCountersCollection::sharded_perf_coll;
}

inline PerfCountersCollection& local_perf_coll() {
  return PerfCountersCollection::sharded_perf_coll.local();
}

}

