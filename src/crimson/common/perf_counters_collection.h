// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/perf_counters.h"
#include "include/common_fwd.h"
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
  void dump_formatted(ceph::Formatter *f, bool schema, bool dump_labeled,
                      const std::string &logger = "",
                      const std::string &counter = "");
};

inline PerfCountersCollection::ShardedPerfCountersCollection& sharded_perf_coll(){
  return PerfCountersCollection::sharded_perf_coll;
}

inline PerfCountersCollection& local_perf_coll() {
  return PerfCountersCollection::sharded_perf_coll.local();
}

class PerfCountersDeleter {
  CephContext* cct;

public:
  PerfCountersDeleter() noexcept : cct(nullptr) {}
  PerfCountersDeleter(CephContext* cct) noexcept : cct(cct) {}
  void operator()(PerfCounters* p) noexcept;
};
}
using PerfCountersRef = std::unique_ptr<crimson::common::PerfCounters, crimson::common::PerfCountersDeleter>;

