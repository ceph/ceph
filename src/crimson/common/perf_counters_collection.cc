// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
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

void PerfCountersCollection::dump_formatted(ceph::Formatter *f, bool schema,
                                            bool dump_labeled,
                                            const std::string &logger,
                                            const std::string &counter)
{
  perf_collection->dump_formatted(f, schema, dump_labeled, logger, counter);
}

PerfCountersCollection::ShardedPerfCountersCollection PerfCountersCollection::sharded_perf_coll;

void PerfCountersDeleter::operator()(PerfCounters* p) noexcept
{
  if (cct) {
    cct->get_perfcounters_collection()->remove(p);
  }
  delete p;
}

}

