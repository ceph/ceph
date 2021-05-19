// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "crimson/common/perf_counters_collection.h"

namespace crimson::os::seastore {

using PerfCountersRef = std::unique_ptr<PerfCounters>;
enum {
  seastore_perfcounters_first = 30000,
  ss_tm_extents_mutated_total,
  ss_tm_extents_mutated_bytes,
  ss_tm_extents_retired_total,
  ss_tm_extents_retired_bytes,
  ss_tm_extents_allocated_total,
  ss_tm_extents_allocated_bytes,
  seastore_perfcounters_last,
};

class PerfService {
  PerfCountersRef build_seastore_perf() {
    std::string name = fmt::format("seastore::shard-{}", seastar::this_shard_id());
    PerfCountersBuilder ss_plb(nullptr, name, seastore_perfcounters_first,
                               seastore_perfcounters_last);
    ss_plb.add_u64_counter(
      ss_tm_extents_mutated_total, "extents_mutated_num",
      "the number of extents which have been mutated");
    ss_plb.add_u64_counter(
      ss_tm_extents_mutated_bytes, "extents_mutated_bytes",
      "the total bytes of extents which have been mutated");
    ss_plb.add_u64_counter(
      ss_tm_extents_retired_total, "extents_retired_num",
      "the number of extents which have been retired");
    ss_plb.add_u64_counter(
      ss_tm_extents_retired_bytes, "extents_retired_bytes",
      "the total bytes of extents which have been retired");
    ss_plb.add_u64_counter(
      ss_tm_extents_allocated_total, "extents_allocated_num",
      "the number of extents which have been allocated");
    ss_plb.add_u64_counter(
      ss_tm_extents_allocated_bytes, "extents_allocated_bytes",
      "the total bytes of extents which have been allocated");
    return PerfCountersRef(ss_plb.create_perf_counters());
  }

  PerfCountersRef ss_perf = nullptr;
public:
  PerfService()
  : ss_perf(build_seastore_perf()) {}

  ~PerfService() {}

  PerfCounters& get_counters() {
    return *ss_perf;
  }
  void add_to_collection() {
    crimson::common::local_perf_coll().get_perf_collection()->add(ss_perf.get());
  }
  void remove_from_collection() {
    crimson::common::local_perf_coll().get_perf_collection()->remove(ss_perf.get());
  }
};
using PerfServiceRef = std::unique_ptr<PerfService>;
}
