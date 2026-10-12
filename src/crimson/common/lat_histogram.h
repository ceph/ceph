// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <array>
#include <chrono>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_types.hh>

namespace crimson::common {

// Millisecond buckets for messenger and OSD op stage latencies. Samples
// come from lowres_clock (granularity ~task_quota), so the first bucket
// holds everything it can't resolve.
inline constexpr std::array<double, 15> LAT_BUCKETS_MS = {
  0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 50, 100, 250, 500, 1000
};

inline void init_lat_histogram(seastar::metrics::histogram& hist) {
  hist.buckets.resize(LAT_BUCKETS_MS.size());
  for (std::size_t i = 0; i < LAT_BUCKETS_MS.size(); ++i) {
    hist.buckets[i].upper_bound = LAT_BUCKETS_MS[i];
  }
}

// Buckets are cumulative (Prometheus "le"), so histogram_quantile() works.
inline void add_lat_sample(
  seastar::metrics::histogram& hist,
  seastar::lowres_clock::duration dur)
{
  const double ms = std::chrono::duration<double, std::milli>(dur).count();
  for (auto& b : hist.buckets) {
    if (ms <= b.upper_bound) {
      ++b.count;
    }
  }
  ++hist.sample_count;
  hist.sample_sum += ms;
}

} // namespace crimson::common
