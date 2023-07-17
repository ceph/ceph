// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_md5.h"
#include "common/ceph_context.h"

namespace rgw {

namespace md5_counters {

enum {
  l_first = 806000,

  l_batch_size,
  l_batch_timeouts,

  l_last
};

PerfCountersRef build(CephContext* cct, const std::string& name)
{
  PerfCountersBuilder b(cct, name, l_first, l_last);

  b.add_u64_avg(l_batch_size, "batch_size",
                "Number of hash updates per batch");
  b.add_u64_counter(l_batch_timeouts, "batch_timeouts",
                    "Number of timeouts waiting for a full batch");

  auto logger = PerfCountersRef{ b.create_perf_counters(), cct };
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

} // md5_counters

static void on_batch(uint32_t batch_size, bool timeout, void* arg)
{
  auto counters = static_cast<PerfCounters*>(arg);
  counters->inc(md5_counters::l_batch_size, batch_size);
  if (timeout) {
    counters->inc(md5_counters::l_batch_timeouts);
  }
}

AsyncMD5::AsyncMD5(CephContext* cct,
                   const boost::asio::any_io_executor& ex,
                   std::chrono::nanoseconds batch_timeout)
  : counters(md5_counters::build(cct, "md5")),
    batch(ex, batch_timeout, on_batch, counters.get())
{
}

} // namespace rgw
