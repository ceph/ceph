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

#pragma once

#include "common/perf_counters_collection.h"
#include "async_md5.h"

namespace rgw {

struct AsyncMD5 {
  PerfCountersRef counters;
  ceph::async_md5::Batch batch;

  AsyncMD5(CephContext* cct,
           const boost::asio::any_io_executor& ex,
           std::chrono::nanoseconds batch_timeout);
};

} // namespace rgw
