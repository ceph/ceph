// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "service_impl.hpp"
#include <string>

namespace kvrgw {

class KvRgwRuntime;
class PerfDataStore;

struct PerfConfig {
  std::string tenant_name{"kv-poc"};
  PerfDataStore* perf_data_store{nullptr};
};

int run_perf_driver(KvRgwRuntime& runtime);
int run_perf_driver(KvRgwServiceImpl& service, KvStore& store, const PerfConfig& config);

}  // namespace kvrgw
