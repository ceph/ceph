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

#include "admin_server.hpp"
#include "data_store.hpp"
#include "gc_config_state.hpp"
#include "gc_worker.hpp"
#include "kv_store.hpp"
#include "ref_tag.hpp"
#include "service_impl.hpp"
#include "sweeper.hpp"
#include "tier_config_state.hpp"

#include <atomic>
#include <optional>
#include <string>
#include <thread>

namespace kvrgw {

struct KvRgwStartOptions {
  bool perf_mode = false;
  std::string data_root;
};

class KvRgwRuntime {
 public:
  KvRgwRuntime() = default;
  ~KvRgwRuntime();

  KvRgwRuntime(const KvRgwRuntime&) = delete;
  KvRgwRuntime& operator=(const KvRgwRuntime&) = delete;
  KvRgwRuntime(KvRgwRuntime&&) = delete;
  KvRgwRuntime& operator=(KvRgwRuntime&&) = delete;

  bool start(const KvRgwStartOptions& opts);
  void stop();
  void request_stop();
  bool stop_requested() const;

  KvRgwServiceImpl& service();
  KvStore& store();
  PerfDataStore* perf_data_store();

 private:
  void join_workers();
  void stop_fdb_network();

  std::atomic<bool> stop_{false};
  bool network_started_{false};
  bool started_{false};
  bool perf_mode_{false};

  std::thread network_thread_;
  std::optional<KvStore> store_;
  std::optional<RefTagGenerator> ref_tags_;
  std::optional<PerfDataStore> perf_data_store_;
  std::optional<FileDataStore> file_data_store_;
  std::optional<TierConfigState> tier_config_state_;
  std::optional<Sweeper> sweeper_;
  std::optional<KvRgwServiceImpl> service_;
  std::optional<GcConfigState> gc_config_;
  std::optional<GcWorker> gc_worker_;
  std::optional<AdminServer> admin_server_;
  std::thread admin_thread_;
  std::thread sweeper_thread_;
  std::thread gc_thread_;
};

}  // namespace kvrgw
