#pragma once

#include <atomic>
#include <thread>
#include <string>

#include "rgw_usage_cache.h"
#include "rgw_perf_counters.h"
class CephContext;

namespace rgw{
class RGWExporter {
  CephContext* cct;
  RGWUsageCache* cache;
  std::thread worker;
  std::atomic<bool> stop_requested{false};

  void run();
  void refresh();

public:
  RGWExporter(CephContext* cct, RGWUsageCache* cache = nullptr);
  ~RGWExporter();

  void start();
  void stop();

  void update_user(const std::string& user, const RGWUsageRecord& record);
  void update_bucket(const std::string& bucket, const RGWUsageRecord& record);
};
}

