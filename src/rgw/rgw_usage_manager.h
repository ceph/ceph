#pragma once

#include "rgw_usage_cache.h"
#include "rgw_perf_counters.h"
#include "rgw_sal.h"
#include "include/Context.h"
#include <thread>
#include <atomic>

class RGWUsageManager {
  const DoutPrefixProvider* dpp;
  rgw::sal::Driver* driver;
  RGWUsageCache* cache;
  std::thread thr;
  std::atomic<bool> stop_flag{false};
  unsigned interval;

  void run();
  void refresh();

public:
  RGWUsageManager(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver,
                  RGWUsageCache* cache);
  ~RGWUsageManager();
  void start();
  void stop();
};
