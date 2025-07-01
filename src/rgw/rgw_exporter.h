#pragma once

#include "rgw_usage_cache.h"
#include "rgw_usage_manager.h"

class RGWExporter {
  std::unique_ptr<RGWUsageCache> cache;
  std::unique_ptr<RGWUsageManager> manager;
public:
  int start(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  void stop();
};
