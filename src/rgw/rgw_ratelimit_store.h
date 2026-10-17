#pragma once

#include <memory>
#include <string>

#include "rgw_common.h"
#include "rgw_ratelimit_core.h"

class RateLimitStore {
public:
  virtual ~RateLimitStore() = default;

  virtual int64_t should_rate_limit(const char* method,
                                    const std::string& key,
                                    ceph::coarse_real_time curr_timestamp,
                                    const RGWRateLimitInfo* ratelimit_info,
                                    const std::string& resource) = 0;

  virtual void giveback_tokens(const char* method,
                               const std::string& key,
                               const std::string& resource,
                               const RGWRateLimitInfo* ratelimit_info) = 0;

  virtual void decrease_bytes(const char* method,
                              const std::string& key,
                              int64_t amount,
                              const RGWRateLimitInfo* info) = 0;
};

class RateLimitService {
public:
  RateLimitService() = delete;
  explicit RateLimitService(CephContext* cct);
  ~RateLimitService();

  RateLimitService(const RateLimitService&) = delete;
  RateLimitService& operator=(const RateLimitService&) = delete;

  std::shared_ptr<RateLimitStore> get_active();
  void start();

private:
  struct Impl;
  std::unique_ptr<Impl> impl;
};
