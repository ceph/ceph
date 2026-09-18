#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "rgw_ratelimit_core.h"
#include "rgw_ratelimit_store.h"

namespace rgw::ratelimit {

class SharedCounterRegistry {
public:
  static SharedCounterRegistry& instance();

  int64_t consume_key(const std::string& key,
                      OpType op_type,
                      const RGWRateLimitInfo* info,
                      ceph::timespan curr_timestamp,
                      int64_t interval);

  void giveback_key(const std::string& key, OpType op_type);

  void decrease_bytes_key(const std::string& key,
                          bool is_read,
                          int64_t amount,
                          const RGWRateLimitInfo* info);

  void clear_for_tests();

private:
  struct entry_t {
    RGWRateLimitCounterState state;
    std::mutex lock;
  };

  std::shared_mutex map_lock;
  std::unordered_map<std::string, std::unique_ptr<entry_t>> counters;

  entry_t& get_or_create(const std::string& key);
};

class SharedCounterRateLimitStore : public RateLimitStore, public DoutPrefix {
  std::string key_prefix;
  int64_t interval;

  std::string full_key(const std::string& key) const;

public:
  SharedCounterRateLimitStore() = delete;
  SharedCounterRateLimitStore(CephContext* cct, std::string prefix);

  int64_t should_rate_limit(const char* method,
                            const std::string& key,
                            ceph::coarse_real_time curr_timestamp,
                            const RGWRateLimitInfo* ratelimit_info,
                            const std::string& resource) override;

  void giveback_tokens(const char* method,
                       const std::string& key,
                       const std::string& resource,
                       const RGWRateLimitInfo* ratelimit_info) override;

  void decrease_bytes(const char* method,
                      const std::string& key,
                      int64_t amount,
                      const RGWRateLimitInfo* info) override;
};

using RedisRateLimitStore = SharedCounterRateLimitStore;

} // namespace rgw::ratelimit
