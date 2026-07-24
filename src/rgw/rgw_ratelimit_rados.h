#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cls/rgw_ratelimit/cls_rgw_ratelimit_client.h"
#include "include/rados/librados.hpp"
#include "rgw_ratelimit_shared.h"
#include "rgw_ratelimit_store.h"

namespace rgw::ratelimit {

class RadosRateLimitStore : public RateLimitStore, public DoutPrefix {
public:
  RadosRateLimitStore() = delete;
  RadosRateLimitStore(CephContext* cct,
                      std::string pool,
                      std::string oid_prefix,
                      uint32_t num_shards);

  int init();
  void shutdown();

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

  void set_test_ioctx(librados::IoCtx* ioctx);

private:
  std::string pool_name;
  std::string oid_prefix;
  uint32_t num_shards;
  int64_t interval;
  bool fail_open;

  librados::Rados* rados = nullptr;
  std::unique_ptr<librados::Rados> rados_client;
  std::unique_ptr<librados::IoCtx> ioctx;
  librados::IoCtx* test_ioctx = nullptr;

  std::unique_ptr<SharedCounterRateLimitStore> fallback;

  std::string shard_oid(const std::string& key) const;

  librados::IoCtx* get_ioctx();
  bool use_fallback();
};

} // namespace rgw::ratelimit
