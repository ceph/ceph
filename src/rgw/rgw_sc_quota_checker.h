// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#ifndef CEPH_RGW_SC_QUOTA_CHECKER_H
#define CEPH_RGW_SC_QUOTA_CHECKER_H

#include "rgw_quota_types.h"
#include "rgw_common.h"
#include "common/ceph_time.h"
#include <mutex>
#include <memory>
#include <string>

class CephContext;
class DoutPrefixProvider;
struct rgw_bucket;

namespace rgw {
namespace sal {
  class Driver;
  class Bucket;
}
}

namespace rgw { namespace quota {

/**
 * Storage Class Quota Checker
 * 
 * Enforces per-storage-class quota limits by reading usage data
 * from PR #66501 (storage_class_stats in bucket index).
 */
class StorageClassQuotaChecker {
private:
  CephContext* cct_;
  rgw::sal::Driver* driver_;

  // Cache entry for quota info
  struct QuotaCacheEntry {
    RGWQuotaInfo quota;
    ceph::real_time cached_at;

    bool is_stale(uint32_t expiry_sec) const {
      auto age = ceph::real_clock::now() - cached_at;
      return std::chrono::duration_cast<std::chrono::seconds>(age).count() > expiry_sec;
    }
  };

  mutable std::mutex cache_mutex_;
  std::unordered_map<std::string, QuotaCacheEntry> quota_cache_;

  // Helpers
  std::string make_cache_key(const rgw_bucket& bucket) const;
  
  int get_quota_info(
      const DoutPrefixProvider* dpp,
      const rgw_bucket& bucket,
      RGWQuotaInfo* quota_info);

  int get_current_usage(
      const DoutPrefixProvider* dpp,
      const rgw_bucket& bucket,
      const std::string& sc_key,
      uint64_t* out_size,
      uint64_t* out_objects);

  int check_bucket_quota(
      const DoutPrefixProvider* dpp,
      const rgw_bucket& bucket,
      const RGWQuotaInfo& quota_info,
      const std::string& sc_key,
      uint64_t obj_size,
      uint64_t num_objs);

public:
  StorageClassQuotaChecker(CephContext* cct, rgw::sal::Driver* driver);
  ~StorageClassQuotaChecker() = default;

  /**
   * Check quota before object write
   * 
   * @param dpp Debug prefix provider
   * @param bucket Bucket being written to
   * @param storage_class Storage class (e.g., "STANDARD", "SSD")
   * @param placement Placement target (e.g., "default-placement")
   * @param obj_size Size of object being written
   * @param num_objs Number of objects (usually 1)
   * @return 0 on success, negative error code on quota exceeded
   */
  int check_quota(
      const DoutPrefixProvider* dpp,
      const rgw_bucket& bucket,
      const std::string& storage_class,
      const std::string& placement,
      uint64_t obj_size,
      uint64_t num_objs = 1);

  // Invalidate cached quota info
  void invalidate_cache(const rgw_bucket& bucket);
};

}} // namespace rgw::quota

#endif // CEPH_RGW_SC_QUOTA_CHECKER_H