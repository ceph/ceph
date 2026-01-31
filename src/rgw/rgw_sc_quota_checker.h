// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#pragma once

#include "rgw_sc_quota_types.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_observability.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/user/cls_user_types.h"

class RGWRados;
class RGWSI_Bucket;
class RGWSI_User;

/**
 * Storage Class Quota Checker
 * 
 * Responsible for enforcing per-storage-class quotas at bucket and user levels.
 * Integrates with Enhanced Observability cache for fast quota checks.
 * 
 * Performance Characteristics:
 * - Cache hit: 
 * - Cache miss: 
 * - Typical latency added to write operations: 
 *  (HSTTODO : update the values)
 * 
 * Thread Safety:
 * - Read-only operations (quota checks) are thread-safe
 * - Quota configuration updates require external synchronization
 */

class RGWStorageClassQuotaChecker {
public:
  RGWStorageClassQuotaChecker(
      CephContext* cct,
      RGWRados* store,
      RGWEnhancedObservabilityCache* obs_cache)
    : cct(cct),
      store(store),
      obs_cache(obs_cache)
  {
  }

  /**
   * Check bucket-level storage class quota
   * 
   * @param bucket The bucket to check
   * @param placement_target Placement target (e.g., "default-placement")
   * @param storage_class Storage class (e.g., "SSD", "STANDARD")
   * @param obj_size Size of object being written
   * @param dpp Debug prefix provider
   * @param y Yield context
   * @return RGWQuotaCheckResult indicating success or failure
   */
  RGWQuotaCheckResult check_bucket_quota(
      const rgw_bucket& bucket,
      const std::string& placement_target,
      const std::string& storage_class,
      uint64_t obj_size,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Check user-level storage class quota
   * 
   * @param user_id User ID
   * @param placement_target Placement target
   * @param storage_class Storage class
   * @param obj_size Size of object being written
   * @param dpp Debug prefix provider
   * @param y Yield context
   * @return RGWQuotaCheckResult indicating success or failure
   */
  RGWQuotaCheckResult check_user_quota(
      const std::string& user_id,
      const std::string& placement_target,
      const std::string& storage_class,
      uint64_t obj_size,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Check both bucket and user quotas
   * 
   * This is the main entry point for quota enforcement.
   * Checks bucket quota first, then user quota.
   * 
   * @param bucket The bucket
   * @param user_id User ID
   * @param placement_target Placement target
   * @param storage_class Storage class
   * @param obj_size Object size
   * @param dpp Debug prefix provider
   * @param y Yield context
   * @return RGWQuotaCheckResult indicating success or failure
   */
  RGWQuotaCheckResult check_quota(
      const rgw_bucket& bucket,
      const std::string& user_id,
      const std::string& placement_target,
      const std::string& storage_class,
      uint64_t obj_size,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Get current quota usage statistics
   * 
   * @param bucket Bucket to query (empty for user-level stats)
   * @param user_id User ID
   * @param stats Output statistics
   * @param dpp Debug prefix provider
   * @param y Yield context
   * @return 0 on success, negative error code on failure
   */
  int get_quota_stats(
      const rgw_bucket& bucket,
      const std::string& user_id,
      RGWQuotaStats* stats,
      const DoutPrefixProvider* dpp,
      optional_yield y);

private:
  CephContext* cct;
  RGWRados* store;
  RGWEnhancedObservabilityCache* obs_cache;

  /**
   * Build canonical storage class key
   * Format: "{placement-target}::{storage-class}"
   */
  std::string make_sc_key(const std::string& placement, const std::string& sc) const {
    return placement + "::" + sc;
  }

  /**
   * Get bucket quota configuration
   */
  int get_bucket_quota_info(
      const rgw_bucket& bucket,
      RGWQuotaInfoExtended* quota,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Get user quota configuration
   */
  int get_user_quota_info(
      const std::string& user_id,
      RGWQuotaInfoExtended* quota,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Get bucket storage class stats from cache or RADOS
   */
  int get_bucket_sc_stats(
      const rgw_bucket& bucket,
      const std::string& sc_key,
      rgw_bucket_category_stats* stats,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Get user storage class stats from cache or RADOS
   */
  int get_user_sc_stats(
      const std::string& user_id,
      const std::string& sc_key,
      rgw_bucket_category_stats* stats,
      const DoutPrefixProvider* dpp,
      optional_yield y);

  /**
   * Check if bucket is converted (has storage class stats)
   */
  bool is_bucket_converted(
      const rgw_bucket& bucket,
      const DoutPrefixProvider* dpp);

  /**
   * Perform the actual quota check logic
   */
  RGWQuotaCheckResult do_quota_check(
      const RGWQuotaInfoExtended& quota,
      const rgw_bucket_category_stats& current_stats,
      const std::string& sc_key,
      uint64_t obj_size,
      const std::string& context,
      const DoutPrefixProvider* dpp);
};

/**
 * Implementation HSTTODO
 */

inline std::string RGWStorageClassQuotaChecker::make_sc_key(
    const std::string& placement, 
    const std::string& sc) const 
{
  return placement + "::" + sc;
}

inline RGWQuotaCheckResult RGWStorageClassQuotaChecker::check_quota(
    const rgw_bucket& bucket,
    const std::string& user_id,
    const std::string& placement_target,
    const std::string& storage_class,
    uint64_t obj_size,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  ldpp_dout(dpp, 20) << __func__ << ": Checking quota for bucket=" << bucket
                     << " user=" << user_id
                     << " placement=" << placement_target
                     << " sc=" << storage_class
                     << " size=" << obj_size << dendl;

  // Check bucket quota first
  auto result = check_bucket_quota(bucket, placement_target, storage_class, 
                                    obj_size, dpp, y);
  if (!result.is_ok()) {
    ldpp_dout(dpp, 5) << __func__ << ": Bucket quota check failed: " 
                      << result.to_string() << dendl;
    return result;
  }

  // Check user quota second
  result = check_user_quota(user_id, placement_target, storage_class, 
                             obj_size, dpp, y);
  if (!result.is_ok()) {
    ldpp_dout(dpp, 5) << __func__ << ": User quota check failed: " 
                      << result.to_string() << dendl;
    return result;
  }

  ldpp_dout(dpp, 20) << __func__ << ": All quota checks passed" << dendl;
  return result;
}

inline RGWQuotaCheckResult RGWStorageClassQuotaChecker::check_bucket_quota(
    const rgw_bucket& bucket,
    const std::string& placement_target,
    const std::string& storage_class,
    uint64_t obj_size,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  RGWQuotaCheckResult result;
  std::string sc_key = make_sc_key(placement_target, storage_class);

  // Get bucket quota configuration
  RGWQuotaInfoExtended quota;
  int ret = get_bucket_quota_info(bucket, &quota, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << __func__ << ": Failed to get bucket quota info: " 
                      << cpp_strerror(ret) << dendl;
    result.status = RGWQuotaCheckResult::ERROR;
    return result;
  }

  // Check global quota first (if enabled)
  if (quota.enforce_global_quotas()) {
    rgw_bucket_category_stats global_stats;
    ret = get_bucket_sc_stats(bucket, "", &global_stats, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << ": Failed to get bucket stats: " 
                        << cpp_strerror(ret) << dendl;
      result.status = RGWQuotaCheckResult::ERROR;
      return result;
    }

    // Check global size quota
    if (quota.max_size >= 0) {
      uint64_t projected_size = global_stats.total_size + obj_size;
      if (projected_size > static_cast<uint64_t>(quota.max_size)) {
        ldpp_dout(dpp, 5) << __func__ << ": Bucket " << bucket 
                          << " global size quota exceeded: "
                          << projected_size << " > " << quota.max_size << dendl;
        result.status = RGWQuotaCheckResult::GLOBAL_SIZE_EXCEEDED;
        result.current_size = global_stats.total_size;
        result.limit_size = quota.max_size;
        return result;
      }
    }

    // Check global object count quota
    if (quota.max_objects >= 0) {
      uint64_t projected_objects = global_stats.num_entries + 1;
      if (projected_objects > static_cast<uint64_t>(quota.max_objects)) {
        ldpp_dout(dpp, 5) << __func__ << ": Bucket " << bucket 
                          << " global object quota exceeded: "
                          << projected_objects << " > " << quota.max_objects << dendl;
        result.status = RGWQuotaCheckResult::GLOBAL_OBJECTS_EXCEEDED;
        result.current_objects = global_stats.num_entries;
        result.limit_objects = quota.max_objects;
        return result;
      }
    }
  }

  // Check per-storage-class quota (if enabled)
  if (quota.enforce_storage_class_quotas()) {
    // Check if bucket is converted
    if (!is_bucket_converted(bucket, dpp)) {
      ldpp_dout(dpp, 5) << __func__ << ": Bucket " << bucket 
                        << " not converted for storage class quotas" << dendl;
      result.status = RGWQuotaCheckResult::BUCKET_NOT_CONVERTED;
      result.storage_class_key = sc_key;
      return result;
    }

    // Get quota for this storage class
    const auto* sc_quota = quota.get_sc_quota(sc_key);
    if (sc_quota && sc_quota->enabled) {
      // Get current stats for this storage class
      rgw_bucket_category_stats sc_stats;
      ret = get_bucket_sc_stats(bucket, sc_key, &sc_stats, dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << ": Failed to get bucket SC stats: " 
                          << cpp_strerror(ret) << dendl;
        result.status = RGWQuotaCheckResult::ERROR;
        return result;
      }

      // Check size quota
      if (sc_quota->is_size_limited()) {
        uint64_t projected_size = sc_stats.total_size + obj_size;
        if (projected_size > static_cast<uint64_t>(sc_quota->max_size)) {
          ldpp_dout(dpp, 5) << __func__ << ": Bucket " << bucket 
                            << " storage class " << sc_key
                            << " size quota exceeded: "
                            << projected_size << " > " << sc_quota->max_size << dendl;
          result.status = RGWQuotaCheckResult::SC_SIZE_EXCEEDED;
          result.storage_class_key = sc_key;
          result.current_size = sc_stats.total_size;
          result.limit_size = sc_quota->max_size;
          return result;
        }
      }

      // Check object count quota
      if (sc_quota->is_objects_limited()) {
        uint64_t projected_objects = sc_stats.num_entries + 1;
        if (projected_objects > static_cast<uint64_t>(sc_quota->max_objects)) {
          ldpp_dout(dpp, 5) << __func__ << ": Bucket " << bucket 
                            << " storage class " << sc_key
                            << " object quota exceeded: "
                            << projected_objects << " > " << sc_quota->max_objects << dendl;
          result.status = RGWQuotaCheckResult::SC_OBJECTS_EXCEEDED;
          result.storage_class_key = sc_key;
          result.current_objects = sc_stats.num_entries;
          result.limit_objects = sc_quota->max_objects;
          return result;
        }
      }
    }
  }

  // All checks passed
  result.status = RGWQuotaCheckResult::OK;
  return result;
}

inline RGWQuotaCheckResult RGWStorageClassQuotaChecker::check_user_quota(
    const std::string& user_id,
    const std::string& placement_target,
    const std::string& storage_class,
    uint64_t obj_size,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  RGWQuotaCheckResult result;
  std::string sc_key = make_sc_key(placement_target, storage_class);

  // Get user quota configuration
  RGWQuotaInfoExtended quota;
  int ret = get_user_quota_info(user_id, &quota, dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << __func__ << ": Failed to get user quota info: " 
                      << cpp_strerror(ret) << dendl;
    result.status = RGWQuotaCheckResult::ERROR;
    return result;
  }

  // Check global quota first (if enabled)
  if (quota.enforce_global_quotas()) {
    rgw_bucket_category_stats global_stats;
    ret = get_user_sc_stats(user_id, "", &global_stats, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << __func__ << ": Failed to get user stats: " 
                        << cpp_strerror(ret) << dendl;
      result.status = RGWQuotaCheckResult::ERROR;
      return result;
    }

    // Check global size quota
    if (quota.max_size >= 0) {
      uint64_t projected_size = global_stats.total_size + obj_size;
      if (projected_size > static_cast<uint64_t>(quota.max_size)) {
        ldpp_dout(dpp, 5) << __func__ << ": User " << user_id 
                          << " global size quota exceeded: "
                          << projected_size << " > " << quota.max_size << dendl;
        result.status = RGWQuotaCheckResult::GLOBAL_SIZE_EXCEEDED;
        result.current_size = global_stats.total_size;
        result.limit_size = quota.max_size;
        return result;
      }
    }

    // Check global object count quota
    if (quota.max_objects >= 0) {
      uint64_t projected_objects = global_stats.num_entries + 1;
      if (projected_objects > static_cast<uint64_t>(quota.max_objects)) {
        ldpp_dout(dpp, 5) << __func__ << ": User " << user_id 
                          << " global object quota exceeded: "
                          << projected_objects << " > " << quota.max_objects << dendl;
        result.status = RGWQuotaCheckResult::GLOBAL_OBJECTS_EXCEEDED;
        result.current_objects = global_stats.num_entries;
        result.limit_objects = quota.max_objects;
        return result;
      }
    }
  }

  // Check per-storage-class quota (if enabled)
  if (quota.enforce_storage_class_quotas()) {
    const auto* sc_quota = quota.get_sc_quota(sc_key);
    if (sc_quota && sc_quota->enabled) {
      // Get current stats for this storage class
      rgw_bucket_category_stats sc_stats;
      ret = get_user_sc_stats(user_id, sc_key, &sc_stats, dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << __func__ << ": Failed to get user SC stats: " 
                          << cpp_strerror(ret) << dendl;
        result.status = RGWQuotaCheckResult::ERROR;
        return result;
      }

      // Check size quota
      if (sc_quota->is_size_limited()) {
        uint64_t projected_size = sc_stats.total_size + obj_size;
        if (projected_size > static_cast<uint64_t>(sc_quota->max_size)) {
          ldpp_dout(dpp, 5) << __func__ << ": User " << user_id 
                            << " storage class " << sc_key
                            << " size quota exceeded: "
                            << projected_size << " > " << sc_quota->max_size << dendl;
          result.status = RGWQuotaCheckResult::SC_SIZE_EXCEEDED;
          result.storage_class_key = sc_key;
          result.current_size = sc_stats.total_size;
          result.limit_size = sc_quota->max_size;
          return result;
        }
      }

      // Check object count quota
      if (sc_quota->is_objects_limited()) {
        uint64_t projected_objects = sc_stats.num_entries + 1;
        if (projected_objects > static_cast<uint64_t>(sc_quota->max_objects)) {
          ldpp_dout(dpp, 5) << __func__ << ": User " << user_id 
                            << " storage class " << sc_key
                            << " object quota exceeded: "
                            << projected_objects << " > " << sc_quota->max_objects << dendl;
          result.status = RGWQuotaCheckResult::SC_OBJECTS_EXCEEDED;
          result.storage_class_key = sc_key;
          result.current_objects = sc_stats.num_entries;
          result.limit_objects = sc_quota->max_objects;
          return result;
        }
      }
    }
  }

  // All checks passed
  result.status = RGWQuotaCheckResult::OK;
  return result;
}

inline int RGWStorageClassQuotaChecker::get_bucket_quota_info(
    const rgw_bucket& bucket,
    RGWQuotaInfoExtended* quota,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  // HSTTODO Implementation: Read bucket metadata and extract quota
  // This would integrate with existing RGW bucket metadata APIs
  return 0;  // Stub
}

inline int RGWStorageClassQuotaChecker::get_user_quota_info(
    const std::string& user_id,
    RGWQuotaInfoExtended* quota,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  // HSTTODO Implementation: Read user metadata and extract quota
  // This would integrate with existing RGW user metadata APIs
  return 0;  // Stub
}

inline int RGWStorageClassQuotaChecker::get_bucket_sc_stats(
    const rgw_bucket& bucket,
    const std::string& sc_key,
    rgw_bucket_category_stats* stats,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  // Try cache first
  if (obs_cache) {
    int ret = obs_cache->get_bucket_sc_stats(bucket, sc_key, stats, dpp, y);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << __func__ << ": Cache hit for bucket " << bucket 
                         << " sc_key " << sc_key << dendl;
      return 0;
    }
  }

  // Cache miss - read from RADOS
  ldpp_dout(dpp, 20) << __func__ << ": Cache miss for bucket " << bucket 
                     << " sc_key " << sc_key << dendl;
  // HSTTODO Implementation: Direct RADOS read
  return 0;  // Stub
}

inline int RGWStorageClassQuotaChecker::get_user_sc_stats(
    const std::string& user_id,
    const std::string& sc_key,
    rgw_bucket_category_stats* stats,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  // Try cache first
  if (obs_cache) {
    int ret = obs_cache->get_user_sc_stats(user_id, sc_key, stats, dpp, y);
    if (ret == 0) {
      ldpp_dout(dpp, 20) << __func__ << ": Cache hit for user " << user_id 
                         << " sc_key " << sc_key << dendl;
      return 0;
    }
  }

  // Cache miss - aggregate from all user buckets
  ldpp_dout(dpp, 20) << __func__ << ": Cache miss for user " << user_id 
                     << " sc_key " << sc_key << dendl;
  // HSTTODO Implementation: Aggregate from RADOS
  return 0;  // Stub
}

inline bool RGWStorageClassQuotaChecker::is_bucket_converted(
    const rgw_bucket& bucket,
    const DoutPrefixProvider* dpp)
{
  if (obs_cache) {
    return obs_cache->is_bucket_converted(bucket);
  }
  // Fallback: Assume not converted
  // HSTTODO handling 
  return false;
}

#endif  // CEPH_RGW_SC_QUOTA_CHECKER_H