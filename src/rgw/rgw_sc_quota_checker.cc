// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_sc_quota_checker.h"
#include "rgw_sal.h"
#include "rgw_bucket.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "common/errno.h"
#include "common/dout.h"

#include <fmt/format.h>

#define dout_subsys ceph_subsys_rgw

using namespace rgw::quota;

StorageClassQuotaChecker::StorageClassQuotaChecker(
    CephContext* cct,
    rgw::sal::Driver* driver)
  : cct_(cct), driver_(driver)
{
}

std::string StorageClassQuotaChecker::make_cache_key(const rgw_bucket& bucket) const
{
  return bucket.get_key();
}

int StorageClassQuotaChecker::get_quota_info(
    const DoutPrefixProvider* dpp,
    const rgw_bucket& bucket,
    RGWQuotaInfo* quota_info)
{
  std::string cache_key = make_cache_key(bucket);
  uint32_t expiry = cct_->_conf->rgw_storage_class_quota_cache_expiry_sec;

  // Try cache first
  {
    std::lock_guard lock(cache_mutex_);
    auto it = quota_cache_.find(cache_key);
    if (it != quota_cache_.end() && !it->second.is_stale(expiry)) {
      *quota_info = it->second.quota;
      ldpp_dout(dpp, 20) << __func__ << " cache hit for bucket=" << bucket << dendl;
      return 0;
    }
  }

  // Cache miss - read from bucket metadata
  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  int ret = driver_->get_bucket(dpp, nullptr, bucket, &sal_bucket, null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to get bucket: " 
                      << bucket << " ret=" << ret << dendl;
    return ret;
  }

  // Get bucket info (contains quota_info)
  RGWBucketInfo& info = sal_bucket->get_info();
  *quota_info = info.quota;

  // Update cache
  {
    std::lock_guard lock(cache_mutex_);
    QuotaCacheEntry entry;
    entry.quota = *quota_info;
    entry.cached_at = ceph::real_clock::now();
    quota_cache_[cache_key] = entry;
  }

  ldpp_dout(dpp, 20) << __func__ << " loaded quota for bucket=" << bucket << dendl;
  return 0;
}

int StorageClassQuotaChecker::get_current_usage(
    const DoutPrefixProvider* dpp,
    const rgw_bucket& bucket,
    const std::string& sc_key,
    uint64_t* out_size,
    uint64_t* out_objects)
{
  // Get bucket
  std::unique_ptr<rgw::sal::Bucket> sal_bucket;
  int ret = driver_->get_bucket(dpp, nullptr, bucket, &sal_bucket, null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to get bucket: " 
                      << bucket << " ret=" << ret << dendl;
    return ret;
  }

  // Read bucket index header (contains PR #66501 storage_class_stats)
  rgw_bucket_dir_header header;
  ret = sal_bucket->read_bucket_stats(dpp, null_yield, &header);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read bucket stats: "
                      << bucket << " ret=" << ret << dendl;
    return ret;
  }

  // Check if bucket has SC stats (PR #66501)
  if (!header.storage_class_stats) {
    ldpp_dout(dpp, 5) << "NOTICE: " << __func__ << " bucket not converted for SC stats: "
                      << bucket << dendl;
    return -ENOTSUP;
  }

  // Find SC-specific stats
  auto it = header.storage_class_stats->find(sc_key);
  if (it == header.storage_class_stats->end()) {
    // No objects in this storage class yet - return 0
    *out_size = 0;
    *out_objects = 0;
    ldpp_dout(dpp, 20) << __func__ << " no usage for sc_key=" << sc_key << dendl;
    return 0;
  }

  *out_size = it->second.total_size;
  *out_objects = it->second.num_entries;

  ldpp_dout(dpp, 20) << __func__ << " sc_key=" << sc_key
                     << " size=" << *out_size
                     << " objects=" << *out_objects << dendl;

  return 0;
}

int StorageClassQuotaChecker::check_bucket_quota(
    const DoutPrefixProvider* dpp,
    const rgw_bucket& bucket,
    const RGWQuotaInfo& quota_info,
    const std::string& sc_key,
    uint64_t obj_size,
    uint64_t num_objs)
{
  // Get SC quota for this storage class
  const RGWStorageClassQuota* sc_quota = quota_info.get_sc_quota(sc_key);
  if (!sc_quota || !sc_quota->enabled) {
    ldpp_dout(dpp, 20) << __func__ << " no quota configured for sc_key=" << sc_key << dendl;
    return 0;  // No quota configured
  }

  // Get current usage from PR #66501 data
  uint64_t current_size = 0;
  uint64_t current_objects = 0;
  int ret = get_current_usage(dpp, bucket, sc_key, &current_size, &current_objects);
  if (ret < 0) {
    if (ret == -ENOTSUP) {
      ldpp_dout(dpp, 1) << "ERROR: " << __func__ 
                        << " bucket not converted for SC quota: " << bucket << dendl;
      // Return specific error for unconverted bucket
      return -ENOTSUP;
    }

    // Fail-safe: allow operation if stats unavailable (configurable)
    if (cct_->_conf->rgw_storage_class_quota_fail_safe) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ 
                        << " stats unavailable, allowing (fail-safe)" << dendl;
      return 0;
    }
    return ret;
  }

  // Check size quota
  if (sc_quota->max_size >= 0) {
    uint64_t new_total = current_size + obj_size;
    if (new_total > (uint64_t)sc_quota->max_size) {
      ldpp_dout(dpp, 1) << "QUOTA EXCEEDED: " << __func__
                        << " sc_key=" << sc_key
                        << " current_size=" << current_size
                        << " limit=" << sc_quota->max_size
                        << " requested=" << obj_size
                        << " would_be=" << new_total << dendl;
      return -ERR_QUOTA_EXCEEDED;
    }
  }

  // Check object count quota
  if (sc_quota->max_objects >= 0) {
    uint64_t new_total = current_objects + num_objs;
    if (new_total > (uint64_t)sc_quota->max_objects) {
      ldpp_dout(dpp, 1) << "QUOTA EXCEEDED: " << __func__
                        << " sc_key=" << sc_key
                        << " current_objects=" << current_objects
                        << " limit=" << sc_quota->max_objects
                        << " requested=" << num_objs
                        << " would_be=" << new_total << dendl;
      return -ERR_QUOTA_EXCEEDED;
    }
  }

  ldpp_dout(dpp, 20) << __func__ << " quota check passed for sc_key=" << sc_key << dendl;
  return 0;
}

int StorageClassQuotaChecker::check_quota(
    const DoutPrefixProvider* dpp,
    const rgw_bucket& bucket,
    const std::string& storage_class,
    const std::string& placement,
    uint64_t obj_size,
    uint64_t num_objs)
{
  // Check if feature enabled
  if (!cct_->_conf->rgw_storage_class_quota_enabled) {
    ldpp_dout(dpp, 20) << __func__ << " SC quota disabled" << dendl;
    return 0;
  }

  // Get quota info
  RGWQuotaInfo quota_info;
  int ret = get_quota_info(dpp, bucket, &quota_info);
  if (ret < 0) {
    if (cct_->_conf->rgw_storage_class_quota_fail_safe) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ 
                        << " failed to get quota info, allowing (fail-safe)" << dendl;
      return 0;  // Fail-open
    }
    return ret;
  }

  // Build storage class key (same format as PR #66501)
  std::string sc_key = fmt::format("{}::{}", placement, storage_class);

  ldpp_dout(dpp, 20) << __func__ 
                     << " bucket=" << bucket
                     << " sc_key=" << sc_key
                     << " obj_size=" << obj_size
                     << " num_objs=" << num_objs
                     << " enforcement_mode=" << (int)quota_info.enforcement_mode << dendl;

  // Determine what to check based on enforcement mode
  bool check_sc_quota = false;
  bool check_global_quota = false;

  switch (quota_info.enforcement_mode) {
    case RGWQuotaEnforcementMode::LEGACY:
    case RGWQuotaEnforcementMode::GLOBAL_ONLY:
      check_global_quota = true;
      break;
    case RGWQuotaEnforcementMode::STORAGE_CLASS:
      check_sc_quota = true;
      break;
    case RGWQuotaEnforcementMode::HYBRID:
      check_sc_quota = true;
      check_global_quota = true;
      break;
  }

  // Check SC quota if enabled
  if (check_sc_quota) {
    ret = check_bucket_quota(dpp, bucket, quota_info, sc_key, obj_size, num_objs);
    if (ret < 0) {
      return ret;  // SC quota exceeded
    }
  }

  // Check global quota if enabled
  // NOTE: This would call existing RGW global quota check function
  // For now, we skip this to avoid dependency issues call: rgw_check_quota(...)
  if (check_global_quota && quota_info.enabled) {
    ldpp_dout(dpp, 20) << __func__ << " would also check global quota here" << dendl;
    // HSTTODO: Call existing global quota check
  }

  return 0;  // All quota checks passed
}

void StorageClassQuotaChecker::invalidate_cache(const rgw_bucket& bucket)
{
  std::lock_guard lock(cache_mutex_);
  std::string cache_key = make_cache_key(bucket);
  quota_cache_.erase(cache_key);
  ldpp_dout(nullptr, 20) << __func__ << " invalidated cache for bucket=" << bucket << dendl;
}