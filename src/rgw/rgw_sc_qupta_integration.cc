// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Storage Class Quota Integration into RGW Operations
 * 
 * This file shows how storage class quota checks are integrated
 * into PUT, COPY, and multipart upload operations.
 */

#include "rgw_sc_quota_checker.h"
#include "rgw_op.h"
#include "rgw_rest.h"

/**
 * Integration Point 1: RGWPutObj::execute()
 * 
 * Called for: PUT /bucket/object
 * 
 * Quota check happens BEFORE any data is written to RADOS.
 */
class RGWPutObj : public RGWOp {
  // HSTTODO : handling to be done with existing members of this class

  int execute(optional_yield y) override {
    const DoutPrefixProvider* dpp = this;
    
    // Extract storage class from request headers
    std::string storage_class = RGW_STORAGE_CLASS_STANDARD;  // Default
    auto sc_header = s->info.env->get("HTTP_X_AMZ_STORAGE_CLASS");
    if (sc_header) {
      storage_class = *sc_header;
    }

    // Determine placement target
    std::string placement_target;
    int ret = get_placement_target(s, &placement_target);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: Failed to determine placement target" << dendl;
      return ret;
    }

    // Get object size
    uint64_t obj_size = s->object->get_obj_size();
    if (obj_size == 0 && s->length) {
      obj_size = s->length;
    }

    // This is the key integration point
    
    RGWStorageClassQuotaChecker quota_checker(
        s->cct, store->getRados(), store->get_observability_cache());
    
    auto quota_result = quota_checker.check_quota(
        s->bucket->get_key(),
        s->user->get_id().to_str(),
        placement_target,
        storage_class,
        obj_size,
        dpp,
        y);
    
    if (!quota_result.is_ok()) {
      ldpp_dout(dpp, 5) << "ERROR: Quota check failed: " 
                        << quota_result.to_string() << dendl;
      
      // Format error response
      std::stringstream ss;
      ss << "Storage quota exceeded. ";
      if (quota_result.storage_class_key.empty()) {
        ss << "Global quota: ";
      } else {
        ss << "Storage class '" << quota_result.storage_class_key << "' quota: ";
      }
      ss << "current usage " << quota_result.current_size 
         << " bytes, limit " << quota_result.limit_size << " bytes";
      
      set_req_state_err(s, -ERR_QUOTA_EXCEEDED, ss.str());
      return -ERR_QUOTA_EXCEEDED;
    }
    
    ldpp_dout(dpp, 20) << "Quota check passed, proceeding with write" << dendl;
    // Continue with normal PUT logic...
    ret = get_params(y);
    if (ret < 0) {
      return ret;
    }

    ret = get_data(dpp, y);
    if (ret < 0) {
      return ret;
    }

  // HSTTODO : handling to be done with existing members of this class
    
    return 0;
  }
};

/**
 * Integration Point 2: RGWCopyObj::execute()
 * 
 * Called for: PUT /dest-bucket/dest-object (with x-amz-copy-source header)
 * 
 * Quota check happens BEFORE copying data.
 */
class RGWCopyObj : public RGWOp {
  // HSTTODO : handling to be done with existing members of this class

  int execute(optional_yield y) override {
    const DoutPrefixProvider* dpp = this;
    
    // Get source object size
    uint64_t src_obj_size = 0;
    int ret = get_source_object_size(&src_obj_size);
    if (ret < 0) {
      return ret;
    }

    // Extract destination storage class
    std::string dest_storage_class = RGW_STORAGE_CLASS_STANDARD;
    auto sc_header = s->info.env->get("HTTP_X_AMZ_STORAGE_CLASS");
    if (sc_header) {
      dest_storage_class = *sc_header;
    } else {
      // Inherit from source if not specified
      dest_storage_class = src_storage_class;
    }

    // Determine destination placement
    std::string placement_target;
    ret = get_placement_target(s, &placement_target);
    if (ret < 0) {
      return ret;
    }

    
    RGWStorageClassQuotaChecker quota_checker(
        s->cct, store->getRados(), store->get_observability_cache());
    
    auto quota_result = quota_checker.check_quota(
        s->bucket->get_key(),
        s->user->get_id().to_str(),
        placement_target,
        dest_storage_class,
        src_obj_size,
        dpp,
        y);
    
    if (!quota_result.is_ok()) {
      ldpp_dout(dpp, 5) << "ERROR: Copy quota check failed: " 
                        << quota_result.to_string() << dendl;
      
      std::stringstream ss;
      ss << "Destination storage quota exceeded for copy operation";
      set_req_state_err(s, -ERR_QUOTA_EXCEEDED, ss.str());
      return -ERR_QUOTA_EXCEEDED;
    }
    

    // Continue with copy operation...
    ret = do_copy(dpp, y);
    return ret;
  }
};

/**
 * Integration Point 3: RGWCompleteMultipart::execute()
 * 
 * Called for: POST /bucket/object?uploadId=X
 * 
 * Quota check happens BEFORE finalizing the multipart upload.
 * This is critical for atomicity.
 */
class RGWCompleteMultipart : public RGWOp {
  // ... existing members ...

  int execute(optional_yield y) override {
    const DoutPrefixProvider* dpp = this;
    
    // Parse the parts list from request
    ret = parse_parts_list();
    if (ret < 0) {
      return ret;
    }

    // Calculate total object size by summing all parts
    uint64_t total_obj_size = 0;
    for (const auto& part : parts) {
      total_obj_size += part.size;
    }

    // Get storage class from upload metadata
    std::string storage_class = upload_meta.storage_class;
    if (storage_class.empty()) {
      storage_class = RGW_STORAGE_CLASS_STANDARD;
    }

    // Get placement target
    std::string placement_target = upload_meta.placement_target;

    // CRITICAL: This must happen atomically with finalization
    // Use a bucket lock to prevent race conditions
    
    RGWBucketStatsLock stats_lock(s->bucket->get_key());
    
    RGWStorageClassQuotaChecker quota_checker(
        s->cct, store->getRados(), store->get_observability_cache());
    
    auto quota_result = quota_checker.check_quota(
        s->bucket->get_key(),
        s->user->get_id().to_str(),
        placement_target,
        storage_class,
        total_obj_size,
        dpp,
        y);
    
    if (!quota_result.is_ok()) {
      ldpp_dout(dpp, 5) << "ERROR: Multipart quota check failed: " 
                        << quota_result.to_string() << dendl;
      
      // Quota exceeded - abort and cleanup
      ret = abort_multipart_upload(upload_id, dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: Failed to abort multipart upload after "
                          << "quota failure: " << cpp_strerror(ret) << dendl;
      }
      
      std::stringstream ss;
      ss << "Storage quota exceeded, multipart upload aborted";
      set_req_state_err(s, -ERR_QUOTA_EXCEEDED, ss.str());
      return -ERR_QUOTA_EXCEEDED;
    }
    
    // Quota OK - proceed with finalization
    ret = finalize_multipart_upload(dpp, y);
    
    // Lock automatically released when stats_lock goes out of scope

    return ret;
  }
};

/**
 * Integration Point 4: Lifecycle Transitions
 * 
 * Called when lifecycle rules transition objects between storage classes.
 * 
 * Example: STANDARD -> GLACIER transition
 */
class RGWLifecycleTransitioner {
  
  int transition_object(
      const rgw_bucket& bucket,
      const std::string& user_id,
      const std::string& obj_key,
      const std::string& src_storage_class,
      const std::string& dest_storage_class,
      uint64_t obj_size,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    // Get destination placement
    std::string dest_placement;
    int ret = get_placement_for_storage_class(dest_storage_class, &dest_placement);
    if (ret < 0) {
      return ret;
    }

    // QUOTA CHECK
    
    RGWStorageClassQuotaChecker quota_checker(
        cct, store, observability_cache);
    
    auto quota_result = quota_checker.check_quota(
        bucket,
        user_id,
        dest_placement,
        dest_storage_class,
        obj_size,
        dpp,
        y);
    
    if (!quota_result.is_ok()) {
      ldpp_dout(dpp, 10) << "WARNING: Lifecycle transition skipped due to "
                         << "destination quota: " << quota_result.to_string() << dendl;
      
      // Don't fail - just skip this transition
      // Object remains in source storage class
      
      // Emit metric for monitoring
      emit_lifecycle_quota_skip_metric(bucket, dest_storage_class);
      
      return -EDQUOT;  // Special return code: quota exceeded but not an error
    }
    
    // Quota OK - proceed with transition
    ret = do_transition(obj_key, src_storage_class, dest_storage_class, dpp, y);
    return ret;
  }
};

/**
 * Error Response Formatter
 * 
 * Formats storage class quota errors for S3 API compatibility
 */
void format_quota_error_response(
    struct req_state* s,
    const RGWQuotaCheckResult& result)
{
  std::stringstream msg;
  
  switch (result.status) {
    case RGWQuotaCheckResult::GLOBAL_SIZE_EXCEEDED:
      msg << "Bucket size quota exceeded. "
          << "Current: " << result.current_size << " bytes, "
          << "Limit: " << result.limit_size << " bytes";
      break;
      
    case RGWQuotaCheckResult::GLOBAL_OBJECTS_EXCEEDED:
      msg << "Bucket object count quota exceeded. "
          << "Current: " << result.current_objects << " objects, "
          << "Limit: " << result.limit_objects << " objects";
      break;
      
    case RGWQuotaCheckResult::SC_SIZE_EXCEEDED:
      msg << "Storage class '" << result.storage_class_key << "' size quota exceeded. "
          << "Current: " << result.current_size << " bytes, "
          << "Limit: " << result.limit_size << " bytes";
      break;
      
    case RGWQuotaCheckResult::SC_OBJECTS_EXCEEDED:
      msg << "Storage class '" << result.storage_class_key << "' object count quota exceeded. "
          << "Current: " << result.current_objects << " objects, "
          << "Limit: " << result.limit_objects << " objects";
      break;
      
    case RGWQuotaCheckResult::BUCKET_NOT_CONVERTED:
      msg << "Bucket not converted for storage class quotas. "
          << "Run 'radosgw-admin bucket check --fix --bucket=<name>' to enable.";
      break;
      
    default:
      msg << "Quota check failed";
  }
  // "Slowdown" reference taken from S3
  s->err.http_ret = 503;
  s->err.err_code = "SlowDown";
  s->err.message = msg.str();
  
  // Add custom headers for programmatic quota handling
  if (!result.storage_class_key.empty()) {
    s->response_headers["x-rgw-quota-storage-class"] = result.storage_class_key;
  }
  s->response_headers["x-rgw-quota-current"] = std::to_string(result.current_size);
  s->response_headers["x-rgw-quota-limit"] = std::to_string(result.limit_size);
}

/**
 * Configuration Options for Storage Class Quotas
 * 
 * HSTTODO : we need to add to src/common/config_opts_rgw.h
 */

// Enable storage class quota enforcement
// Default: false (for backward compatibility)
OPTION(rgw_storage_class_quota_enabled, OPT_BOOL)
.set_default(false)
.set_description("Enable per-storage-class quota enforcement")
.set_long_description(
    "When enabled, RGW enforces per-storage-class quotas in addition to "
    "global bucket and user quotas. Requires buckets to be converted with "
    "'radosgw-admin bucket check --fix'.")
.add_tag("rgw")

// Quota check timeout (milliseconds)
// Default: 5000ms (5 seconds)
OPTION(rgw_storage_class_quota_check_timeout_ms, OPT_U32)
.set_default(5000)
.set_min(100)
.set_max(30000)
.set_description("Timeout for storage class quota checks")
.set_long_description(
    "Maximum time to wait for quota check to complete. If exceeded, "
    "quota check fails and request is rejected.")
.add_tag("rgw")

// Priority refresh threshold (percentage)
// Default: 90% (refresh more often when near quota)
OPTION(rgw_storage_class_quota_priority_refresh_threshold, OPT_U32)
.set_default(90)
.set_min(50)
.set_max(100)
.set_description("Usage percentage to trigger priority quota cache refresh")
.set_long_description(
    "When a storage class's usage exceeds this percentage of its quota, "
    "the observability cache will refresh stats more frequently to ensure "
    "accurate quota enforcement.")
.add_tag("rgw")

// Quota enforcement mode for new buckets
// Default: LEGACY (global quotas only)
OPTION(rgw_storage_class_quota_default_mode, OPT_STR)
.set_default("legacy")
.set_enum_allowed({"legacy", "global", "storage_class", "hybrid"})
.set_description("Default quota enforcement mode for new buckets")
.set_long_description(
    "Determines how quotas are enforced for newly created buckets:\n"
    "  legacy: Only global quotas (backward compatible)\n"
    "  global: Only global quotas (explicit)\n"
    "  storage_class: Only per-storage-class quotas\n"
    "  hybrid: Both global and per-storage-class quotas")
.add_tag("rgw")

/**
 * Performance Metrics
 * 
 * Add to RGW performance counters for monitoring quota checks
 */
enum {
  l_rgw_quota_check_total = 0,
  l_rgw_quota_check_passed,
  l_rgw_quota_check_failed_global,
  l_rgw_quota_check_failed_sc,
  l_rgw_quota_check_latency,
  l_rgw_quota_cache_hit,
  l_rgw_quota_cache_miss,
  l_rgw_quota_last
};

static PerfCountersBuilder rgw_quota_perf_builder(
    cct, "rgw_storage_class_quota", l_rgw_quota_first, l_rgw_quota_last);

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_check_total,
    "quota_checks_total",
    "Total number of storage class quota checks");

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_check_passed,
    "quota_checks_passed",
    "Number of quota checks that passed");

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_check_failed_global,
    "quota_checks_failed_global",
    "Number of quota checks failed due to global quota");

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_check_failed_sc,
    "quota_checks_failed_storage_class",
    "Number of quota checks failed due to storage class quota");

rgw_quota_perf_builder.add_time_avg(
    l_rgw_quota_check_latency,
    "quota_check_latency",
    "Average latency of quota checks (microseconds)");

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_cache_hit,
    "quota_cache_hit",
    "Number of quota checks served from cache");

rgw_quota_perf_builder.add_u64_counter(
    l_rgw_quota_cache_miss,
    "quota_cache_miss",
    "Number of quota checks that missed cache");