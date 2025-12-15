// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/encoding.h"
#include "common/Formatter.h"
#include <unordered_map>
#include <string>

/**
 * Storage Class Quota Configuration
 * 
 * Represents quota limits for a specific storage class within a placement target.
 * Key format: "{placement-target}::{storage-class}"
 * Example: "default-placement::SSD", "nvme-placement::NVME"
 */

struct RGWStorageClassQuota {
  int64_t max_size{-1};        // Maximum size in bytes (-1 = unlimited)
  int64_t max_objects{-1};     // Maximum number of objects (-1 = unlimited)
  bool enabled{false};         // Whether this quota is enforced

  RGWStorageClassQuota() = default;
  RGWStorageClassQuota(int64_t size, int64_t objects, bool en = true)
    : max_size(size), max_objects(objects), enabled(en) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_size, bl);
    encode(max_objects, bl);
    encode(enabled, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_size, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const {
    f->dump_int("max_size", max_size);
    f->dump_int("max_objects", max_objects);
    f->dump_bool("enabled", enabled);
  }

  static void generate_test_instances(std::list<RGWStorageClassQuota*>& o) {
    o.push_back(new RGWStorageClassQuota);
    o.push_back(new RGWStorageClassQuota(100 * 1024 * 1024 * 1024LL, 1000, true));
  }

  bool is_unlimited() const {
    return (max_size < 0 && max_objects < 0) || !enabled;
  }

  bool is_size_limited() const {
    return enabled && max_size >= 0;
  }

  bool is_objects_limited() const {
    return enabled && max_objects >= 0;
  }
};
WRITE_CLASS_ENCODER(RGWStorageClassQuota)

/**
 * Quota Enforcement Mode
 * 
 * Determines how quotas are enforced for a bucket or user:
 * - LEGACY: Only global quotas (for non-converted buckets)
 * - GLOBAL_ONLY: Only global quotas (for converted buckets with SC quotas disabled)
 * - STORAGE_CLASS: Only per-storage-class quotas
 * - HYBRID: Both global AND per-storage-class quotas enforced
 */

enum class RGWQuotaEnforcementMode : uint8_t {
  LEGACY = 0,           // Pre-PR #65589 behavior :: hstTODO update the comment later
  GLOBAL_ONLY = 1,      // Global quotas only
  STORAGE_CLASS = 2,    // Per-SC quotas only
  HYBRID = 3            // Both global and per-SC quotas
};

inline void encode(RGWQuotaEnforcementMode mode, ceph::buffer::list& bl) {
  uint8_t val = static_cast<uint8_t>(mode);
  encode(val, bl);
}

inline void decode(RGWQuotaEnforcementMode& mode, ceph::buffer::list::const_iterator& bl) {
  uint8_t val;
  decode(val, bl);
  mode = static_cast<RGWQuotaEnforcementMode>(val);
}

/**
 * Extended RGWQuotaInfo with Per-Storage-Class Support
 * 
 * This extends the existing RGWQuotaInfo structure to support per-storage-class quotas
 * while maintaining backward compatibility.
 * 
 * Usage Example:
 *   RGWQuotaInfo quota;
 *   quota.enabled = true;
 *   quota.max_size = 1TB;  // Global quota
 *   quota.mode = RGWQuotaEnforcementMode::HYBRID;
 *   quota.storage_class_quotas["default-placement::SSD"] = {100GB, 1000, true};
 *   quota.storage_class_quotas["default-placement::HDD"] = {500GB, 5000, true};
 */
struct RGWQuotaInfoExtended {
  // Existing global quota fields
  int64_t max_size{-1};
  int64_t max_size_kb{0};      // Deprecated, use max_size
  int64_t max_objects{-1};
  bool enabled{false};
  bool check_on_raw{false};
  
  // Per-storage-class quotas
  // Key format: "{placement-target}::{storage-class}"
  std::unordered_map<std::string, RGWStorageClassQuota> storage_class_quotas;
  
  // Enforcement mode
  RGWQuotaEnforcementMode mode{RGWQuotaEnforcementMode::LEGACY};

  RGWQuotaInfoExtended() = default;

  void encode(ceph::buffer::list& bl) const {
    // Use encoding version 3 to add storage class support
    ENCODE_START(3, 1, bl);
    encode(max_size, bl);
    if (max_size < 0) {
      encode(max_size_kb, bl);
    } else {
      encode(static_cast<int64_t>(0), bl);
    }
    encode(max_objects, bl);
    encode(enabled, bl);
    encode(check_on_raw, bl);
    // Version 3 additions
    encode(storage_class_quotas, bl);
    encode(mode, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(max_size, bl);
    decode(max_size_kb, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    if (struct_v >= 2) {
      decode(check_on_raw, bl);
    }
    // Version 3 additions
    if (struct_v >= 3) {
      decode(storage_class_quotas, bl);
      decode(mode, bl);
    } else {
      // Legacy encoding - default to LEGACY mode
      mode = RGWQuotaEnforcementMode::LEGACY;
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const {
    f->dump_int("max_size", max_size);
    f->dump_int("max_size_kb", max_size_kb);
    f->dump_int("max_objects", max_objects);
    f->dump_bool("enabled", enabled);
    f->dump_bool("check_on_raw", check_on_raw);
    
    f->open_array_section("storage_class_quotas");
    for (const auto& [sc_key, sc_quota] : storage_class_quotas) {
      f->open_object_section("storage_class");
      f->dump_string("key", sc_key);
      sc_quota.dump(f);
      f->close_section();
    }
    f->close_section();
    
    f->dump_int("enforcement_mode", static_cast<int>(mode));
  }

  static void generate_test_instances(std::list<RGWQuotaInfoExtended*>& o) {
    o.push_back(new RGWQuotaInfoExtended);
    
    auto q = new RGWQuotaInfoExtended;
    q->enabled = true;
    q->max_size = 1024 * 1024 * 1024 * 1024LL;  // 1TB
    q->max_objects = 1000000;
    q->mode = RGWQuotaEnforcementMode::HYBRID;
    q->storage_class_quotas["default-placement::SSD"] = 
        RGWStorageClassQuota(100 * 1024 * 1024 * 1024LL, 10000, true);
    q->storage_class_quotas["default-placement::HDD"] = 
        RGWStorageClassQuota(500 * 1024 * 1024 * 1024LL, 50000, true);
    o.push_back(q);
  }

  // Check if any storage class quotas are configured
  bool has_storage_class_quotas() const {
    return !storage_class_quotas.empty();
  }

  // Get quota for specific storage class
  const RGWStorageClassQuota* get_sc_quota(const std::string& sc_key) const {
    auto it = storage_class_quotas.find(sc_key);
    return (it != storage_class_quotas.end()) ? &it->second : nullptr;
  }

  // Check if storage class quotas should be enforced
  bool enforce_storage_class_quotas() const {
    return (mode == RGWQuotaEnforcementMode::STORAGE_CLASS ||
            mode == RGWQuotaEnforcementMode::HYBRID) &&
           has_storage_class_quotas();
  }

  // Check if global quotas should be enforced
  bool enforce_global_quotas() const {
    return enabled && (mode == RGWQuotaEnforcementMode::LEGACY ||
                       mode == RGWQuotaEnforcementMode::GLOBAL_ONLY ||
                       mode == RGWQuotaEnforcementMode::HYBRID);
  }
};
WRITE_CLASS_ENCODER(RGWQuotaInfoExtended)

/**
 * Quota Check Result
 * 
 * Returned by quota check operations to indicate success or failure
 * with detailed information about which quota was exceeded.
 */
struct RGWQuotaCheckResult {
  enum Status {
    OK = 0,
    GLOBAL_SIZE_EXCEEDED = 1,
    GLOBAL_OBJECTS_EXCEEDED = 2,
    SC_SIZE_EXCEEDED = 3,
    SC_OBJECTS_EXCEEDED = 4,
    BUCKET_NOT_CONVERTED = 5,
    ERROR = 6
  };

  Status status{OK};
  std::string storage_class_key;  // Populated if SC quota exceeded
  int64_t current_size{0};
  int64_t current_objects{0};
  int64_t limit_size{-1};
  int64_t limit_objects{-1};

  bool is_ok() const { return status == OK; }
  bool is_exceeded() const { 
    return status == GLOBAL_SIZE_EXCEEDED ||
           status == GLOBAL_OBJECTS_EXCEEDED ||
           status == SC_SIZE_EXCEEDED ||
           status == SC_OBJECTS_EXCEEDED;
  }

  int to_errno() const {
    if (is_ok()) return 0;
    if (status == BUCKET_NOT_CONVERTED) return -EINVAL;
    if (is_exceeded()) return -ERR_QUOTA_EXCEEDED;
    return -EIO;
  }

  std::string to_string() const {
    switch (status) {
      case OK: return "OK";
      case GLOBAL_SIZE_EXCEEDED: return "Global size quota exceeded";
      case GLOBAL_OBJECTS_EXCEEDED: return "Global object count quota exceeded";
      case SC_SIZE_EXCEEDED: 
        return "Storage class '" + storage_class_key + "' size quota exceeded";
      case SC_OBJECTS_EXCEEDED: 
        return "Storage class '" + storage_class_key + "' object count quota exceeded";
      case BUCKET_NOT_CONVERTED: 
        return "Bucket not converted for storage class quotas";
      case ERROR: return "Quota check error";
      default: return "Unknown";
    }
  }
};

/**
 * Quota Statistics for Admin Display
 * 
 * Provides a view of current usage vs. quota limits for a bucket or user.
 */
struct RGWQuotaStats {
  struct SCStats {
    std::string storage_class_key;
    int64_t current_size{0};
    int64_t current_objects{0};
    int64_t limit_size{-1};
    int64_t limit_objects{-1};
    bool quota_enabled{false};

    double size_usage_pct() const {
      if (limit_size < 0 || !quota_enabled) return 0.0;
      return (static_cast<double>(current_size) / limit_size) * 100.0;
    }

    double objects_usage_pct() const {
      if (limit_objects < 0 || !quota_enabled) return 0.0;
      return (static_cast<double>(current_objects) / limit_objects) * 100.0;
    }
  };

  int64_t global_current_size{0};
  int64_t global_current_objects{0};
  int64_t global_limit_size{-1};
  int64_t global_limit_objects{-1};
  bool global_quota_enabled{false};

  std::vector<SCStats> storage_class_stats;

  void dump(ceph::Formatter* f) const {
    f->open_object_section("global");
    f->dump_int("current_size", global_current_size);
    f->dump_int("current_objects", global_current_objects);
    f->dump_int("limit_size", global_limit_size);
    f->dump_int("limit_objects", global_limit_objects);
    f->dump_bool("enabled", global_quota_enabled);
    if (global_limit_size >= 0) {
      double pct = (static_cast<double>(global_current_size) / global_limit_size) * 100.0;
      f->dump_float("usage_percentage", pct);
    }
    f->close_section();

    f->open_array_section("storage_classes");
    for (const auto& sc : storage_class_stats) {
      f->open_object_section("storage_class");
      f->dump_string("key", sc.storage_class_key);
      f->dump_int("current_size", sc.current_size);
      f->dump_int("current_objects", sc.current_objects);
      f->dump_int("limit_size", sc.limit_size);
      f->dump_int("limit_objects", sc.limit_objects);
      f->dump_bool("enabled", sc.quota_enabled);
      if (sc.limit_size >= 0) {
        f->dump_float("usage_percentage", sc.size_usage_pct());
      }
      f->close_section();
    }
    f->close_section();
  }
};

#endif  // CEPH_RGW_SC_QUOTA_TYPES_H