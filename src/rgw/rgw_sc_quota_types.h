// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Per-storage-class quota types.
 *
 * This header defines the fundamental serialized types that extend
 * RGWQuotaInfo with a per-storage-class breakdown.  It is intentionally
 * minimal and free of radosgw / OSD-context includes so that it can be
 * pulled in from headers shared with the OSD-side bucket index code.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "include/encoding.h"
#include "rgw_placement_types.h"

class JSONObj;
namespace ceph { class Formatter; }

/*
 * RGWStorageClassQuota
 *
 * Holds a single per-storage-class quota limit.  An instance lives inside
 * RGWQuotaInfo's storage_class_quotas map, keyed by
 *   rgw_sc_quota_key(placement, storage_class).
 *
 * Field semantics intentionally mirror RGWQuotaInfo so that callers can
 * reason about them identically:
 *   max_size    : maximum total bytes allowed in this storage class.
 *                 A value < 0 means "unlimited".
 *   max_objects : maximum total object count.  < 0 means "unlimited".
 *   enabled     : if false, the quota is configured but not enforced.
 *                
 */
struct RGWStorageClassQuota {
  int64_t max_size    = -1;
  int64_t max_objects = -1;
  bool    enabled     = false;

  RGWStorageClassQuota() = default;
  RGWStorageClassQuota(int64_t sz, int64_t objs, bool en)
    : max_size(sz), max_objects(objs), enabled(en) {}

  bool has_size_limit()    const { return enabled && max_size    >= 0; }
  bool has_object_limit()  const { return enabled && max_objects >= 0; }
  bool is_active()         const { return enabled && (max_size >= 0 || max_objects >= 0); }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_size,    bl);
    encode(max_objects, bl);
    encode(enabled,     bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_size,    bl);
    decode(max_objects, bl);
    decode(enabled,     bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWStorageClassQuota)

/*
 * RGWQuotaEnforcementMode
 *
 * Controls which quota dimensions are evaluated on the write path.
 *
 *   LEGACY        : Behaves exactly like pre-feature RGW: only the global
 *                   bucket/user quotas are checked.  This is the value
 *                   produced when an old (v3) RGWQuotaInfo blob is decoded,
 *                   so existing buckets get the legacy semantics by default.
 *   GLOBAL_ONLY   : Same as LEGACY semantically, but set explicitly by the
 *                   admin (so the value survives re-encoding without being
 *                   reinterpreted).
 *   STORAGE_CLASS : Only per-storage-class quotas are enforced; the global
 *                   bucket/user quota fields are ignored.  Useful for tier
 *                   workloads where the global limit would just be the sum
 *                   of the per-class limits anyway.
 *   HYBRID        : Both global AND per-storage-class quotas are enforced.
 *                   A write must satisfy every applicable limit.  This is
 *                   the mode automatically set by `radosgw-admin quota set`
 *                   when --placement-target/--storage-class are provided.
 *
 * The enum is encoded as uint8_t so its on-wire footprint is one byte.
 */
enum class RGWQuotaEnforcementMode : uint8_t {
  LEGACY        = 0,
  GLOBAL_ONLY   = 1,
  STORAGE_CLASS = 2,
  HYBRID        = 3,
};

inline const char* to_string(RGWQuotaEnforcementMode m) {
  switch (m) {
    case RGWQuotaEnforcementMode::LEGACY:        return "legacy";
    case RGWQuotaEnforcementMode::GLOBAL_ONLY:   return "global_only";
    case RGWQuotaEnforcementMode::STORAGE_CLASS: return "storage_class";
    case RGWQuotaEnforcementMode::HYBRID:        return "hybrid";
  }
  return "unknown";
}

inline bool parse_quota_enforcement_mode(const std::string& s,
                                         RGWQuotaEnforcementMode& out) {
  if      (s == "legacy")        { out = RGWQuotaEnforcementMode::LEGACY;        return true; }
  else if (s == "global_only" ||
           s == "global")        { out = RGWQuotaEnforcementMode::GLOBAL_ONLY;   return true; }
  else if (s == "storage_class" ||
           s == "sc")            { out = RGWQuotaEnforcementMode::STORAGE_CLASS; return true; }
  else if (s == "hybrid" ||
           s == "both")          { out = RGWQuotaEnforcementMode::HYBRID;        return true; }
  return false;
}

/*
 * rgw_sc_quota_key
 *
 * Composite key used to index both the configured per-storage-class
 *
 * Format: "<placement-id>::<storage-class>"
 *
 * The "::" delimiter is chosen deliberately:
 *   - Neither placement IDs nor S3 storage-class names use "::"
 */
inline std::string rgw_sc_quota_key(const std::string& placement_id,
                                    const std::string& storage_class) {
  const std::string& sc =
    rgw_placement_rule::get_canonical_storage_class(storage_class);
  return placement_id + "::" + sc;
}

inline std::string rgw_sc_quota_key(const rgw_placement_rule& rule) {
  return rgw_sc_quota_key(rule.name, rule.storage_class);
}

/*
 * RGWStorageClassQuotaMap
 */
using RGWStorageClassQuotaMap = std::map<std::string, RGWStorageClassQuota>;