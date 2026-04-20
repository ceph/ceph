// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#ifndef CEPH_RGW_SC_QUOTA_TYPES_H
#define CEPH_RGW_SC_QUOTA_TYPES_H

#include "include/encoding.h"
#include <cstdint>
#include <string>

// Per-storage-class quota limits
struct RGWStorageClassQuota {
  int64_t max_size;      // -1 = unlimited
  int64_t max_objects;   // -1 = unlimited
  bool enabled;

  RGWStorageClassQuota()
    : max_size(-1),
      max_objects(-1),
      enabled(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_size, bl);
    encode(max_objects, bl);
    encode(enabled, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_size, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWStorageClassQuota)

// Quota enforcement modes
enum class RGWQuotaEnforcementMode : uint32_t {
  LEGACY = 0,         // Backward compat - only global quotas
  GLOBAL_ONLY = 1,    // Enforce only global quotas
  STORAGE_CLASS = 2,  // Enforce only per-SC quotas
  HYBRID = 3          // Enforce both (most restrictive wins)
};

inline void encode(RGWQuotaEnforcementMode mode, bufferlist& bl) {
  encode((uint32_t)mode, bl);
}

inline void decode(RGWQuotaEnforcementMode& mode, bufferlist::const_iterator& bl) {
  uint32_t val;
  decode(val, bl);
  mode = (RGWQuotaEnforcementMode)val;
}

#endif // CEPH_RGW_SC_QUOTA_TYPES_H