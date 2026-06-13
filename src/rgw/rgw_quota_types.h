// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include "rgw_sc_quota_types.h"

static inline int64_t rgw_rounded_kb(int64_t bytes)
{
  return (bytes + 1023) / 1024;
}

class JSONObj;

struct RGWQuotaInfo {
  int64_t max_size;
  int64_t max_objects;
  bool enabled;
  /* Do we want to compare with raw, not rounded RGWStorageStats::size (true)
   * or maybe rounded-to-4KiB RGWStorageStats::size_rounded (false)? */
  bool check_on_raw;

  /*
   * Per-storage-class quotas (added in encode v4).
   *
   * Keyed by rgw_sc_quota_key("<placement>", "<storage_class>"); each value
   * holds an independent (max_size, max_objects, enabled) tuple.  An empty
   * map means "no per-SC quotas configured" which is the equivalent of the
   * pre-feature behaviour and is exactly what a v3 blob decodes to.
   */
  RGWStorageClassQuotaMap storage_class_quotas;

  /*
   * Which quota dimensions to evaluate on the write path (added in v4).
   * Defaults to LEGACY so that:
   *   1. A freshly-constructed RGWQuotaInfo (no admin action yet) preserves
   *      legacy semantics.
   *   2. An RGWQuotaInfo decoded from a v3 blob -- which has no on-disk
   *      enforcement_mode field -- also reads back as LEGACY.
   * In both cases the existing global-only enforcement behaviour is
   * bit-for-bit preserved.
   */
  RGWQuotaEnforcementMode enforcement_mode = RGWQuotaEnforcementMode::LEGACY;

  RGWQuotaInfo()
    : max_size(-1),
      max_objects(-1),
      enabled(false),
      check_on_raw(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    if (max_size < 0) {
      encode(-rgw_rounded_kb(abs(max_size)), bl);
    } else {
      encode(rgw_rounded_kb(max_size), bl);
    }
    encode(max_objects, bl);
    encode(enabled, bl);
    encode(max_size, bl);
    encode(check_on_raw, bl);
    /* v4 additions.  Appended AFTER all v3 fields so that an old (v3)
     * decoder running against a v4 blob will simply stop at the
     * DECODE_FINISH() boundary and silently ignore them.  This is the
     * standard Ceph "additive" forward-compat pattern.
     *
     * The map is encoded manually (count + key/value pairs) rather than
     * via the std::map template so that nested RGWStorageClassQuota
     * blobs always use the member encode/decode path regardless of how
     * denc/feature traits evolve for std::map in encoding.h. */
    {
      const __u32 n = static_cast<__u32>(storage_class_quotas.size());
      encode(n, bl);
      for (const auto& [key, q] : storage_class_quotas) {
        encode(key, bl);
        q.encode(bl);
      }
    }
    encode(static_cast<uint8_t>(enforcement_mode), bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(4, 1, 1, bl);
    int64_t max_size_kb;
    decode(max_size_kb, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    if (struct_v < 2) {
      max_size = max_size_kb * 1024;
    } else {
      decode(max_size, bl);
    }
    if (struct_v >= 3) {
      decode(check_on_raw, bl);
    }
    if (struct_v >= 4) {
      __u32 n = 0;
      decode(n, bl);
      storage_class_quotas.clear();
      while (n--) {
        std::string key;
        RGWStorageClassQuota q;
        decode(key, bl);
        q.decode(bl);
        storage_class_quotas.emplace(std::move(key), std::move(q));
      }
      uint8_t mode_byte = 0;
      decode(mode_byte, bl);
      enforcement_mode = static_cast<RGWQuotaEnforcementMode>(mode_byte);
    } else {
      /* Explicitly leave defaults: empty SC-quota map and LEGACY mode.
       * This is what gives us "old buckets keep doing exactly what they
       * always did, with zero behavioural change". */
      storage_class_quotas.clear();
      enforcement_mode = RGWQuotaEnforcementMode::LEGACY;
    }
    DECODE_FINISH(bl);
  }

  /*
   * Lookup helper.  Returns a pointer into storage_class_quotas (so the
   * caller can also read max_size/max_objects) or nullptr if no per-SC
   * quota is configured for the given (placement, storage-class) pair.
   *
   * Returning a pointer avoids copying RGWStorageClassQuota on the
   * write-path hot loop -- this is called per-request.
   */
  const RGWStorageClassQuota* get_sc_quota(const std::string& sc_key) const {
    auto it = storage_class_quotas.find(sc_key);
    return it == storage_class_quotas.end() ? nullptr : &it->second;
  }

  /* Convenience: true if any per-SC quota in this RGWQuotaInfo is enabled.
   * Used by the checker to short-circuit out of the per-SC code path on
   * the (very common) write request that has no SC quotas configured. */
  bool has_any_sc_quota() const {
    for (const auto& [_, q] : storage_class_quotas) {
      if (q.enabled) return true;
    }
    return false;
  }

  void dump(Formatter *f) const;
  static std::list<RGWQuotaInfo> generate_test_instances();
  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWQuotaInfo)

struct RGWQuota {
    RGWQuotaInfo user_quota;
    RGWQuotaInfo bucket_quota;
};
