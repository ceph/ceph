// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Per-storage-class quota enforcement engine.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sc_quota_checker.h"
#include <limits>

namespace rgw::quota {

bool sc_quota_would_exceed(int64_t limit, uint64_t current, uint64_t delta) {
  if (limit < 0) return false;  // unlimited
  if (delta > 0 && current > std::numeric_limits<uint64_t>::max() - delta) {
    return true;  // overflow
  }
  return (current + delta) > static_cast<uint64_t>(limit);
}

bool sc_enforcement_active(const RGWQuotaInfo& q) {
  switch (q.enforcement_mode) {
    case RGWQuotaEnforcementMode::STORAGE_CLASS:
    case RGWQuotaEnforcementMode::HYBRID:
      return q.has_any_sc_quota();
    case RGWQuotaEnforcementMode::LEGACY:
    case RGWQuotaEnforcementMode::GLOBAL_ONLY:
      return false;
  }
  return false;
}

EffectiveScQuota combine_sc_quota(const RGWStorageClassQuota* bq,
                                   const RGWStorageClassQuota* uq) {
  EffectiveScQuota out;
  auto fold = [&](const RGWStorageClassQuota* q) {
    if (!q || !q->enabled) return;
    if (q->max_size >= 0) {
      if (!out.have_size_limit || q->max_size < out.max_size) {
        out.max_size = q->max_size;
      }
      out.have_size_limit = true;
    }
    if (q->max_objects >= 0) {
      if (!out.have_object_limit || q->max_objects < out.max_objects) {
        out.max_objects = q->max_objects;
      }
      out.have_object_limit = true;
    }
  };
  fold(bq);
  fold(uq);
  return out;
}

} // namespace rgw::quota