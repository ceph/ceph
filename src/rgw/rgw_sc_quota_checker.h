// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Per-storage-class quota enforcement engine.
 *
 * This is the layer that turns the configured RGWStorageClassQuota values
 * (in RGWQuotaInfo) into accept/reject decisions on the write path.  It is
 * deliberately separated from rgw_quota.{h,cc} because:
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "common/dout.h"

#include "rgw_quota_types.h"      // RGWQuotaInfo, RGWQuota
#include "rgw_sc_quota_types.h"   // RGWStorageClassQuota, rgw_sc_quota_key
#include "rgw_placement_types.h"  // rgw_placement_rule

namespace rgw::quota {

// Current usage for one storage class, as read from RGWStorageStats.
struct ScUsageStats {
  uint64_t size = 0;
  uint64_t num_objects = 0;
};

// The effective (tighter) limit when both bucket and user quotas apply.
struct EffectiveScQuota {
  bool have_size_limit = false;
  bool have_object_limit = false;
  int64_t max_size = -1;
  int64_t max_objects = -1;
};

bool sc_enforcement_active(const RGWQuotaInfo& q);

EffectiveScQuota combine_sc_quota(const RGWStorageClassQuota* bq,
                                   const RGWStorageClassQuota* uq);

bool sc_quota_would_exceed(int64_t limit, uint64_t current, uint64_t delta);

} // namespace rgw::quota