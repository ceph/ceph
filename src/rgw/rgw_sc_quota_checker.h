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
 *   1. The legacy quota path (RGWQuotaHandler) is tightly coupled to RADOS;
 *      our checker must be usable from any SAL backend.
 *   2. The legacy path owns its own cache.  We do NOT want a second cache
 *      for SC stats -- we read from the observability cache (PR #66109).
 *   3. Mixing the two would force PR #66109 and Pedro's per-SC stats PR
 *      (#66501) into one giant patch series.  Splitting them keeps each
 *      reviewable on its own merits.
 *
 * The checker exposes a single hot-path function -- rgw_check_storage_class_quota
 * -- plus a tiny stats-provider interface that lets the obs-cache PR plug
 * itself in without us taking a build-time dependency on it.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "common/async/yield_context.h"
#include "common/dout.h"

#include "rgw_quota_types.h"      // RGWQuotaInfo, RGWQuota
#include "rgw_sc_quota_types.h"   // RGWStorageClassQuota, rgw_sc_quota_key
#include "rgw_placement_types.h"  // rgw_placement_rule

struct rgw_bucket;

namespace rgw::quota {


struct ScUsageStats {
  uint64_t size        = 0;  // total bytes currently stored in this SC
  uint64_t num_objects = 0;  // total object count in this SC
};

class ScUsageStatsProvider {
 public:
  virtual ~ScUsageStatsProvider() = default;

 
  virtual std::optional<ScUsageStats> lookup(
      const rgw_bucket& bucket,
      const std::string& sc_key) const = 0;
};

void set_sc_stats_provider(ScUsageStatsProvider* p);
ScUsageStatsProvider* get_sc_stats_provider();

/*
 * The hot-path entry point.
 *
 * Called from rgw_op.cc on every write that could touch a quota-managed
 * storage class.  Returns:
 *
 *   0           : write is allowed (either no SC quota applies, or
 *                 it applies and there is sufficient headroom).
 *   -EDQUOT     : write would exceed a configured SC quota.  rgw_op.cc
 *                 already translates -EDQUOT into S3 QuotaExceeded.
 *   other < 0   : unrecoverable internal error.  In practice we never
 *                 return such a code: any internal error path falls
 *                 through to "fail open" and returns 0.
 *
 * Arguments:
 *   quota       : the aggregated (bucket + user) RGWQuota for this op,
 *                 as already built by get_quota_info() in rgw_op.cc.
 *   bucket      : the destination bucket.  
 *   placement   : the destination placement rule (name + storage_class).
 *                 Already filled in by rgw_build_object_placement() for
 *                 every write-path op before we get here.
 *   new_size    : bytes about to be added by this op.  For multipart it
 *                 is the sum of part sizes.
 *   new_objects : object count about to be added (almost always 1).           
 */
int rgw_check_storage_class_quota(const DoutPrefixProvider* dpp,
                                  const RGWQuota& quota,
                                  const rgw_bucket& bucket,
                                  const rgw_placement_rule& placement,
                                  uint64_t new_size,
                                  uint64_t new_objects,
                                  optional_yield y);

} // namespace rgw::quota