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

/*
 * Live per-storage-class usage numbers, as observed by the obs cache.
 *
 * Why a struct rather than two parallel return values: future extensions
 * (e.g. per-SC inflight-bytes for in-flight overshoot accounting) want
 * to add fields without touching every call site.
 */
struct ScUsageStats {
  uint64_t size        = 0;  // total bytes currently stored in this SC
  uint64_t num_objects = 0;  // total object count in this SC
};

/*
 * Storage-class usage stats source.
 *
 * Implemented by the observability cache (PR #66109).  Kept here as a pure
 * abstract interface so:
 *
 *   - This PR can be reviewed and merged independently of #66109.  Until
 *     a provider is installed, the checker fails open -- which is the
 *     correct behaviour by design (see option 3 in the design doc).
 *   - Tests can inject deterministic stat values without spinning up the
 *     full obs-cache subsystem.
 *
 * Implementations MUST be thread-safe; lookup() is called from the write
 * path with no external locking.
 */
class ScUsageStatsProvider {
 public:
  virtual ~ScUsageStatsProvider() = default;

  /*
   * Returns the most recent cached usage for (bucket, sc_key), or
   * std::nullopt if the cache has no data yet (cold start, refresh in
   * progress, key never observed, etc.).  std::nullopt MUST be treated
   * as "fail open" by the caller.
   *
   * Must not block on RADOS.  This is called per-PUT.
   */
  virtual std::optional<ScUsageStats> lookup(
      const rgw_bucket& bucket,
      const std::string& sc_key) const = 0;
};

/*
 * Process-global provider registration.
 *
 * The obs-cache subsystem calls set_sc_stats_provider() during RGW boot
 * once its background refresh thread is running.  Passing nullptr unsets.
 *
 * Lifetime: the caller (the obs cache) owns the object.  RGW's shutdown
 * sequence will call set_sc_stats_provider(nullptr) before tearing down
 * the obs cache so we never see a dangling pointer.
 *
 * The pointer is stored in a std::atomic<> so the read side
 * (get_sc_stats_provider, called by the write path) does not need a lock.
 */
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
 *   dpp         : for ldpp_dout() tracing.
 *   quota       : the aggregated (bucket + user) RGWQuota for this op,
 *                 as already built by get_quota_info() in rgw_op.cc.
 *   bucket      : the destination bucket.  Used as the obs-cache key.
 *   placement   : the destination placement rule (name + storage_class).
 *                 Already filled in by rgw_build_object_placement() for
 *                 every write-path op before we get here.
 *   new_size    : bytes about to be added by this op.  For RGWPutObj's
 *                 pre-check this is content_length; for the post-check
 *                 it is the actual streamed size.  For RGWCopyObj it is
 *                 the source object's accounted size.  For multipart it
 *                 is the sum of part sizes.
 *   new_objects : object count about to be added (almost always 1, but
 *                 multipart-complete or bulk-upload paths may pass more).
 *   y           : the request's optional_yield, passed through for
 *                 future async provider implementations.  Currently the
 *                 default provider does not block on it.
 */
int rgw_check_storage_class_quota(const DoutPrefixProvider* dpp,
                                  const RGWQuota& quota,
                                  const rgw_bucket& bucket,
                                  const rgw_placement_rule& placement,
                                  uint64_t new_size,
                                  uint64_t new_objects,
                                  optional_yield y);

} // namespace rgw::quota
