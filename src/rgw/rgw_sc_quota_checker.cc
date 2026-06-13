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

#include <atomic>
#include <cerrno>
#include <limits>

#include "common/dout.h"
#include "rgw_common.h"     // rgw_bucket, ldpp_dout

#define dout_subsys ceph_subsys_rgw

namespace rgw::quota {

/*
 * Global provider slot.
 *
 * - We deliberately do NOT use a shared_ptr here.  The obs cache is a
 *   long-lived singleton that outlives all in-flight requests; reference
 *   counting on the hot path would just be overhead.
 * - std::atomic with relaxed ordering on the load is sufficient: stale
 *   reads are tolerable (they fail open by returning a null provider),
 *   and there is no causal dependency between the pointer and any other
 *   memory the consumer reads.  The store side uses release ordering
 *   so that any provider state initialised before the install is visible.
 */
static std::atomic<ScUsageStatsProvider*> g_sc_stats_provider{nullptr};

void set_sc_stats_provider(ScUsageStatsProvider* p) {
  g_sc_stats_provider.store(p, std::memory_order_release);
}

ScUsageStatsProvider* get_sc_stats_provider() {
  return g_sc_stats_provider.load(std::memory_order_acquire);
}

namespace {

/*
 * Evaluate a single (limit, current, delta) triple.
 *
 * Pulled out into a helper so the size and object-count branches read
 * identically and so the overflow check lives in exactly one place.
 *
 * Returns true iff the resulting value would exceed `limit`.
 *
 * NOTE on signed vs unsigned: `limit` is int64_t (matches the on-disk
 * type and "-1 means unlimited" convention) but `current` and `delta`
 * are uint64_t (matches the obs-cache and write-path types).  We
 * already excluded the "unlimited" case before calling this, so a
 * static_cast<uint64_t> of `limit` is safe.
 */
inline bool would_exceed(int64_t limit, uint64_t current, uint64_t delta) {
  // overflow-safe: current + delta cannot exceed UINT64_MAX in practice
  // (bucket sizes are bounded by addressable storage), but we guard
  // anyway because the obs cache could in theory return garbage.
  if (delta > 0 && current > std::numeric_limits<uint64_t>::max() - delta) {
    return true;  // overflow -> definitely over quota
  }
  return (current + delta) > static_cast<uint64_t>(limit);
}

/*
 * Combine the bucket and user SC quotas for a given key into "the
 * effective limit this op must satisfy".
 *
 * The semantics intentionally mirror the existing global-quota path
 * (bucket->check_quota): if both a bucket-level and user-level limit
 * apply, the tighter one wins.  When only one side has a quota, that
 * one is used directly.
 */
struct EffectiveScQuota {
  bool      have_size_limit   = false;
  bool      have_object_limit = false;
  int64_t   max_size          = -1;
  int64_t   max_objects       = -1;
};

EffectiveScQuota combine(const RGWStorageClassQuota* bq,
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

/*
 * Should the per-SC code path run at all for this op?
 *
 * The intent here is to keep the cost of "no SC quotas configured"
 * down to ONE std::map::find on the empty map -- which is the case
 * for the overwhelming majority of buckets in any deployment.
 *
 * Returns true iff *either* enforcement_mode requests it AND at least
 * one side has any enabled per-SC entry that could possibly match.
 *
 * Note we intentionally check `has_any_sc_quota()` rather than the
 * specific key: a bucket may have an SC quota for "SSD" but the write
 * is to "STANDARD".  We let the lookup-by-key step (in the caller)
 * filter that out -- it's cheaper than two map iterations.
 */
inline bool sc_enforcement_active(const RGWQuotaInfo& q) {
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

} // anonymous namespace

int rgw_check_storage_class_quota(const DoutPrefixProvider* dpp,
                                  const RGWQuota& quota,
                                  const rgw_bucket& bucket,
                                  const rgw_placement_rule& placement,
                                  uint64_t new_size,
                                  uint64_t new_objects,
                                  optional_yield y) {
  /* Fast bail-out: neither side has any SC quotas configured. This is
   * the path taken by ~every write in a typical cluster, so it MUST
   * be branch-predictable and allocation-free.  Two integer loads and
   * two `enabled` checks is the entire cost. */
  const bool bucket_active = sc_enforcement_active(quota.bucket_quota);
  const bool user_active   = sc_enforcement_active(quota.user_quota);
  if (!bucket_active && !user_active) {
    return 0;
  }

  /* Compose the lookup key.  See rgw_sc_quota_key() for the format
   * rationale ("placement::storage_class"). */
  const std::string sc_key = rgw_sc_quota_key(placement);

  /* Find the per-SC limit on each side. */
  const RGWStorageClassQuota* bq =
    bucket_active ? quota.bucket_quota.get_sc_quota(sc_key) : nullptr;
  const RGWStorageClassQuota* uq =
    user_active   ? quota.user_quota.get_sc_quota(sc_key)   : nullptr;

  /* The bucket/user quota object enabled an SC quota in general but
   * not for THIS specific (placement, storage_class).  Nothing to do. */
  if ((!bq || !bq->enabled) && (!uq || !uq->enabled)) {
    return 0;
  }

  const EffectiveScQuota lim = combine(bq, uq);
  if (!lim.have_size_limit && !lim.have_object_limit) {
    /* Both sides set enabled=true but neither set an actual limit
     * (max_size=-1 and max_objects=-1).  Treat as "no constraint". */
    return 0;
  }

  /* Pull live usage from the obs cache.  No provider installed, or the
   * cache has no data yet for this key -> fail open.  This is the
   * single most important safety property of this feature: any failure
   * to *acquire* a measurement falls through to "allow the write".
   *
   * Hard-rejecting writes when the cache is missing would turn a stats
   * subsystem outage into a data-plane outage, which is exactly the
   * tradeoff we are explicitly choosing to avoid. */
  ScUsageStatsProvider* provider = get_sc_stats_provider();
  if (!provider) {
    ldpp_dout(dpp, 20)
      << "sc-quota: no stats provider installed; failing open for sc_key="
      << sc_key << dendl;
    return 0;
  }

  const std::optional<ScUsageStats> usage = provider->lookup(bucket, sc_key);
  if (!usage) {
    ldpp_dout(dpp, 20)
      << "sc-quota: no cached stats for sc_key=" << sc_key
      << " bucket=" << bucket << "; failing open" << dendl;
    return 0;
  }

  /* Enforce.  Order: size check first, then object-count.  Either
   * dimension being over the limit returns -EDQUOT; we don't bother
   * computing both -- the order does not affect the externally
   * observable behaviour and a single message is enough for the log. */
  if (lim.have_size_limit &&
      would_exceed(lim.max_size, usage->size, new_size)) {
    ldpp_dout(dpp, 5)
      << "sc-quota: size limit exceeded for sc_key=" << sc_key
      << " bucket=" << bucket
      << " current=" << usage->size
      << " delta=" << new_size
      << " limit=" << lim.max_size
      << dendl;
    return -EDQUOT;
  }

  if (lim.have_object_limit &&
      would_exceed(lim.max_objects, usage->num_objects, new_objects)) {
    ldpp_dout(dpp, 5)
      << "sc-quota: object-count limit exceeded for sc_key=" << sc_key
      << " bucket=" << bucket
      << " current=" << usage->num_objects
      << " delta=" << new_objects
      << " limit=" << lim.max_objects
      << dendl;
    return -EDQUOT;
  }

  ldpp_dout(dpp, 25)
    << "sc-quota: ok sc_key=" << sc_key
    << " size " << usage->size << "+" << new_size
    << "<=" << (lim.have_size_limit ? lim.max_size : -1)
    << " objs " << usage->num_objects << "+" << new_objects
    << "<=" << (lim.have_object_limit ? lim.max_objects : -1)
    << dendl;
  return 0;
}

} // namespace rgw::quota
