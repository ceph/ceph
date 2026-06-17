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
#include "rgw_bucket_layout.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::quota {

static std::atomic<ScUsageStatsProvider*> g_sc_stats_provider{nullptr};

void set_sc_stats_provider(ScUsageStatsProvider* p) {
  g_sc_stats_provider.store(p, std::memory_order_release);
}

ScUsageStatsProvider* get_sc_stats_provider() {
  return g_sc_stats_provider.load(std::memory_order_acquire);
}

namespace {

inline bool would_exceed(int64_t limit, uint64_t current, uint64_t delta) {
  
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
                                  rgw::sal::Driver* driver, 
                                  const RGWQuota& quota,
                                  const rgw_bucket& bucket,
                                  const rgw_placement_rule& placement,
                                  uint64_t new_size,
                                  uint64_t new_objects,
                                  optional_yield y) {
  
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

  if ((!bq || !bq->enabled) && (!uq || !uq->enabled)) {
    return 0;
  }

  const EffectiveScQuota lim = combine(bq, uq);
  if (!lim.have_size_limit && !lim.have_object_limit) {
    /* Both sides set enabled=true but neither set an actual limit
     * (max_size=-1 and max_objects=-1).  Treat as "no constraint". */
    return 0;
  }

  // Get current usage 
  ScUsageStats usage{0, 0};

  ScUsageStatsProvider* provider = get_sc_stats_provider();
  if (provider) {
    // Fast path: obs cache registered
    auto cached = provider->lookup(bucket, sc_key);
    if (!cached) {
      ldpp_dout(dpp, 20) << "sc-quota: no cached stats for sc_key="
                         << sc_key << " failing open" << dendl;
      return 0;
    }
    usage = *cached;
  } else {
    // Fallback: read directly from Pedro's bucket index (PR #66501)
    std::unique_ptr<rgw::sal::Bucket> bkt;
    int r = driver->load_bucket(dpp, bucket, &bkt, y);
    if (r < 0) {
      return 0;
    }

    std::map<RGWObjCategory, RGWStorageStats> cat_stats;
    std::optional<std::map<std::string, RGWStorageStats>> sc_stats;
    sc_stats.emplace(); // must pre-initialize for accumulate_raw_stats to populate it
    std::string bver, mver;
    const auto& idx_layout = bkt->get_info().layout.current_index;

    r = bkt->read_stats(dpp, y, idx_layout, -1, &bver, &mver, cat_stats, sc_stats);
    if (r < 0) {
      return 0;
    }
    if (!sc_stats.has_value()) {
      return 0;
    }

    // Log all keys Pedro's stats actually contain
    for (const auto& [k, v] : *sc_stats) {
    }

    // read_stats keys stats by SC name only ("STANDARD"),
    // not the composite quota key ("default-placement::STANDARD").
    // Extract just the SC name for the lookup.
    std::string sc_name_for_lookup = sc_key;
    auto sep_pos = sc_key.rfind("::");
    if (sep_pos != std::string::npos) {
      sc_name_for_lookup = sc_key.substr(sep_pos + 2);
    }
    auto it = sc_stats->find(sc_name_for_lookup);
    if (it != sc_stats->end()) {
      usage.size        = it->second.size;
      usage.num_objects = it->second.num_objects;
    }
  }

  // Enforce 
  if (lim.have_size_limit &&
    would_exceed(lim.max_size, usage.size, new_size)) {
    ldpp_dout(dpp, 5) << "sc-quota: size exceeded sc_key=" << sc_key
                      << " current=" << usage.size
                      << " delta=" << new_size
                      << " limit=" << lim.max_size << dendl;
    return -EDQUOT;
  }

  if (lim.have_object_limit &&
    would_exceed(lim.max_objects, usage.num_objects, new_objects)) {
    ldpp_dout(dpp, 5) << "sc-quota: object count exceeded sc_key=" << sc_key
                      << " current=" << usage.num_objects
                      << " delta=" << new_objects
                      << " limit=" << lim.max_objects << dendl;
    return -EDQUOT;
  }

  ldpp_dout(dpp, 25) << "sc-quota: ok sc_key=" << sc_key
                     << " size=" << usage.size << "+" << new_size
                     << "<=" << lim.max_size
                     << " objs=" << usage.num_objects << "+" << new_objects
                     << dendl;
  return 0;
}

} // namespace rgw::quota