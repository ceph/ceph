// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "bucket_index.h"

#include <algorithm>
#include <concepts>

#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "common/dout.h"

#include "cls/rgw/cls_rgw_client.h"

#include "rgw_aio_throttle.h"
#include "rgw_bucket_layout.h"
#include "rgw_common.h"

#include "rgw_tools.h"
#include "rgw_zone.h"

namespace rgwrados::bucket_index {

auto shard_oid(std::string_view prefix, uint64_t gen,
               const rgw::bucket_index_normal_layout& index, uint32_t shard)
    -> std::string
{
  if (index.num_shards == 0) {
    return std::string{prefix}; // legacy buckets have no gen or shard id
  } else if (gen > 0) {
    return fmt::format("{}.{}.{}", prefix, gen, shard);
  } else {
    // for backward compatibility, gen==0 is not added in the object name
    return fmt::format("{}.{}", prefix, shard);
  }
}

namespace {

constexpr std::string_view dir_oid_prefix = ".dir.";

std::string make_oid_prefix(std::string_view bucket_id)
{
  return fmt::format("{}{}", dir_oid_prefix, bucket_id);
}

int open_index_pool(const DoutPrefixProvider* dpp,
                    librados::Rados& rados,
                    const rgw::SiteConfig& site,
                    const RGWBucketInfo& info,
                    librados::IoCtx& ioctx)
{
  constexpr bool create = true;
  constexpr bool mostly_omap = true;

  const rgw_pool& explicit_pool = info.bucket.explicit_placement.index_pool;
  if (!explicit_pool.empty()) {
    return rgw_init_ioctx(dpp, &rados, explicit_pool,
                          ioctx, create, mostly_omap);
  }

  const rgw_placement_rule* rule = &info.placement_rule;
  if (rule->empty()) {
    const auto& zonegroup = site.get_zonegroup();
    rule = &zonegroup.default_placement;
  }

  const auto& zone = site.get_zone_params();
  auto i = zone.placement_pools.find(rule->name);
  if (i == zone.placement_pools.end()) {
    ldpp_dout(dpp, 0) << "could not find placement rule " << *rule
        << " within zonegroup" << dendl;
    return -EINVAL;
  }

  return rgw_init_ioctx(dpp, &rados, i->second.index_pool,
                        ioctx, create, mostly_omap);
}

// transfer matching entries from one list to another
void transfer_if(rgw::AioResultList& from,
                 rgw::AioResultList& to,
                 std::invocable<int> auto pred)
{
  auto is_error = [&pred] (const rgw::AioResult& e) { return pred(e.result); };
  auto i = std::find_if(from.begin(), from.end(), is_error);
  while (i != from.end()) {
    auto& entry = *i;
    auto next = from.erase(i);
    to.push_back(entry);
    i = std::find_if(next, from.end(), is_error);
  }
}

} // anonymous namespace

int init(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw::SiteConfig& site,
         const RGWBucketInfo& info,
         const rgw::bucket_index_layout_generation& index,
         bool judge_support_logrecord)
{
  if (index.layout.type != rgw::BucketIndexType::Normal) {
    return 0;
  }

  librados::IoCtx ioctx;
  int ret = open_index_pool(dpp, rados, site, info, ioctx);
  if (ret < 0) {
    return ret;
  }
  const std::string oid_prefix = make_oid_prefix(info.bucket.bucket_id);

  // issue up to max_aio requests in parallel
  const auto max_aio = dpp->get_cct()->_conf->rgw_bucket_index_max_aio;
  auto aio = rgw::make_throttle(max_aio, y);
  constexpr uint64_t cost = 1; // 1 throttle unit per request
  constexpr uint64_t id = 0; // ids unused

  // ignore EEXIST errors from exclusive create
  constexpr auto is_error = [] (int r) { return r < 0 && r != -EEXIST; };
  constexpr auto is_ok = [] (int r) { return r == 0; };

  // track successful completions so we can roll back on error
  rgw::AioResultList created;

  for (uint32_t shard = 0; shard < num_shards(index.layout.normal); shard++) {
    librados::ObjectWriteOperation op;
    op.create(true);
    if (judge_support_logrecord) {
      cls_rgw_bucket_init_index2(op);
    } else {
      cls_rgw_bucket_init_index(op);
    }

    rgw_raw_obj obj; // obj.pool is empty and unused
    obj.oid = shard_oid(oid_prefix, index.gen, index.layout.normal, shard);

    auto completed = aio->get(obj, rgw::Aio::librados_op(
            ioctx, std::move(op), y), cost, id);
    ret = rgw::check_for_errors(completed, is_error, dpp,
                                "failed to init index object");
    transfer_if(completed, created, is_ok);

    if (ret < 0) {
      break;
    }
  }

  {
    auto completed = aio->drain();
    int r = rgw::check_for_errors(completed, is_error, dpp,
                                  "failed to init index object");
    if (ret == 0 && r == 0) {
      return 0;
    }
    transfer_if(completed, created, is_ok);
  }

  // on error, try to delete any objects that were successfully created
  for (const rgw::AioResult& e : created) {
    librados::ObjectWriteOperation op;
    op.remove();

    auto completed = aio->get(e.obj, rgw::Aio::librados_op(
            ioctx, std::move(op), y), cost, id);
    std::ignore = rgw::check_for_errors(completed, is_error, dpp,
                                        "failed to remove index object");
  }

  auto completed = aio->drain();
  std::ignore = rgw::check_for_errors(completed, is_error, dpp,
                                      "failed to remove index object");

  return ret;
}

} // namespace rgwrados::bucket_index
