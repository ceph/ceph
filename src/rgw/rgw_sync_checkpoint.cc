// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <fmt/format.h>
#include "common/errno.h"
#include "rgw_sync_checkpoint.h"
#include "rgw_sal_rados.h"
#include "rgw_bucket_sync.h"
#include "rgw_data_sync.h"
#include "rgw_http_errors.h"
#include "cls/rgw/cls_rgw_client.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

namespace {

std::string incremental_marker(const rgw_bucket_shard_sync_info& info)
{
  return BucketIndexShardsManager::get_shard_marker(info.inc_marker.position);
}

bool operator<(const std::vector<rgw_bucket_shard_sync_info>& lhs,
               const BucketIndexShardsManager& rhs)
{
  for (size_t i = 0; i < lhs.size(); ++i) {
    const auto& l = incremental_marker(lhs[i]);
    const auto& r = rhs.get(i, "");
    if (l < r) {
      return true;
    }
  }
  return false;
}

bool empty(const BucketIndexShardsManager& markers, int size)
{
  static const std::string empty_string;
  for (int i = 0; i < size; ++i) {
    const auto& m = markers.get(i, empty_string);
    if (!m.empty()) {
      return false;
    }
  }
  return true;
}

std::ostream& operator<<(std::ostream& out, const std::vector<rgw_bucket_shard_sync_info>& rhs)
{
  const char* separator = ""; // first entry has no comma
  out << '[';
  for (auto& i : rhs) {
    out << std::exchange(separator, ", ") << incremental_marker(i);
  }
  return out << ']';
}

std::ostream& operator<<(std::ostream& out, const BucketIndexShardsManager& rhs)
{
  out << '[';
  const char* separator = ""; // first entry has no comma
  for (auto& [i, marker] : rhs.get()) {
    out << std::exchange(separator, ", ") << marker;
  }
  return out << ']';
}

int bucket_source_sync_checkpoint(const DoutPrefixProvider* dpp,
                                  rgw::sal::RadosStore* store,
                                  const RGWBucketInfo& bucket_info,
                                  const RGWBucketInfo& source_bucket_info,
                                  const rgw_sync_bucket_pipe& pipe,
                                  uint64_t latest_gen,
                                  const BucketIndexShardsManager& remote_markers,
                                  ceph::timespan retry_delay,
                                  ceph::coarse_mono_time timeout_at)
{

  const int num_shards = remote_markers.get().size();
  rgw_bucket_sync_status full_status;
  int r = rgw_read_bucket_full_sync_status(dpp, store, pipe, &full_status, null_yield);
  if (r < 0 && r != -ENOENT) { // retry on ENOENT
    return r;
  }

  // wait for incremental
  while (full_status.state != BucketSyncState::Incremental) {
    const auto delay_until = ceph::coarse_mono_clock::now() + retry_delay;
    if (delay_until > timeout_at) {
      lderr(store->ctx()) << "bucket checkpoint timed out waiting to reach incremental sync" << dendl;
      return -ETIMEDOUT;
    }
    ldout(store->ctx(), 1) << "waiting to reach incremental sync.." << dendl;
    std::this_thread::sleep_until(delay_until);

    r = rgw_read_bucket_full_sync_status(dpp, store, pipe, &full_status, null_yield);
    if (r < 0 && r != -ENOENT) { // retry on ENOENT
      return r;
    }
  }

  // wait for latest_gen
  while (full_status.incremental_gen < latest_gen) {
    const auto delay_until = ceph::coarse_mono_clock::now() + retry_delay;
    if (delay_until > timeout_at) {
      lderr(store->ctx()) << "bucket checkpoint timed out waiting to reach "
          "latest generation " << latest_gen << dendl;
      return -ETIMEDOUT;
    }
    ldout(store->ctx(), 1) << "waiting to reach latest gen " << latest_gen
        << ", on " << full_status.incremental_gen << ".." << dendl;
    std::this_thread::sleep_until(delay_until);

    r = rgw_read_bucket_full_sync_status(dpp, store, pipe, &full_status, null_yield);
    if (r < 0 && r != -ENOENT) { // retry on ENOENT
      return r;
    }
  }

  if (full_status.incremental_gen > latest_gen) {
    ldpp_dout(dpp, 1) << "bucket sync caught up with source:\n"
        << "        local gen: " << full_status.incremental_gen << '\n'
        << "       remote gen: " << latest_gen << dendl;
    return 0;
  }

  if (empty(remote_markers, num_shards)) {
    ldpp_dout(dpp, 1) << "bucket sync caught up with empty source" << dendl;
    return 0;
  }

  std::vector<rgw_bucket_shard_sync_info> status;
  status.resize(std::max<size_t>(1, num_shards));
  r = rgw_read_bucket_inc_sync_status(dpp, store, pipe,
                                      full_status.incremental_gen, &status);
  if (r < 0) {
    return r;
  }

  while (status < remote_markers) {
    const auto delay_until = ceph::coarse_mono_clock::now() + retry_delay;
    if (delay_until > timeout_at) {
      ldpp_dout(dpp, 0) << "bucket checkpoint timed out waiting for incremental sync to catch up" << dendl;
      return -ETIMEDOUT;
    }
    ldpp_dout(dpp, 1) << "waiting for incremental sync to catch up:\n"
        << "      local status: " << status << '\n'
        << "    remote markers: " << remote_markers << dendl;
    std::this_thread::sleep_until(delay_until);
    r = rgw_read_bucket_inc_sync_status(dpp, store, pipe,
                                        full_status.incremental_gen, &status);
    if (r < 0) {
      return r;
    }
  }
  ldpp_dout(dpp, 1) << "bucket sync caught up with source:\n"
      << "      local status: " << status << '\n'
      << "    remote markers: " << remote_markers << dendl;
  return 0;
}

int source_bilog_info(const DoutPrefixProvider *dpp,
                      RGWSI_Zone* zone_svc,
                      const rgw_sync_bucket_pipe& pipe,
                      rgw_bucket_index_marker_info& info,
                      BucketIndexShardsManager& markers,
                      optional_yield y)
{
  ceph_assert(pipe.source.zone);

  auto& zone_conn_map = zone_svc->get_zone_conn_map();
  auto conn = zone_conn_map.find(pipe.source.zone->id);
  if (conn == zone_conn_map.end()) {
    return -EINVAL;
  }

  return rgw_read_remote_bilog_info(dpp, conn->second, *pipe.source.bucket,
                                    info, markers, y);
}

} // anonymous namespace

int rgw_bucket_sync_checkpoint(const DoutPrefixProvider* dpp,
                               rgw::sal::RadosStore* store,
                               const RGWBucketSyncPolicyHandler& policy,
                               const RGWBucketInfo& info,
                               std::optional<rgw_zone_id> opt_source_zone,
                               std::optional<rgw_bucket> opt_source_bucket,
                               ceph::timespan retry_delay,
                               ceph::coarse_mono_time timeout_at)
{
  struct sync_source_entry {
    rgw_sync_bucket_pipe pipe;
    uint64_t latest_gen = 0;
    BucketIndexShardsManager remote_markers;
    RGWBucketInfo source_bucket_info;
  };
  std::list<sync_source_entry> sources;

  // fetch remote markers and bucket info in parallel
  boost::asio::io_context ioctx;

  for (const auto& [source_zone_id, pipe] : policy.get_all_sources()) {
    // filter by source zone/bucket
    if (opt_source_zone && *opt_source_zone != *pipe.source.zone) {
      continue;
    }
    if (opt_source_bucket && !opt_source_bucket->match(*pipe.source.bucket)) {
      continue;
    }
    auto& entry = sources.emplace_back();
    entry.pipe = pipe;

    // fetch remote markers
    boost::asio::spawn(ioctx, [&] (boost::asio::yield_context yield) {
      auto y = optional_yield{yield};
      rgw_bucket_index_marker_info info;
      int r = source_bilog_info(dpp, store->svc()->zone, entry.pipe,
                                info, entry.remote_markers, y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "failed to fetch remote bilog markers: "
            << cpp_strerror(r) << dendl;
        throw std::system_error(-r, std::system_category());
      }
      entry.latest_gen = info.latest_gen;
    }, [] (std::exception_ptr eptr) {
      if (eptr) std::rethrow_exception(eptr);
    });
    // fetch source bucket info
    boost::asio::spawn(ioctx, [&] (boost::asio::yield_context yield) {
      auto y = optional_yield{yield};
      int r = store->getRados()->get_bucket_instance_info(
          *entry.pipe.source.bucket, entry.source_bucket_info,
          nullptr, nullptr, y, dpp);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "failed to read source bucket info: "
            << cpp_strerror(r) << dendl;
        throw std::system_error(-r, std::system_category());
      }
    }, [] (std::exception_ptr eptr) {
      if (eptr) std::rethrow_exception(eptr);
    });
  }

  try {
    ioctx.run();
  } catch (const std::system_error& e) {
    return -e.code().value();
  }

  // checkpoint each source sequentially
  for (const auto& e : sources) {
    int r = bucket_source_sync_checkpoint(dpp, store, info, e.source_bucket_info,
                                          e.pipe, e.latest_gen, e.remote_markers,
                                          retry_delay, timeout_at);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "bucket sync checkpoint failed: " << cpp_strerror(r) << dendl;
      return r;
    }
  }
  ldpp_dout(dpp, 0) << "bucket checkpoint complete" << dendl;
  return 0;
}

