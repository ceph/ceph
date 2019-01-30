// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "svc_bilog_rados.h"
#include "svc_bi_rados.h"
#include "rgw/rgw_cls.h"
#include "rgw/rgw_error_code.h"
#include "common/expected.h"

#include "RADOS/cls/rgw.h"

namespace ba = boost::asio;
namespace bs = boost::system;
namespace bc = boost::container;
namespace RCr = RADOS::CLS::rgw;

#define dout_subsys ceph_subsys_rgw

RGWSI_BILog_RADOS::RGWSI_BILog_RADOS(CephContext *cct, ba::io_context& ioctx)
  : RGWServiceInstance(cct, ioctx) {}

void RGWSI_BILog_RADOS::init(RGWSI_RADOS* rados,
			     RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.rados = rados;
  svc.bi = bi_rados_svc;
}

bs::error_code RGWSI_BILog_RADOS::log_trim(const RGWBucketInfo& bucket_info,
					   int shard_id,
					   std::string_view start_marker,
					   std::string_view end_marker,
					   optional_yield y)
{
  auto&& [index_pool, bucket_objs, _] =
    TRYE(svc.bi->open_bucket_index(bucket_info, shard_id, y));

  auto start_marker_mgr = RCr::make_bi_shards_mgr(start_marker, shard_id);
  auto end_marker_mgr = RCr::make_bi_shards_mgr(end_marker, shard_id);
  if (!start_marker_mgr || !end_marker_mgr)
    return rgw_errc::internal_error;

  return rgw::cls::bi_log_trim(index_pool, *start_marker_mgr,
			       *end_marker_mgr, bucket_objs,
			       cct->_conf->rgw_bucket_index_max_aio, y);
}

bs::error_code RGWSI_BILog_RADOS::log_start(const RGWBucketInfo& bucket_info,
					    int shard_id,
					    optional_yield y)
{
  auto&& [index_pool, bucket_objs, _] =
    TRYE(svc.bi->open_bucket_index(bucket_info, shard_id, y));

  return rgw::cls::bi_log_resync(index_pool, bucket_objs,
				 cct->_conf->rgw_bucket_index_max_aio, y);
}

bs::error_code RGWSI_BILog_RADOS::log_stop(const RGWBucketInfo& bucket_info,
					   int shard_id, optional_yield y)
{
  auto&& [index_pool, bucket_objs, _] =
    TRYE(svc.bi->open_bucket_index(bucket_info, shard_id, y));

  return rgw::cls::bi_log_stop(index_pool, bucket_objs,
			       cct->_conf->rgw_bucket_index_max_aio, y);
}

static std::string build_bucket_index_marker(std::string_view shard_id_str,
					     std::string_view shard_marker)
{
  return fmt::format("{}{}{}", shard_id_str, RCr::KEY_VALUE_SEPARATOR,
		     shard_marker);
}

tl::expected<std::pair<std::vector<rgw_bi_log_entry>, bool>, bs::error_code>
RGWSI_BILog_RADOS::log_list(const RGWBucketInfo& bucket_info,
			    int shard_id, std::string_view marker,
			    uint32_t max, optional_yield y)
{
  ldout(cct, 20) << __func__ << ": " << bucket_info.bucket << " marker "
		 << marker << " shard_id=" << shard_id << " max " << max
		 << dendl;
  auto&& [index_pool, oids, _] =
    TRY(svc.bi->open_bucket_index(bucket_info, shard_id, y));
  bool has_shards = (oids.size() > 1 || shard_id >= 0);

  // If there are multiple shards for the bucket index object, the marker
  // should have the pattern '{shard_id_1}#{shard_marker_1},{shard_id_2}#
  // {shard_marker_2}...', if there is no sharding, the bi_log_list should
  // only contain one record, and the key is the bucket instance id.
  auto marker_mgr = RCr::make_bi_shards_mgr(marker, shard_id);
  if (!marker_mgr)
    return tl::unexpected(rgw_errc::internal_error);

  auto log_lists =
    TRY(rgw::cls::bi_log_list(index_pool, *marker_mgr, max, oids,
			      cct->_conf->rgw_bucket_index_max_aio, y));

  bc::flat_map<int, std::vector<rgw_bi_log_entry>::iterator> vcurrents;
  bc::flat_map<int, std::vector<rgw_bi_log_entry>::iterator> vends;
  bool truncated = false;

  for (auto& [shard_id, log_list] : log_lists) {
    vcurrents[shard_id] = log_list.entries.begin();
    vends[shard_id] = log_list.entries.end();
    truncated = truncated || log_list.truncated;
  }

  std::vector<rgw_bi_log_entry> result;
  size_t total = 0;
  bool has_more = true;
  decltype(vcurrents)::iterator viter;
  decltype(vends)::iterator eiter;

  while (total < max && has_more) {
    has_more = false;

    viter = vcurrents.begin();
    eiter = vends.begin();

    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());

      int shard_id = viter->first;
      auto& liter = viter->second;

      if (liter == eiter->second) {
        continue;
      }
      rgw_bi_log_entry& entry = *(liter);
      if (has_shards) {
        auto tmp_id = build_bucket_index_marker(fmt::format("{0:d}", shard_id),
						entry.id);
        entry.id.swap(tmp_id);
      }
      marker_mgr->emplace(shard_id, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (viter = vcurrents.begin(), eiter = vends.begin(); viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());
      truncated = truncated || (viter->second != eiter->second);
    }
  }

  // Refresh marker, if there are multiple shards, the output will look like
  // '{shard_oid_1}#{shard_marker_1},{shard_oid_2}#{shard_marker_2}...',
  // if there is no sharding, the simply marker (without oid) is returned
  if (has_shards) {
    marker = RCr::to_string(*marker_mgr);
  } else {
    if (!result.empty()) {
      marker = result.rbegin()->id;
    }
  }

  return std::make_pair(result, truncated);
}

tl::expected<bc::flat_map<int, std::string>, bs::error_code>
RGWSI_BILog_RADOS::get_log_status(const RGWBucketInfo& bucket_info,
				  int shard_id, optional_yield y)
{
  auto [headers, bucket_instance_ids] =
    TRY(svc.bi->cls_bucket_head(bucket_info, shard_id, y));

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  auto viter = bucket_instance_ids.begin();

  bc::flat_map<int, std::string> markers;

  for(; iter != headers.end(); ++iter, ++viter) {
    if (shard_id >= 0) {
      markers[shard_id] = iter->max_marker;
    } else {
      markers[viter->first] = iter->max_marker;
    }
  }

  return markers;
}
