// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "svc_bilog.h"
#include "svc_bi.h"

#include "cls/rgw/cls_rgw_client.h"


#define dout_subsys ceph_subsys_rgw

RGWSI_BILog::RGWSI_BILog(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_BILog::init(RGWSI_BucketIndex *bi_svc)
{
  svc.bi = bi_svc;
}

int RGWSI_BILog::log_trim(const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  int shard_id,
			  std::string_view start_marker,
			  std::string_view end_marker)
{
  librados::IoCtx index_pool;
  std::map<int, string> bucket_objs;

  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, &index_pool,
				    &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  r = start_marker_mgr.from_string(start_marker, shard_id);
  if (r < 0) {
    return r;
  }

  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBILogTrim(index_pool, start_marker_mgr, end_marker_mgr,
			      bucket_objs,
			      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog::log_start(const DoutPrefixProvider* dpp,
			   const RGWBucketInfo& bucket_info,
			   int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, &index_pool,
				    &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueResyncBucketBILog(index_pool, bucket_objs,
				      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog::log_stop(const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, &index_pool,
				    &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueBucketBILogStop(index_pool, bucket_objs,
				    cct->_conf->rgw_bucket_index_max_aio)();
}

namespace {
void build_bucket_index_marker(std::string_view shard_id_str,
			       std::string_view shard_marker,
			       std::string* marker) {
  if (marker) {
    *marker = shard_id_str;
    marker->append(BucketIndexShardsManager::KEY_VALUE_SEPARATOR);
    marker->append(shard_marker);
  }
}
}

int RGWSI_BILog::log_list(const DoutPrefixProvider* dpp,
			  const RGWBucketInfo& bucket_info,
			  int shard_id,
			  std::string& marker, uint32_t max,
			  std::vector<rgw_bi_log_entry>& result,
			  bool* truncated)
{
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket << " marker "
		     << marker << " shard_id=" << shard_id << " max " << max
		     << dendl;
  result.clear();

  librados::IoCtx index_pool;
  std::map<int, string> oids;
  std::map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, &index_pool,
				    &oids, nullptr);
  if (r < 0)
    return r;

  BucketIndexShardsManager marker_mgr;
  bool has_shards = (oids.size() > 1 || shard_id >= 0);
  // If there are multiple shards for the bucket index object, the marker
  // should have the pattern '{shard_id_1}#{shard_marker_1},{shard_id_2}#
  // {shard_marker_2}...', if there is no sharding, the bi_log_list should
  // only contain one record, and the key is the bucket instance id.
  r = marker_mgr.from_string(marker, shard_id);
  if (r < 0)
    return r;

  r = CLSRGWIssueBILogList(index_pool, marker_mgr, max, oids, bi_log_lists,
			   cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  std::map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  std::map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  for (auto miter = bi_log_lists.begin(); miter != bi_log_lists.end(); ++miter) {
    int shard_id = miter->first;
    vcurrents[shard_id] = miter->second.entries.begin();
    vends[shard_id] = miter->second.entries.end();
    if (truncated) {
      *truncated = (*truncated || miter->second.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  std::map<int, list<rgw_bi_log_entry>::iterator>::iterator viter;
  std::map<int, list<rgw_bi_log_entry>::iterator>::iterator eiter;
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
        string tmp_id;
        build_bucket_index_marker(std::to_string(shard_id), entry.id, &tmp_id);
        entry.id.swap(tmp_id);
      }
      marker_mgr.add(shard_id, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (viter = vcurrents.begin(), eiter = vends.begin(); viter != vcurrents.end(); ++viter, ++eiter) {
      ceph_assert(eiter != vends.end());
      *truncated = (*truncated || (viter->second != eiter->second));
    }
  }

  // Refresh marker, if there are multiple shards, the output will look like
  // '{shard_oid_1}#{shard_marker_1},{shard_oid_2}#{shard_marker_2}...',
  // if there is no sharding, the simply marker (without oid) is returned
  if (has_shards) {
    marker_mgr.to_string(&marker);
  } else {
    if (!result.empty()) {
      marker = result.rbegin()->id;
    }
  }

  return 0;
}

int RGWSI_BILog::get_log_status(const DoutPrefixProvider* dpp,
				const RGWBucketInfo& bucket_info,
				int shard_id,
				std::map<int, std::string>* markers,
				optional_yield y)
{
  std::vector<rgw_bucket_dir_header> headers;
  std::map<int, string> bucket_instance_ids;
  int r = svc.bi->cls_bucket_head(dpp, bucket_info, shard_id, &headers, &bucket_instance_ids, y);
  if (r < 0)
    return r;

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  auto viter = bucket_instance_ids.begin();

  for(; iter != headers.end(); ++iter, ++viter) {
    if (shard_id >= 0) {
      (*markers)[shard_id] = iter->max_marker;
    } else {
      (*markers)[viter->first] = iter->max_marker;
    }
  }

  return 0;
}

