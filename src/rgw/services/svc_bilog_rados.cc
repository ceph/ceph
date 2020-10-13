// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_bilog_rados.h"
#include "svc_bi_rados.h"

#include "common/errno.h"
#include "cls/rgw/cls_rgw_client.h"



#define dout_subsys ceph_subsys_rgw

RGWSI_BILog_RADOS::RGWSI_BILog_RADOS(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_BILog_RADOS_InIndex::init(RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.bi = bi_rados_svc;
}

int RGWSI_BILog_RADOS_InIndex::log_trim(const RGWBucketInfo& bucket_info,
                                        const int shard_id,
                                        string& end_marker)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;

  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  // empty string nowadays. In the past we supported `start_marker` but
  // it has to die because of cls_fifo. See also ae5660fbb6.
  r = start_marker_mgr.from_string(std::string{}, shard_id);
  if (r < 0) {
    return r;
  }

  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  return CLSRGWIssueBILogTrim(index_pool.ioctx(), start_marker_mgr, end_marker_mgr, bucket_objs,
			      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog_RADOS_InIndex::log_start(const RGWBucketInfo& bucket_info, int shard_id)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueResyncBucketBILog(index_pool.ioctx(), bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BILog_RADOS_InIndex::log_stop(const RGWBucketInfo& bucket_info, int shard_id)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> bucket_objs;
  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  return CLSRGWIssueBucketBILogStop(index_pool.ioctx(), bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

static void build_bucket_index_marker(const string& shard_id_str,
                                      const string& shard_marker,
                                      string *marker) {
  if (marker) {
    *marker = shard_id_str;
    marker->append(BucketIndexShardsManager::KEY_VALUE_SEPARATOR);
    marker->append(shard_marker);
  }
}

int RGWSI_BILog_RADOS_InIndex::log_list(const RGWBucketInfo& bucket_info, int shard_id, string& marker, uint32_t max,
                                std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  ldout(cct, 20) << __func__ << ": " << bucket_info.bucket << " marker " << marker << " shard_id=" << shard_id << " max " << max << dendl;
  result.clear();

  RGWSI_RADOS::Pool index_pool;
  map<int, string> oids;
  map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  int r = svc.bi->open_bucket_index(bucket_info, shard_id, &index_pool, &oids, nullptr);
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
 
  r = CLSRGWIssueBILogList(index_pool.ioctx(), marker_mgr, max, oids, bi_log_lists, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  map<int, cls_rgw_bi_log_list_ret>::iterator miter = bi_log_lists.begin();
  for (; miter != bi_log_lists.end(); ++miter) {
    int shard_id = miter->first;
    vcurrents[shard_id] = miter->second.entries.begin();
    vends[shard_id] = miter->second.entries.end();
    if (truncated) {
      *truncated = (*truncated || miter->second.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator viter;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator eiter;
  while (total < max && has_more) {
    has_more = false;

    viter = vcurrents.begin();
    eiter = vends.begin();

    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());

      int shard_id = viter->first;
      list<rgw_bi_log_entry>::iterator& liter = viter->second;

      if (liter == eiter->second){
        continue;
      }
      rgw_bi_log_entry& entry = *(liter);
      if (has_shards) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", shard_id);
        string tmp_id;
        build_bucket_index_marker(buf, entry.id, &tmp_id);
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
      assert (eiter != vends.end());
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

int RGWSI_BILog_RADOS_InIndex::get_log_status(const RGWBucketInfo& bucket_info,
                                              int shard_id,
                                              map<int, string> *markers)
{
  vector<rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  int r = svc.bi->cls_bucket_head(bucket_info, shard_id, &headers, &bucket_instance_ids, null_yield);
  if (r < 0)
    return r;

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  map<int, string>::iterator viter = bucket_instance_ids.begin();

  for(; iter != headers.end(); ++iter, ++viter) {
    if (shard_id >= 0) {
      (*markers)[shard_id] = iter->max_marker;
    } else {
      (*markers)[viter->first] = iter->max_marker;
    }
  }

  return 0;
}

// CLS FIFO
void RGWSI_BILog_RADOS_FIFO::init(RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.bi = bi_rados_svc;
}

int RGWSI_BILog_RADOS_FIFO::log_trim(const RGWBucketInfo& bucket_info,
                                     int shard_id,
                                     string& marker)
{
  if (shard_id > 0) {
    // the initial implementation does support single shard only.
    // this is supposed to change in the future. It's worth to
    // point out the plan is to decouple the BILog's sharding
    // policy from the BI's one.
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);

  auto fifo = _open_fifo(bucket_info);
  if (!fifo) {
    lderr(cct) << __PRETTY_FUNCTION__
               << ": unable to open FIFO: " << ""//get_oid(index)
               << dendl;
    return -EIO;
  }
  if (int ret = fifo->trim(marker, false, null_yield); ret < 0) {
    lderr(cct) << __PRETTY_FUNCTION__
               << ": unable to trim FIFO: " << ""//get_oid(index)
               << ": " << cpp_strerror(-ret) << dendl;
  }
  return 0;
}

int RGWSI_BILog_RADOS_FIFO::log_start(const RGWBucketInfo& bucket_info,
                                      int shard_id)
{
  if (shard_id > 0) {
    // the initial implementation does support single shard only.
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  return 0;
}

int RGWSI_BILog_RADOS_FIFO::log_stop(const RGWBucketInfo& bucket_info,
                                     int shard_id)
{
  if (shard_id > 0) {
    // the initial implementation does support single shard only.
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  return 0;
}

std::unique_ptr<rgw::cls::fifo::FIFO>
RGWSI_BILog_RADOS_FIFO::_open_fifo(const RGWBucketInfo& bucket_info)
{
  RGWSI_RADOS::Pool index_pool;
  std::string bucket_oid;
  ceph_assert(svc.bi);
  if (const int ret = svc.bi->open_bucket_index(bucket_info,
                                                &index_pool,
                                                &bucket_oid);
      ret < 0) {
    return nullptr;
  }

  static constexpr char BILOG_FIFO_SUFFIX[] = ".bilog_fifo";
  std::unique_ptr<rgw::cls::fifo::FIFO> ret_fifo;
  rgw::cls::fifo::FIFO::create(index_pool.ioctx(),
                               bucket_oid + BILOG_FIFO_SUFFIX,
                               &ret_fifo,
                               null_yield /* FIXME */);
  return ret_fifo;
}

int RGWSI_BILog_RADOS_FIFO::log_list(const RGWBucketInfo& bucket_info,
                                     int shard_id,
                                     string& _marker /* in/out? */,
                                     const uint32_t max_entries,
                                     std::list<rgw_bi_log_entry>& result,
                                     bool *truncated)
{
  ldout(cct, 20) << __func__ << ": " << bucket_info.bucket
                 << " _marker=" << _marker << " shard_id=" << shard_id
                 << " max_entries=" << max_entries << dendl;
  if (shard_id > 0) {
    // the initial implementation does support single shard only.
    return 0;
  } else {
    ceph_assert(shard_id == 0 || shard_id == -1);
  }

  auto fifo = _open_fifo(bucket_info);
  if (!fifo) {
    lderr(cct) << __PRETTY_FUNCTION__
               << ": unable to open FIFO: " << ""//get_oid(index)
               << dendl;
    return -EIO;
  }

  std::optional<std::string_view> marker;
  if (!_marker.empty()) {
    marker.emplace(std::move(_marker));
  }

  std::vector<rgw::cls::fifo::list_entry> raw_entries;
  bool more = false;
  auto r = fifo->list(max_entries, marker, &raw_entries, &more,
                              null_yield);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__
               << ": unable to list FIFO: " << ""//get_oid(index)
               << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  for (const auto& raw_entry : raw_entries) {
    result.emplace_back();
    auto& bilog_entry = result.back();

    auto liter = raw_entry.data.cbegin();
    try {
      decode(bilog_entry, liter);
    } catch (const buffer::error& err) {
      lderr(cct) << __PRETTY_FUNCTION__
                 << ": failed to decode data changes log entry: "
                 << err.what() << dendl;
      return -EIO;
    }
  }

  if (truncated) {
    *truncated = more;
  }
  if (!raw_entries.empty()) {
    _marker = raw_entries.back().marker;
  }
  return 0;
}

int RGWSI_BILog_RADOS_FIFO::get_log_status(const RGWBucketInfo& bucket_info,
                                           int shard_id,
                                           map<int, string> *markers)
{
  if (shard_id > 1) {
    // the initial implementation does support single shard only.
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  return 0;
}
