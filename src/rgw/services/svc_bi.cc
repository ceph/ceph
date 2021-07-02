// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/format.h>

#include "svc_bi.h"
#include "svc_bilog.h"
#include "svc_zone.h"

#include "rgw/rgw_bucket.h"
#include "rgw/rgw_zone.h"
#include "rgw/rgw_datalog.h"

#include "cls/rgw/cls_rgw_client.h"

#define dout_subsys ceph_subsys_rgw

static constexpr auto dir_oid_prefix = ".dir."sv;

RGWSI_BucketIndex::RGWSI_BucketIndex(CephContext* cct)
  : RGWServiceInstance(cct) {}

void RGWSI_BucketIndex::init(librados::Rados* _rados,
			     RGWSI_Zone* zone_svc,
			     RGWSI_BILog* bilog_svc,
			     RGWDataChangesLog* datalog_svc)
{
  rados = _rados;
  svc.zone = zone_svc;
  svc.bilog = bilog_svc;
  svc.datalog = datalog_svc;
}

int RGWSI_BucketIndex::open_pool(const DoutPrefixProvider* dpp,
				 const rgw_pool& pool,
				 librados::IoCtx* index_pool,
				 bool mostly_omap)
{
  return rgw_init_ioctx(dpp, rados, pool, *index_pool,
			true, true);
}

int RGWSI_BucketIndex::open_bucket_index_pool(const DoutPrefixProvider* dpp,
					      const RGWBucketInfo& bucket_info,
					      librados::IoCtx* index_pool)
{
  const rgw_pool& explicit_pool =
    bucket_info.bucket.explicit_placement.index_pool;

  if (!explicit_pool.empty()) {
    return open_pool(dpp, explicit_pool, index_pool, false);
  }

  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();

  const rgw_placement_rule* rule = &bucket_info.placement_rule;
  if (rule->empty()) {
    rule = &zonegroup.default_placement;
  }
  auto iter = zone_params.placement_pools.find(rule->name);
  if (iter == zone_params.placement_pools.end()) {
    ldpp_dout(dpp, 0) << "could not find placement rule " << *rule
		      << " within zonegroup " << dendl;
    return -EINVAL;
  }

  int r = open_pool(dpp, iter->second.index_pool, index_pool, true);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_BucketIndex::open_bucket_index_base(const DoutPrefixProvider* dpp,
					      const RGWBucketInfo& bucket_info,
					      librados::IoCtx* index_pool,
					      std::string* bucket_oid_base)
{
  const auto& bucket = bucket_info.bucket;
  int r = open_bucket_index_pool(dpp, bucket_info, index_pool);
  if (r < 0)
    return r;

  if (bucket.bucket_id.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: empty bucket_id for bucket operation" << dendl;
    return -EIO;
  }

  *bucket_oid_base = dir_oid_prefix;
  bucket_oid_base->append(bucket.bucket_id);

  return 0;

}

int RGWSI_BucketIndex::open_bucket_index(const DoutPrefixProvider* dpp,
					 const RGWBucketInfo& bucket_info,
					 librados::IoCtx* index_pool,
					 std::string* bucket_oid)
{
  const auto& bucket = bucket_info.bucket;
  int r = open_bucket_index_pool(dpp, bucket_info, index_pool);
  if (r < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
		       << r << dendl;
    return r;
  }

  if (bucket.bucket_id.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: empty bucket id for bucket operation" << dendl;
    return -EIO;
  }

  *bucket_oid = dir_oid_prefix;
  bucket_oid->append(bucket.bucket_id);

  return 0;
}

static void get_bucket_index_objects(std::string_view bucket_oid_base,
			      uint32_t num_shards,
			      std::map<int, std::string>* _bucket_objects,
			      int shard_id = -1)
{
  auto& bucket_objects = *_bucket_objects;
  if (!num_shards) {
    bucket_objects[0] = bucket_oid_base;
  } else {
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        bucket_objects[i] = fmt::format("{}.{}", bucket_oid_base, i);
      }
    } else {
      if ((uint32_t)shard_id > num_shards) {
        return;
      }
      bucket_objects[shard_id] = fmt::format("{}.{}", bucket_oid_base,
					     shard_id);
    }
  }
}

static void get_bucket_instance_ids(const RGWBucketInfo& bucket_info,
				    int shard_id,
				    std::map<int, std::string>* result)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  std::string plain_id = fmt::format("{}:{}", bucket.name, bucket.bucket_id);

  if (!bucket_info.layout.current_index.layout.normal.num_shards) {
    (*result)[0] = plain_id;
  } else {
    if (shard_id < 0) {
      for (uint32_t i = 0;
	   i < bucket_info.layout.current_index.layout.normal.num_shards;
	   ++i) {
	(*result)[i] = fmt::format("{}:{}", plain_id, i);
      }
    } else {
      if ((uint32_t)shard_id >
	  bucket_info.layout.current_index.layout.normal.num_shards) {
	return;
      }
      (*result)[shard_id] = fmt::format("{}:{}", shard_id, shard_id);
    }
  }
}


int RGWSI_BucketIndex::open_bucket_index(
  const DoutPrefixProvider* dpp,
  const RGWBucketInfo& bucket_info,
  std::optional<int> _shard_id,
  librados::IoCtx* index_pool,
  std::map<int, string>* bucket_objs,
  std::map<int, string>* bucket_instance_ids)
{
  int shard_id = _shard_id.value_or(-1);
  std::string bucket_oid_base;
  int ret = open_bucket_index_base(dpp, bucket_info, index_pool,
				   &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
		       << ret << dendl;
    return ret;
  }

  get_bucket_index_objects(bucket_oid_base,
			   bucket_info.layout.current_index.layout.normal
			   .num_shards,
			   bucket_objs, shard_id);

  if (bucket_instance_ids) {
    get_bucket_instance_ids(bucket_info, shard_id, bucket_instance_ids);
  }
  return 0;
}

void RGWSI_BucketIndex::get_bucket_index_object(
  std::string_view  bucket_oid_base,
  uint32_t num_shards,
  int shard_id,
  uint64_t gen_id,
  std::string* bucket_obj)
{
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
  } else {
    if (gen_id != 0) {
      *bucket_obj = fmt::format("{}.{}.{}", bucket_oid_base,
				gen_id, shard_id);
    } else {
      // for backward compatibility, gen_id(0) will not be added in
      // the object name
      *bucket_obj = fmt::format("{}.{}", bucket_oid_base, shard_id);
    }
  }
}

int RGWSI_BucketIndex::get_bucket_index_object(std::string_view bucket_oid_base,
					       std::string_view obj_key,
					       uint32_t num_shards,
					       rgw::BucketHashType hash_type,
					       std::string* bucket_obj,
					       int* shard_id)
{
  int r = 0;
  switch (hash_type) {
    case rgw::BucketHashType::Mod:
      if (!num_shards) {
        // By default with no sharding, we use the bucket oid as itself
        (*bucket_obj) = bucket_oid_base;
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = bucket_shard_index(obj_key, num_shards);
        (*bucket_obj) = fmt::format("{}.{}", bucket_oid_base, sid);
        if (shard_id) {
          *shard_id = (int)sid;
        }
      }
      break;
    default:
      r = -ENOTSUP;
  }
  return r;
}

int RGWSI_BucketIndex::open_bucket_index_shard(const DoutPrefixProvider* dpp,
					       const RGWBucketInfo& bucket_info,
					       std::string_view obj_key,
					       librados::IoCtx* pool,
					       std::string* bucket_obj,
					       int* shard_id)
{
  std::string bucket_oid_base;

  int ret = open_bucket_index_base(dpp, bucket_info, pool, &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
                   << ret << dendl;
    return ret;
  }

  ret = get_bucket_index_object(
    bucket_oid_base, obj_key,
    bucket_info.layout.current_index.layout.normal.num_shards,
    bucket_info.layout.current_index.layout.normal.hash_type,
    bucket_obj, shard_id);

  if (ret < 0) {
    ldpp_dout(dpp, 10) << "get_bucket_index_object() returned ret=" << ret
		       << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_BucketIndex::open_bucket_index_shard(
  const DoutPrefixProvider* dpp,
  const RGWBucketInfo& bucket_info,
  int shard_id,
  const rgw::bucket_index_layout_generation& idx_layout,
  librados::IoCtx* index_pool,
  std::string* bucket_obj)
{
  std::string bucket_oid_base;
  int ret = open_bucket_index_base(dpp, bucket_info, index_pool, &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
                   << ret << dendl;
    return ret;
  }

  get_bucket_index_object(bucket_oid_base, idx_layout.layout.normal.num_shards,
                          shard_id, idx_layout.gen, bucket_obj);

  return 0;
}

int RGWSI_BucketIndex::cls_bucket_head(
  const DoutPrefixProvider* dpp,
  const RGWBucketInfo& bucket_info,
  int shard_id,
  std::vector<rgw_bucket_dir_header>* headers,
  std::map<int, std::string>* bucket_instance_ids,
  optional_yield y)
{
  librados::IoCtx index_pool;
  std::map<int, std::string> oids;
  int r = open_bucket_index(dpp, bucket_info, shard_id, &index_pool, &oids,
			    bucket_instance_ids);
  if (r < 0)
    return r;

  std::map<int, struct rgw_cls_list_ret> list_results;
  for (auto& iter : oids) {
    list_results.emplace(iter.first, rgw_cls_list_ret());
  }

  r = CLSRGWIssueGetDirHeader(index_pool, oids, list_results,
			      cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  for (auto iter = list_results.begin(); iter != list_results.end(); ++iter) {
    headers->push_back(std::move(iter->second.dir.header));
  }

  return 0;
}


int RGWSI_BucketIndex::init_index(const DoutPrefixProvider* dpp,
				  RGWBucketInfo& bucket_info)
{
  librados::IoCtx index_pool;

  auto dir_oid = std::string(dir_oid_prefix);
  int r = open_bucket_index_pool(dpp, bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  std::map<int, std::string> bucket_objs;
  get_bucket_index_objects(dir_oid,
			   bucket_info.layout.current_index.layout.normal
			   .num_shards,
			   &bucket_objs);

  return CLSRGWIssueBucketIndexInit(index_pool,
				    bucket_objs,
				    cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex::clean_index(const DoutPrefixProvider* dpp,
				   RGWBucketInfo& bucket_info)
{
  librados::IoCtx index_pool;

  auto dir_oid = std::string(dir_oid_prefix);
  int r = open_bucket_index_pool(dpp, bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  std::map<int, std::string> bucket_objs;
  get_bucket_index_objects(dir_oid,
			   bucket_info.layout.current_index.layout.normal
			   .num_shards,
			   &bucket_objs);

  return CLSRGWIssueBucketIndexClean(index_pool,
				     bucket_objs,
				     cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex::read_stats(const DoutPrefixProvider* dpp,
				  const RGWBucketInfo& bucket_info,
				  RGWBucketEnt* result,
				  optional_yield y)
{
  std::vector<rgw_bucket_dir_header> headers;

  result->bucket = bucket_info.bucket;
  int r = cls_bucket_head(dpp, bucket_info, RGW_NO_SHARD, &headers, nullptr, y);
  if (r < 0) {
    return r;
  }

  for (auto hiter = headers.begin(); hiter != headers.end(); ++hiter) {
    RGWObjCategory category = RGWObjCategory::Main;
    auto iter = (hiter->stats).find(category);
    if (iter != hiter->stats.end()) {
      struct rgw_bucket_category_stats& stats = iter->second;
      result->count += stats.num_entries;
      result->size += stats.total_size;
      result->size_rounded += stats.total_size_rounded;
    }
  }

  result->placement_rule = std::move(bucket_info.placement_rule);

  return 0;
}

int RGWSI_BucketIndex::get_reshard_status(
  const DoutPrefixProvider* dpp,
  const RGWBucketInfo& bucket_info,
  std::vector<cls_rgw_bucket_instance_entry>* status)
{
  std::map<int, std::string> bucket_objs;

  librados::IoCtx index_pool;

  int r = open_bucket_index(dpp, bucket_info,
			    std::nullopt,
			    &index_pool,
                            &bucket_objs,
                            nullptr);
  if (r < 0) {
    return r;
  }

  for (auto i : bucket_objs) {
    cls_rgw_bucket_instance_entry entry;

    int ret = cls_rgw_get_bucket_resharding(index_pool, i.second, &entry);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: " << __func__
			 << ": cls_rgw_get_bucket_resharding() returned ret="
			 << ret << dendl;
      return ret;
    }

    status->push_back(entry);
  }

  return 0;
}

int RGWSI_BucketIndex::handle_overwrite(const DoutPrefixProvider* dpp,
					const RGWBucketInfo& info,
					const RGWBucketInfo& orig_info)
{
  bool new_sync_enabled = info.datasync_flag_enabled();
  bool old_sync_enabled = orig_info.datasync_flag_enabled();

  if (old_sync_enabled != new_sync_enabled) {
    int shards_num = info.layout.current_index.layout.normal.num_shards ?
      info.layout.current_index.layout.normal.num_shards : 1;
    int shard_id = info.layout.current_index.layout.normal.num_shards? 0 : -1;

    int ret;
    if (!new_sync_enabled) {
      ret = svc.bilog->log_stop(dpp, info, -1);
    } else {
      ret = svc.bilog->log_start(dpp, info, -1);
    }
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed writing bilog (bucket="
			 << info.bucket << "); ret=" << ret << dendl;
      return ret;
    }

    for (int i = 0; i < shards_num; ++i, ++shard_id) {
      ret = svc.datalog->add_entry(dpp, info, shard_id);
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR: failed writing data log (info.bucket="
			   << info.bucket << ", shard_id=" << shard_id << ")"
			   << dendl;
        return ret;
      }
    }
  }

  return 0;
}
