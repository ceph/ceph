// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_bi_rados.h"
#include "svc_bilog_rados.h"
#include "svc_zone.h"

#include "rgw_bucket.h"
#include "rgw_zone.h"
#include "rgw_datalog.h"

#include "cls/rgw/cls_rgw_client.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static string dir_oid_prefix = ".dir.";

RGWSI_BucketIndex_RADOS::RGWSI_BucketIndex_RADOS(CephContext *cct) : RGWSI_BucketIndex(cct)
{
}

void RGWSI_BucketIndex_RADOS::init(RGWSI_Zone *zone_svc,
				   librados::Rados* rados_,
				   RGWSI_BILog_RADOS *bilog_svc,
				   RGWDataChangesLog *datalog_rados_svc)
{
  svc.zone = zone_svc;
  rados = rados_;
  svc.bilog = bilog_svc;
  svc.datalog_rados = datalog_rados_svc;
}

int RGWSI_BucketIndex_RADOS::open_pool(const DoutPrefixProvider *dpp,
                                       const rgw_pool& pool,
                                       librados::IoCtx* index_pool,
                                       bool mostly_omap)
{
  return rgw_init_ioctx(dpp, rados, pool, *index_pool, true, mostly_omap);
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_pool(const DoutPrefixProvider *dpp,
                                                    const RGWBucketInfo& bucket_info,
                                                    librados::IoCtx* index_pool)
{
  const rgw_pool& explicit_pool = bucket_info.bucket.explicit_placement.index_pool;

  if (!explicit_pool.empty()) {
    return open_pool(dpp, explicit_pool, index_pool, false);
  }

  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();

  const rgw_placement_rule *rule = &bucket_info.placement_rule;
  if (rule->empty()) {
    rule = &zonegroup.default_placement;
  }
  auto iter = zone_params.placement_pools.find(rule->name);
  if (iter == zone_params.placement_pools.end()) {
    ldpp_dout(dpp, 0) << "could not find placement rule " << *rule << " within zonegroup " << dendl;
    return -EINVAL;
  }

  int r = open_pool(dpp, iter->second.index_pool, index_pool, true);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_base(const DoutPrefixProvider *dpp,
                                                    const RGWBucketInfo& bucket_info,
                                                    librados::IoCtx* index_pool,
                                                    string *bucket_oid_base)
{
  const rgw_bucket& bucket = bucket_info.bucket;
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

int RGWSI_BucketIndex_RADOS::open_bucket_index(const DoutPrefixProvider *dpp,
                                               const RGWBucketInfo& bucket_info,
                                               librados::IoCtx* index_pool,
                                               string *bucket_oid)
{
  const rgw_bucket& bucket = bucket_info.bucket;
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

static char bucket_obj_with_generation(char *buf, size_t len, const string& bucket_oid_base, uint64_t gen_id,
                                    uint32_t shard_id)
{
  return snprintf(buf, len, "%s.%" PRIu64 ".%d", bucket_oid_base.c_str(), gen_id, shard_id);
}

static char bucket_obj_without_generation(char *buf, size_t len, const string& bucket_oid_base, uint32_t shard_id)
{
  return snprintf(buf, len, "%s.%d", bucket_oid_base.c_str(), shard_id);
}

static void get_bucket_index_objects(const string& bucket_oid_base,
                                     uint32_t num_shards, uint64_t gen_id,
                                     map<int, string> *_bucket_objects,
                                     int shard_id = -1)
{
  auto& bucket_objects = *_bucket_objects;
  if (!num_shards) {
    bucket_objects[0] = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 64];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        if (gen_id) {
          bucket_obj_with_generation(buf, sizeof(buf), bucket_oid_base, gen_id, i);
        } else {
          bucket_obj_without_generation(buf, sizeof(buf), bucket_oid_base, i);
        }
        bucket_objects[i] = buf;
      }
    } else {
      if (std::cmp_greater(shard_id, num_shards)) {
        return;
      } else {
        if (gen_id) {
          bucket_obj_with_generation(buf, sizeof(buf), bucket_oid_base, gen_id, shard_id);
        } else {
          // for backward compatibility, gen_id(0) will not be added in the object name
          bucket_obj_without_generation(buf, sizeof(buf), bucket_oid_base, shard_id);
        }
        bucket_objects[shard_id] = buf;
      }
    }
  }
}

static void get_bucket_instance_ids(const RGWBucketInfo& bucket_info,
                                    int num_shards, int shard_id,
                                    map<int, string> *result)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  string plain_id = bucket.name + ":" + bucket.bucket_id;

  if (!num_shards) {
    (*result)[0] = plain_id;
  } else {
    char buf[16];
    if (shard_id < 0) {
      for (int i = 0; i < num_shards; ++i) {
        snprintf(buf, sizeof(buf), ":%d", i);
        (*result)[i] = plain_id + buf;
      }
    } else {
      if (shard_id > num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      (*result)[shard_id] = plain_id + buf;
    }
  }
}

int RGWSI_BucketIndex_RADOS::open_bucket_index(const DoutPrefixProvider *dpp,
                                               const RGWBucketInfo& bucket_info,
                                               std::optional<int> _shard_id,
                                               const rgw::bucket_index_layout_generation& idx_layout,
                                               librados::IoCtx* index_pool,
                                               map<int, string> *bucket_objs,
                                               map<int, string> *bucket_instance_ids)
{
  int shard_id = _shard_id.value_or(-1);
  string bucket_oid_base;
  int ret = open_bucket_index_base(dpp, bucket_info, index_pool, &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
                   << ret << dendl;
    return ret;
  }

  get_bucket_index_objects(bucket_oid_base, idx_layout.layout.normal.num_shards,
                           idx_layout.gen, bucket_objs, shard_id);
  if (bucket_instance_ids) {
    get_bucket_instance_ids(bucket_info, idx_layout.layout.normal.num_shards,
                            shard_id, bucket_instance_ids);
  }
  return 0;
}

void RGWSI_BucketIndex_RADOS::get_bucket_index_object(
    const std::string& bucket_oid_base,
    const rgw::bucket_index_normal_layout& normal,
    uint64_t gen_id, int shard_id,
    std::string* bucket_obj)
{
  if (!normal.num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 64];
    if (gen_id) {
      bucket_obj_with_generation(buf, sizeof(buf), bucket_oid_base, gen_id, shard_id);
      (*bucket_obj) = buf;
	  ldout(cct, 10) << "bucket_obj is " << (*bucket_obj) << dendl;
    } else {
      // for backward compatibility, gen_id(0) will not be added in the object name
      bucket_obj_without_generation(buf, sizeof(buf), bucket_oid_base, shard_id);
      (*bucket_obj) = buf;
    }
  }
}

int RGWSI_BucketIndex_RADOS::get_bucket_index_object(
    const std::string& bucket_oid_base,
    const rgw::bucket_index_normal_layout& normal,
    uint64_t gen_id, const std::string& obj_key,
    std::string* bucket_obj, int* shard_id)
{
  int r = 0;
  switch (normal.hash_type) {
    case rgw::BucketHashType::Mod:
      if (!normal.num_shards) {
        // By default with no sharding, we use the bucket oid as itself
        (*bucket_obj) = bucket_oid_base;
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = bucket_shard_index(obj_key, normal.num_shards);
        char buf[bucket_oid_base.size() + 64];
        if (gen_id) {
          bucket_obj_with_generation(buf, sizeof(buf), bucket_oid_base, gen_id, sid);
        } else {
          bucket_obj_without_generation(buf, sizeof(buf), bucket_oid_base, sid);
        }
        (*bucket_obj) = buf;
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

int RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const DoutPrefixProvider *dpp,
                                                     const RGWBucketInfo& bucket_info,
                                                     const string& obj_key,
                                                     rgw_rados_ref* bucket_obj,
                                                     int *shard_id)
{
  string bucket_oid_base;

  int ret = open_bucket_index_base(dpp, bucket_info, &bucket_obj->ioctx, &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
                   << ret << dendl;
    return ret;
  }

  const auto& current_index = bucket_info.layout.current_index;
  ret = get_bucket_index_object(bucket_oid_base, current_index.layout.normal,
                                current_index.gen, obj_key,
				&bucket_obj->obj.oid, shard_id);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "get_bucket_index_object() returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const DoutPrefixProvider *dpp,
                                                     const RGWBucketInfo& bucket_info,
                                                     const rgw::bucket_index_layout_generation& index,
                                                     int shard_id,
                                                     rgw_rados_ref* bucket_obj)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(dpp, bucket_info, &bucket_obj->ioctx,
				   &bucket_oid_base);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": open_bucket_index_pool() returned "
                   << ret << dendl;
    return ret;
  }

  get_bucket_index_object(bucket_oid_base, index.layout.normal,
                          index.gen, shard_id, &bucket_obj->obj.oid);

  return 0;
}

int RGWSI_BucketIndex_RADOS::cls_bucket_head(const DoutPrefixProvider *dpp,
                                             const RGWBucketInfo& bucket_info,
                                             const rgw::bucket_index_layout_generation& idx_layout,
                                             int shard_id,
                                             vector<rgw_bucket_dir_header> *headers,
                                             map<int, string> *bucket_instance_ids,
                                             optional_yield y)
{
  librados::IoCtx index_pool;
  map<int, string> oids;
  int r = open_bucket_index(dpp, bucket_info, shard_id, idx_layout, &index_pool, &oids, bucket_instance_ids);
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret> list_results;
  for (auto& iter : oids) {
    list_results.emplace(iter.first, rgw_cls_list_ret());
  }

  r = CLSRGWIssueGetDirHeader(index_pool, oids, list_results,
			      cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  for(; iter != list_results.end(); ++iter) {
    headers->push_back(std::move(iter->second.dir.header));
  }
  return 0;
}

int RGWSI_BucketIndex_RADOS::init_index(const DoutPrefixProvider *dpp,RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout)
{
  librados::IoCtx index_pool;

  string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_pool(dpp, bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  map<int, string> bucket_objs;
  get_bucket_index_objects(dir_oid, idx_layout.layout.normal.num_shards, idx_layout.gen, &bucket_objs);

  return CLSRGWIssueBucketIndexInit(index_pool,
				    bucket_objs,
				    cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex_RADOS::clean_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout)
{
  librados::IoCtx index_pool;

  std::string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_pool(dpp, bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  std::map<int, std::string> bucket_objs;
  get_bucket_index_objects(dir_oid, idx_layout.layout.normal.num_shards,
                           idx_layout.gen, &bucket_objs);

  return CLSRGWIssueBucketIndexClean(index_pool,
				     bucket_objs,
				     cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex_RADOS::read_stats(const DoutPrefixProvider *dpp,
                                        const RGWBucketInfo& bucket_info,
                                        RGWBucketEnt *result,
                                        optional_yield y)
{
  vector<rgw_bucket_dir_header> headers;

  result->bucket = bucket_info.bucket;
  int r = cls_bucket_head(dpp, bucket_info, bucket_info.layout.current_index, RGW_NO_SHARD, &headers, nullptr, y);
  if (r < 0) {
    return r;
  }

  result->count = 0; 
  result->size = 0; 
  result->size_rounded = 0; 

  auto hiter = headers.begin();
  for (; hiter != headers.end(); ++hiter) {
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

int RGWSI_BucketIndex_RADOS::get_reshard_status(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, list<cls_rgw_bucket_instance_entry> *status)
{
  map<int, string> bucket_objs;

  librados::IoCtx index_pool;

  int r = open_bucket_index(dpp, bucket_info,
                            std::nullopt,
                            bucket_info.layout.current_index,
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
      ldpp_dout(dpp, -1) << "ERROR: " << __func__ << ": cls_rgw_get_bucket_resharding() returned ret=" << ret << dendl;
      return ret;
    }

    status->push_back(entry);
  }

  return 0;
}

int RGWSI_BucketIndex_RADOS::handle_overwrite(const DoutPrefixProvider *dpp,
                                              const RGWBucketInfo& info,
                                              const RGWBucketInfo& orig_info,
					      optional_yield y)
{
  bool new_sync_enabled = info.datasync_flag_enabled();
  bool old_sync_enabled = orig_info.datasync_flag_enabled();

  if (old_sync_enabled == new_sync_enabled) {
    return 0; // datasync flag didn't change
  }
  if (info.layout.logs.empty()) {
    return 0; // no bilog
  }
  const auto& bilog = info.layout.logs.back();
  if (bilog.layout.type != rgw::BucketLogType::InIndex) {
    return -ENOTSUP;
  }
  const int shards_num = rgw::num_shards(bilog.layout.in_index);

  int ret;
  if (!new_sync_enabled) {
    ret = svc.bilog->log_stop(dpp, info, bilog, -1);
  } else {
    ret = svc.bilog->log_start(dpp, info, bilog, -1);
  }
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed writing bilog (bucket=" << info.bucket << "); ret=" << ret << dendl;
    return ret;
  }

  for (int i = 0; i < shards_num; ++i) {
    ret = svc.datalog_rados->add_entry(dpp, info, bilog, i, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed writing data log (info.bucket=" << info.bucket << ", shard_id=" << i << ")" << dendl;
    } // datalog error is not fatal
  }

  return 0;
}
