
#include "svc_bi_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

#include "cls/rgw/cls_rgw_client.h"

#define dout_subsys ceph_subsys_rgw

static string dir_oid_prefix = ".dir.";

RGWSI_BucketIndex_RADOS::RGWSI_BucketIndex_RADOS(CephContext *cct) : RGWSI_BucketIndex(cct)
{
}

void RGWSI_BucketIndex_RADOS::init(RGWSI_Zone *zone_svc,
                                   RGWSI_RADOS *rados_svc)
{
  svc.zone = zone_svc;
  svc.rados = rados_svc;
}

int RGWSI_BucketIndex_RADOS::open_pool(const rgw_pool& pool,
                                       RGWSI_RADOS::Pool *index_pool)
{
  *index_pool = svc.rados->pool(pool);
  return index_pool->open();
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_pool(const RGWBucketInfo& bucket_info,
                                                    RGWSI_RADOS::Pool *index_pool)
{
  const rgw_pool& explicit_pool = bucket_info.bucket.explicit_placement.index_pool;

  if (!explicit_pool.empty()) {
    return open_pool(explicit_pool, index_pool);
  }

  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();

  const rgw_placement_rule *rule = &bucket_info.placement_rule;
  if (rule->empty()) {
    rule = &zonegroup.default_placement;
  }
  auto iter = zone_params.placement_pools.find(rule->name);
  if (iter == zone_params.placement_pools.end()) {
    ldout(cct, 0) << "could not find placement rule " << *rule << " within zonegroup " << dendl;
    return -EINVAL;
  }

  int r = open_pool(iter->second.index_pool, index_pool);
  if (r < 0)
    return r;

  return 0;
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_base(const RGWBucketInfo& bucket_info,
                                                    RGWSI_RADOS::Pool *index_pool,
                                                    string *bucket_oid_base)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  int r = open_bucket_index_pool(bucket_info, index_pool);
  if (r < 0)
    return r;

  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket_id for bucket operation" << dendl;
    return -EIO;
  }

  *bucket_oid_base = dir_oid_prefix;
  bucket_oid_base->append(bucket.bucket_id);

  return 0;

}

int RGWSI_BucketIndex_RADOS::open_bucket_index(const RGWBucketInfo& bucket_info,
                                               RGWSI_RADOS::Pool *index_pool,
                                               string *bucket_oid)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  int r = open_bucket_index_pool(bucket_info, index_pool);
  if (r < 0)
    return r;

  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket id for bucket operation" << dendl;
    return -EIO;
  }

  *bucket_oid = dir_oid_prefix;
  bucket_oid->append(bucket.bucket_id);

  return 0;
}

static void get_bucket_index_objects(const string& bucket_oid_base,
                                     uint32_t num_shards,
                                     map<int, string> *_bucket_objects,
                                     int shard_id = -1)
{
  auto& bucket_objects = *_bucket_objects;
  if (!num_shards) {
    bucket_objects[0] = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), i);
        bucket_objects[i] = buf;
      }
    } else {
      if ((uint32_t)shard_id > num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
      bucket_objects[shard_id] = buf;
    }
  }
}

static void get_bucket_instance_ids(const RGWBucketInfo& bucket_info,
                                    int shard_id,
                                    map<int, string> *result)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  string plain_id = bucket.name + ":" + bucket.bucket_id;
  if (!bucket_info.num_shards) {
    (*result)[0] = plain_id;
  } else {
    char buf[16];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < bucket_info.num_shards; ++i) {
        snprintf(buf, sizeof(buf), ":%d", i);
        (*result)[i] = plain_id + buf;
      }
    } else {
      if ((uint32_t)shard_id > bucket_info.num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      (*result)[shard_id] = plain_id + buf;
    }
  }
}

int RGWSI_BucketIndex_RADOS::open_bucket_index(const RGWBucketInfo& bucket_info,
                                               std::optional<int> _shard_id,
                                               RGWSI_RADOS::Pool *index_pool,
                                               map<int, string> *bucket_objs,
                                               map<int, string> *bucket_instance_ids)
{
  int shard_id = _shard_id.value_or(-1);
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_pool, &bucket_oid_base);
  if (ret < 0) {
    return ret;
  }

  get_bucket_index_objects(bucket_oid_base, bucket_info.num_shards, bucket_objs, shard_id);
  if (bucket_instance_ids) {
    get_bucket_instance_ids(bucket_info, shard_id, bucket_instance_ids);
  }
  return 0;
}

void RGWSI_BucketIndex_RADOS::get_bucket_index_object(const string& bucket_oid_base,
                                                      uint32_t num_shards,
                                                      int shard_id,
                                                      string *bucket_obj)
{
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    (*bucket_obj) = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
    (*bucket_obj) = buf;
  }
}

int RGWSI_BucketIndex_RADOS::get_bucket_index_object(const string& bucket_oid_base, const string& obj_key,
                                                     uint32_t num_shards, RGWBucketInfo::BIShardsHashType hash_type,
                                                     string *bucket_obj, int *shard_id)
{
  int r = 0;
  switch (hash_type) {
    case RGWBucketInfo::MOD:
      if (!num_shards) {
        // By default with no sharding, we use the bucket oid as itself
        (*bucket_obj) = bucket_oid_base;
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = rgw_bucket_shard_index(obj_key, num_shards);
        char buf[bucket_oid_base.size() + 32];
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), sid);
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

int RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                                                     const string& obj_key,
                                                     RGWSI_RADOS::Pool *index_pool,
                                                     string *bucket_obj,
                                                     int *shard_id)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_pool, &bucket_oid_base);
  if (ret < 0)
    return ret;

  ret = get_bucket_index_object(bucket_oid_base, obj_key, bucket_info.num_shards,
        (RGWBucketInfo::BIShardsHashType)bucket_info.bucket_index_shard_hash_type, bucket_obj, shard_id);
  if (ret < 0) {
    ldout(cct, 10) << "get_bucket_index_object() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

int RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                                                     int shard_id,
                                                     RGWSI_RADOS::Pool *index_pool,
                                                     string *bucket_obj)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket_info, index_pool, &bucket_oid_base);
  if (ret < 0)
    return ret;

  get_bucket_index_object(bucket_oid_base, bucket_info.num_shards,
                          shard_id, bucket_obj);
  return 0;
}

int RGWSI_BucketIndex_RADOS::cls_bucket_head(const RGWBucketInfo& bucket_info,
                                             int shard_id,
                                             vector<rgw_bucket_dir_header> *headers,
                                             map<int, string> *bucket_instance_ids)
{
  RGWSI_RADOS::Pool index_pool;
  map<int, string> oids;
  int r = open_bucket_index(bucket_info, shard_id, &index_pool, &oids, bucket_instance_ids);
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret> list_results;
  for (auto& iter : oids) {
    list_results.emplace(iter.first, rgw_cls_list_ret());
  }

  r = CLSRGWIssueGetDirHeader(index_pool.ioctx(), oids, list_results, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  for(; iter != list_results.end(); ++iter) {
    headers->push_back(std::move(iter->second.dir.header));
  }
  return 0;
}


int RGWSI_BucketIndex_RADOS::init_index(RGWBucketInfo& bucket_info)
{
  RGWSI_RADOS::Pool index_pool;

  string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_pool(bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  map<int, string> bucket_objs;
  get_bucket_index_objects(dir_oid, bucket_info.num_shards, &bucket_objs);

  return CLSRGWIssueBucketIndexInit(index_pool.ioctx(),
				    bucket_objs,
				    cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex_RADOS::clean_index(RGWBucketInfo& bucket_info)
{
  RGWSI_RADOS::Pool index_pool;

  std::string dir_oid = dir_oid_prefix;
  int r = open_bucket_index_pool(bucket_info, &index_pool);
  if (r < 0) {
    return r;
  }

  dir_oid.append(bucket_info.bucket.bucket_id);

  std::map<int, std::string> bucket_objs;
  get_bucket_index_objects(dir_oid, bucket_info.num_shards, &bucket_objs);

  return CLSRGWIssueBucketIndexClean(index_pool.ioctx(),
				     bucket_objs,
				     cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWSI_BucketIndex_RADOS::read_stats(const RGWBucketInfo& bucket_info,
                                        RGWBucketEnt *result)
{
  vector<rgw_bucket_dir_header> headers;

  result->bucket = bucket_info.bucket;
  int r = cls_bucket_head(bucket_info, RGW_NO_SHARD, &headers, nullptr);
  if (r < 0) {
    return r;
  }

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

