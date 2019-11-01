#include "svc_bucket_sync_sobj.h"
#include "svc_zone.h"
#include "svc_sys_obj_cache.h"
#include "svc_bucket_sobj.h"

#include "rgw/rgw_bucket_sync.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_Bucket_Sync_SObj::~RGWSI_Bucket_Sync_SObj() {
}

void RGWSI_Bucket_Sync_SObj::init(RGWSI_Zone *_zone_svc,
                                  RGWSI_SysObj_Cache *_cache_svc,
                                  RGWSI_Bucket_SObj *bucket_sobj_svc)
{
  svc.zone = _zone_svc;
  svc.cache = _cache_svc;
  svc.bucket_sobj = bucket_sobj_svc;
}

int RGWSI_Bucket_Sync_SObj::do_start()
{
  sync_policy_cache.reset(new RGWChainedCacheImpl<bucket_sync_policy_cache_entry>);
  sync_policy_cache->init(svc.cache);

  return 0;
}

int RGWSI_Bucket_Sync_SObj::get_policy_handler(RGWSI_Bucket_BI_Ctx& ctx,
                                               std::optional<string> zone,
                                               std::optional<rgw_bucket> _bucket,
                                               RGWBucketSyncPolicyHandlerRef *handler,
                                               optional_yield y)
{
  if (!_bucket) {
    *handler = svc.zone->get_sync_policy_handler(zone);
    return 0;
  }

  auto& bucket = *_bucket;

  string zone_key;
  string bucket_key;

  if (zone && *zone != svc.zone->zone_id()) {
    zone_key = *zone;
  }

  bucket_key = RGWSI_Bucket::get_bi_meta_key(bucket);

  string cache_key("bi/" + zone_key + "/" + bucket_key);

  if (auto e = sync_policy_cache->find(cache_key)) {
    *handler = e->handler;
    return 0;
  }

  bucket_sync_policy_cache_entry e;
  rgw_cache_entry_info cache_info;

  RGWBucketInfo bucket_info;

  int r = svc.bucket_sobj->read_bucket_instance_info(ctx,
                                                     bucket_key,
                                                     &bucket_info,
                                                     nullptr,
                                                     nullptr,
                                                     y,
                                                     &cache_info);
  if (r < 0) {
    if (r != -ENOENT) {
      ldout(cct, 0) << "ERROR: svc.bucket->read_bucket_instance_info(key=" << bucket_key << ") returned r=" << r << dendl;
    }
    return r;
  }

  e.handler.reset(svc.zone->get_sync_policy_handler(zone)->alloc_child(bucket_info));

  if (!sync_policy_cache->put(svc.cache, cache_key, &e, {&cache_info})) {
    ldout(cct, 20) << "couldn't put bucket_sync_policy cache entry, might have raced with data changes" << dendl;
  }

  *handler = e.handler;

  return 0;
}

