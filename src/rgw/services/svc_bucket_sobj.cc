// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_bucket_sobj.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_bi.h"
#include "svc_meta.h"
#include "svc_meta_be_sobj.h"
#include "svc_sync_modules.h"

#include "rgw_bucket.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

#define RGW_BUCKET_INSTANCE_MD_PREFIX ".bucket.meta."

using namespace std;

class RGWSI_Bucket_SObj_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Bucket_SObj::Svc& svc;

  const string prefix;
public:
  RGWSI_Bucket_SObj_Module(RGWSI_Bucket_SObj::Svc& _svc) : RGWSI_MBSObj_Handler_Module("bucket"),
                                                 svc(_svc) {}

  void get_pool_and_oid(const string& key, rgw_pool *pool, string *oid) override {
    if (pool) {
      *pool = svc.zone->get_zone_params().domain_root;
    }
    if (oid) {
      *oid = key;
    }
  }

  const string& get_oid_prefix() override {
    return prefix;
  }

  bool is_valid_oid(const string& oid) override {
    return (!oid.empty() && oid[0] != '.');
  }

  string key_to_oid(const string& key) override {
    return key;
  }

  string oid_to_key(const string& oid) override {
    /* should have been called after is_valid_oid(),
     * so no need to check for validity */
    return oid;
  }
};

class RGWSI_BucketInstance_SObj_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Bucket_SObj::Svc& svc;

  const string prefix;
public:
  RGWSI_BucketInstance_SObj_Module(RGWSI_Bucket_SObj::Svc& _svc) : RGWSI_MBSObj_Handler_Module("bucket.instance"),
                                                                     svc(_svc), prefix(RGW_BUCKET_INSTANCE_MD_PREFIX) {}

  void get_pool_and_oid(const string& key, rgw_pool *pool, string *oid) override {
    if (pool) {
      *pool = svc.zone->get_zone_params().domain_root;
    }
    if (oid) {
      *oid = key_to_oid(key);
    }
  }

  const string& get_oid_prefix() override {
    return prefix;
  }

  bool is_valid_oid(const string& oid) override {
    return (oid.compare(0, prefix.size(), RGW_BUCKET_INSTANCE_MD_PREFIX) == 0);
  }

// 'tenant/' is used in bucket instance keys for sync to avoid parsing ambiguity
// with the existing instance[:shard] format. once we parse the shard, the / is
// replaced with a : to match the [tenant:]instance format
  string key_to_oid(const string& key) override {
    string oid = prefix + key;

    // replace tenant/ with tenant:
    auto c = oid.find('/', prefix.size());
    if (c != string::npos) {
      oid[c] = ':';
    }

    return oid;
  }

  // convert bucket instance oids back to the tenant/ format for metadata keys.
  // it's safe to parse 'tenant:' only for oids, because they won't contain the
  // optional :shard at the end
  string oid_to_key(const string& oid) override {
    /* this should have been called after oid was checked for validity */

    if (oid.size() < prefix.size()) { /* just sanity check */
      return string();
    }

    string key = oid.substr(prefix.size());

    // find first : (could be tenant:bucket or bucket:instance)
    auto c = key.find(':');
    if (c != string::npos) {
      // if we find another :, the first one was for tenant
      if (key.find(':', c + 1) != string::npos) {
        key[c] = '/';
      }
    }

    return key;
  }

  /*
   * hash entry for mdlog placement. Use the same hash key we'd have for the bucket entry
   * point, so that the log entries end up at the same log shard, so that we process them
   * in order
   */
  string get_hash_key(const string& key) override {
    string k = "bucket:";
    int pos = key.find(':');
    if (pos < 0)
      k.append(key);
    else
      k.append(key.substr(0, pos));

    return k;
  }
};

RGWSI_Bucket_SObj::RGWSI_Bucket_SObj(CephContext *cct): RGWSI_Bucket(cct) {
}

RGWSI_Bucket_SObj::~RGWSI_Bucket_SObj() {
}

void RGWSI_Bucket_SObj::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                             RGWSI_SysObj_Cache *_cache_svc, RGWSI_BucketIndex *_bi,
                             RGWSI_Meta *_meta_svc, RGWSI_MetaBackend *_meta_be_svc,
                             RGWSI_SyncModules *_sync_modules_svc,
                             RGWSI_Bucket_Sync *_bucket_sync_svc)
{
  svc.bucket = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.bi = _bi;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sync_modules = _sync_modules_svc;
  svc.bucket_sync = _bucket_sync_svc;
}

int RGWSI_Bucket_SObj::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  binfo_cache.reset(new RGWChainedCacheImpl<bucket_info_cache_entry>);
  binfo_cache->init(svc.cache);

  /* create first backend handler for bucket entrypoints */

  RGWSI_MetaBackend_Handler *ep_handler;

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ, &ep_handler);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  ep_be_handler = ep_handler;

  RGWSI_MetaBackend_Handler_SObj *ep_bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(ep_handler);

  auto ep_module = new RGWSI_Bucket_SObj_Module(svc);
  ep_be_module.reset(ep_module);
  ep_bh->set_module(ep_module);

  /* create a second backend handler for bucket instance */

  RGWSI_MetaBackend_Handler *bi_handler;

  r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ, &bi_handler);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  bi_be_handler = bi_handler;

  RGWSI_MetaBackend_Handler_SObj *bi_bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(bi_handler);

  auto bi_module = new RGWSI_BucketInstance_SObj_Module(svc);
  bi_be_module.reset(bi_module);
  bi_bh->set_module(bi_module);

  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                                   const string& key,
                                                   RGWBucketEntryPoint *entry_point,
                                                   RGWObjVersionTracker *objv_tracker,
                                                   real_time *pmtime,
                                                   map<string, bufferlist> *pattrs,
                                                   optional_yield y,
                                                   const DoutPrefixProvider *dpp,
                                                   rgw_cache_entry_info *cache_info,
                                                   boost::optional<obj_version> refresh_version)
{
  bufferlist bl;

  auto params = RGWSI_MBSObj_GetParams(&bl, pattrs, pmtime).set_cache_info(cache_info)
                                                           .set_refresh_version(refresh_version);
                                                    
  int ret = svc.meta_be->get_entry(ctx.get(), key, params, objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*entry_point, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_Bucket_SObj::store_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                                    const string& key,
                                                    RGWBucketEntryPoint& info,
                                                    bool exclusive,
                                                    real_time mtime,
                                                    const map<string, bufferlist> *pattrs,
                                                    RGWObjVersionTracker *objv_tracker,
                                                    optional_yield y,
                                                    const DoutPrefixProvider *dpp)
{
  bufferlist bl;
  encode(info, bl);

  RGWSI_MBSObj_PutParams params(bl, pattrs, mtime, exclusive);

  int ret = svc.meta_be->put(ctx.get(), key, params, objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return ret;
}

int RGWSI_Bucket_SObj::remove_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                                     const string& key,
                                                     RGWObjVersionTracker *objv_tracker,
                                                     optional_yield y,
                                                     const DoutPrefixProvider *dpp)
{
  RGWSI_MBSObj_RemoveParams params;
  return svc.meta_be->remove(ctx.get(), key, params, objv_tracker, y, dpp);
}

int RGWSI_Bucket_SObj::read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                 const string& key,
                                                 RGWBucketInfo *info,
                                                 real_time *pmtime, map<string, bufferlist> *pattrs,
                                                 optional_yield y,
                                                 const DoutPrefixProvider *dpp,
                                                 rgw_cache_entry_info *cache_info,
                                                 boost::optional<obj_version> refresh_version)
{
  string cache_key("bi/");
  cache_key.append(key);

  if (auto e = binfo_cache->find(cache_key)) {
    if (refresh_version &&
        e->info.objv_tracker.read_version.compare(&(*refresh_version))) {
      ldpp_dout(dpp, -1) << "WARNING: The bucket info cache is inconsistent. This is "
        << "a failure that should be debugged. I am a nice machine, "
        << "so I will try to recover." << dendl;
      binfo_cache->invalidate(key);
    } else {
      *info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  bucket_info_cache_entry e;
  rgw_cache_entry_info ci;

  int ret = do_read_bucket_instance_info(ctx, key,
                                  &e.info, &e.mtime, &e.attrs,
                                  &ci, refresh_version, y, dpp);
  *info = e.info;

  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: do_read_bucket_instance_info failed: " << ret << dendl;
    } else {
      ldpp_dout(dpp, 20) << "do_read_bucket_instance_info, bucket instance not found (key=" << key << ")" << dendl;
    }
    return ret;
  }

  if (pmtime) {
    *pmtime = e.mtime;
  }
  if (pattrs) {
    *pattrs = e.attrs;
  }
  if (cache_info) {
    *cache_info = ci;
  }

  /* chain to only bucket instance and *not* bucket entrypoint */
  if (!binfo_cache->put(dpp, svc.cache, cache_key, &e, {&ci})) {
    ldpp_dout(dpp, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info->objv_tracker.read_version)) {
    ldpp_dout(dpp, -1) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}

int RGWSI_Bucket_SObj::do_read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                    const string& key,
                                                    RGWBucketInfo *info,
                                                    real_time *pmtime, map<string, bufferlist> *pattrs,
                                                    rgw_cache_entry_info *cache_info,
                                                    boost::optional<obj_version> refresh_version,
                                                    optional_yield y,
                                                    const DoutPrefixProvider *dpp)
{
  bufferlist bl;
  RGWObjVersionTracker ot;

  auto params = RGWSI_MBSObj_GetParams(&bl, pattrs, pmtime).set_cache_info(cache_info)
                                                           .set_refresh_version(refresh_version);

  int ret = svc.meta_be->get_entry(ctx.get(), key, params, &ot, y, dpp);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info->objv_tracker = ot;
  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                        const rgw_bucket& bucket,
                                        RGWBucketInfo *info,
                                        real_time *pmtime,
                                        map<string, bufferlist> *pattrs,
                                        boost::optional<obj_version> refresh_version,
                                        optional_yield y,
                                        const DoutPrefixProvider *dpp)
{
  rgw_cache_entry_info cache_info;

  if (!bucket.bucket_id.empty()) {
    return read_bucket_instance_info(ctx.bi, get_bi_meta_key(bucket),
                                     info,
                                     pmtime, pattrs,
                                     y,
                                     dpp,
                                     &cache_info, refresh_version);
  }

  string bucket_entry = get_entrypoint_meta_key(bucket);
  string cache_key("b/");
  cache_key.append(bucket_entry);

  if (auto e = binfo_cache->find(cache_key)) {
    bool found_version = (bucket.bucket_id.empty() ||
                          bucket.bucket_id == e->info.bucket.bucket_id);

    if (!found_version ||
        (refresh_version &&
         e->info.objv_tracker.read_version.compare(&(*refresh_version)))) {
      ldpp_dout(dpp, -1) << "WARNING: The bucket info cache is inconsistent. This is "
        << "a failure that should be debugged. I am a nice machine, "
        << "so I will try to recover." << dendl;
      binfo_cache->invalidate(cache_key);
    } else {
      *info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  rgw_cache_entry_info entry_cache_info;
  int ret = read_bucket_entrypoint_info(ctx.ep, bucket_entry,
                                        &entry_point, &ot, &ep_mtime, pattrs,
                                        y,
                                        dpp,
                                        &entry_cache_info, refresh_version);
  if (ret < 0) {
    /* only init these fields */
    info->bucket = bucket;
    return ret;
  }

  if (entry_point.has_bucket_info) {
    *info = entry_point.old_bucket_info;
    info->bucket.tenant = bucket.tenant;
    ldpp_dout(dpp, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info->bucket << " owner " << info->owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldpp_dout(dpp, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  bucket_info_cache_entry e;

  ret = read_bucket_instance_info(ctx.bi, get_bi_meta_key(entry_point.bucket),
                                  &e.info, &e.mtime, &e.attrs,
                                  y,
                                  dpp,
                                  &cache_info, refresh_version);
  *info = e.info;
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: read_bucket_instance_from_oid failed: " << ret << dendl;
    info->bucket = bucket;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  /* chain to both bucket entry point and bucket instance */
  if (!binfo_cache->put(dpp, svc.cache, cache_key, &e, {&entry_cache_info, &cache_info})) {
    ldpp_dout(dpp, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info->objv_tracker.read_version)) {
    ldpp_dout(dpp, -1) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}


int RGWSI_Bucket_SObj::store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                  const string& key,
                                                  RGWBucketInfo& info,
                                                  std::optional<RGWBucketInfo *> orig_info,
                                                  bool exclusive,
                                                  real_time mtime,
                                                  const map<string, bufferlist> *pattrs,
                                                  optional_yield y,
                                                  const DoutPrefixProvider *dpp)
{
  bufferlist bl;
  encode(info, bl);

  /*
   * we might need some special handling if overwriting
   */
  RGWBucketInfo shared_bucket_info;
  if (!orig_info && !exclusive) {  /* if exclusive, we're going to fail when try
                                      to overwrite, so the whole check here is moot */
    /*
     * we're here because orig_info wasn't passed in
     * we don't have info about what was there before, so need to fetch first
     */
    int r  = read_bucket_instance_info(ctx,
                                       key,
                                       &shared_bucket_info,
                                       nullptr, nullptr,
                                       y,
                                       dpp,
                                       nullptr, boost::none);
    if (r < 0) {
      if (r != -ENOENT) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): read_bucket_instance_info() of key=" << key << " returned r=" << r << dendl;
        return r;
      }
    } else {
      orig_info = &shared_bucket_info;
    }
  }

  if (orig_info && *orig_info && !exclusive) {
    int r = svc.bi->handle_overwrite(dpp, info, *(orig_info.value()), y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): svc.bi->handle_overwrite() of key=" << key << " returned r=" << r << dendl;
      return r;
    }
  }

  RGWSI_MBSObj_PutParams params(bl, pattrs, mtime, exclusive);

  int ret = svc.meta_be->put(ctx.get(), key, params, &info.objv_tracker, y, dpp);

  if (ret >= 0) {
    int r = svc.bucket_sync->handle_bi_update(dpp, info,
                                              orig_info.value_or(nullptr),
                                              y);
    if (r < 0) {
      return r;
    }
  } else if (ret == -EEXIST) {
    /* well, if it's exclusive we shouldn't overwrite it, because we might race with another
     * bucket operation on this specific bucket (e.g., being synced from the master), but
     * since bucket instance meta object is unique for this specific bucket instance, we don't
     * need to return an error.
     * A scenario where we'd get -EEXIST here, is in a multi-zone config, we're not on the
     * master, creating a bucket, sending bucket creation to the master, we create the bucket
     * locally, while in the sync thread we sync the new bucket.
     */
    ret = 0;
  }

  if (ret < 0) {
    return ret;
  }

  return ret;
}

int RGWSI_Bucket_SObj::remove_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                   const string& key,
                                                   const RGWBucketInfo& info,
                                                   RGWObjVersionTracker *objv_tracker,
                                                   optional_yield y,
                                                   const DoutPrefixProvider *dpp)
{
  RGWSI_MBSObj_RemoveParams params;
  int ret = svc.meta_be->remove_entry(dpp, ctx.get(), key, params, objv_tracker, y);

  if (ret < 0 &&
      ret != -ENOENT) {
    return ret;
  }

  int r = svc.bucket_sync->handle_bi_removal(dpp, info, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to update bucket instance sync index: r=" << r << dendl;
    /* returning success as index is just keeping hints, so will keep extra hints,
     * but bucket removal succeeded
     */
  }

  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_stats(const RGWBucketInfo& bucket_info,
                                         RGWBucketEnt *ent,
                                         optional_yield y,
                                         const DoutPrefixProvider *dpp)
{
  ent->count = 0;
  ent->size = 0;
  ent->size_rounded = 0;

  vector<rgw_bucket_dir_header> headers;

  int r = svc.bi->read_stats(dpp, bucket_info, ent, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): read_stats returned r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_stats(RGWSI_Bucket_X_Ctx& ctx,
                                         const rgw_bucket& bucket,
                                         RGWBucketEnt *ent,
                                         optional_yield y,
                                         const DoutPrefixProvider *dpp)
{
  RGWBucketInfo bucket_info;
  int ret = read_bucket_info(ctx, bucket, &bucket_info, nullptr, nullptr, boost::none, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return read_bucket_stats(bucket_info, ent, y, dpp);
}

int RGWSI_Bucket_SObj::read_buckets_stats(RGWSI_Bucket_X_Ctx& ctx,
                                          std::vector<RGWBucketEnt>& buckets,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  for (auto& ent : buckets) {
    int r = read_bucket_stats(ctx, ent.bucket, &ent, y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): read_bucket_stats returned r=" << r << dendl;
      return r;
    }
  }

  return buckets.size();
}
