// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_bucket_sobj.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_bi.h"
#include "svc_mdlog.h"
#include "svc_sync_modules.h"

#include "rgw_bucket.h"
#include "rgw_metadata_lister.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static const std::string instance_oid_prefix = ".bucket.meta.";

// convert bucket instance oids back to the tenant/ format for metadata keys.
// it's safe to parse 'tenant:' only for oids, because they won't contain the
// optional :shard at the end
static std::string instance_meta_key_to_oid(const std::string& metadata_key)
{
  std::string oid = string_cat_reserve(instance_oid_prefix, metadata_key);

  // replace tenant/ with tenant:
  auto c = oid.find('/', instance_oid_prefix.size());
  if (c != string::npos) {
    oid[c] = ':';
  }

  return oid;
}

// convert bucket instance oids back to the tenant/ format for metadata keys.
// it's safe to parse 'tenant:' only for oids, because they won't contain the
// optional :shard at the end
static std::string instance_oid_to_meta_key(const std::string& oid)
{
  if (oid.size() < instance_oid_prefix.size()) { /* just sanity check */
    return string();
  }

  std::string key = oid.substr(instance_oid_prefix.size());

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


RGWSI_Bucket_SObj::RGWSI_Bucket_SObj(CephContext *cct): RGWSI_Bucket(cct) {
}

RGWSI_Bucket_SObj::~RGWSI_Bucket_SObj() {
}

void RGWSI_Bucket_SObj::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                             RGWSI_SysObj_Cache *_cache_svc, RGWSI_BucketIndex *_bi,
                             RGWSI_MDLog* mdlog_svc,
                             RGWSI_SyncModules *_sync_modules_svc,
                             RGWSI_Bucket_Sync *_bucket_sync_svc)
{
  svc.bucket = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.bi = _bi;
  svc.mdlog = mdlog_svc;
  svc.sync_modules = _sync_modules_svc;
  svc.bucket_sync = _bucket_sync_svc;
}

int RGWSI_Bucket_SObj::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  binfo_cache.reset(new RGWChainedCacheImpl<bucket_info_cache_entry>);
  binfo_cache->init(svc.cache);
  return 0;
}


class BucketEntrypointLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  void filter_transform(std::vector<std::string>& oids,
                        std::list<std::string>& keys) override
  {
    // bucket entrypoints and instances share a namespace, so filter out the
    // instances based on prefix
    constexpr auto filter = [] (const std::string& oid) {
                              return oid.starts_with('.');
                            };
    // 'oids' is mutable so we can move its elements instead of copying
    std::remove_copy_if(std::make_move_iterator(oids.begin()),
                        std::make_move_iterator(oids.end()),
                        std::back_inserter(keys), filter);
  }
};

int RGWSI_Bucket_SObj::create_entrypoint_lister(
    const DoutPrefixProvider* dpp,
    const std::string& marker,
    std::unique_ptr<RGWMetadataLister>& lister)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  auto p = std::make_unique<BucketEntrypointLister>(svc.sysobj->get_pool(pool));
  int r = p->init(dpp, marker, ""); // empty prefix
  if (r < 0) {
    return r;
  }
  lister = std::move(p);
  return 0;
}


class BucketInstanceLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  void filter_transform(std::vector<std::string>& oids,
                        std::list<std::string>& keys) override
  {
    // transform instance oids to metadata keys
    std::transform(oids.begin(), oids.end(),
                   std::back_inserter(keys),
                   instance_oid_to_meta_key);
  }
};

int RGWSI_Bucket_SObj::create_instance_lister(
    const DoutPrefixProvider* dpp,
    const std::string& marker,
    std::unique_ptr<RGWMetadataLister>& lister)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  auto p = std::make_unique<BucketInstanceLister>(svc.sysobj->get_pool(pool));
  int r = p->init(dpp, marker, instance_oid_prefix);
  if (r < 0) {
    return r;
  }
  lister = std::move(p);
  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_entrypoint_info(const string& key,
                                                   RGWBucketEntryPoint *entry_point,
                                                   RGWObjVersionTracker *objv_tracker,
                                                   real_time *pmtime,
                                                   map<string, bufferlist> *pattrs,
                                                   optional_yield y,
                                                   const DoutPrefixProvider *dpp,
                                                   rgw_cache_entry_info *cache_info,
                                                   boost::optional<obj_version> refresh_version)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  bufferlist bl;
  int ret = rgw_get_system_obj(svc.sysobj, pool, key, bl,
                               objv_tracker, pmtime, y, dpp,
                               pattrs, cache_info, refresh_version);
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

int RGWSI_Bucket_SObj::store_bucket_entrypoint_info(const string& key,
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

  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  int ret = rgw_put_system_obj(dpp, svc.sysobj, pool, key, bl, exclusive,
                               objv_tracker, mtime, y, pattrs);
  if (ret < 0) {
    return ret;
  }

  return svc.mdlog->complete_entry(dpp, y, "bucket", key, objv_tracker);
}

int RGWSI_Bucket_SObj::remove_bucket_entrypoint_info(const string& key,
                                                     RGWObjVersionTracker *objv_tracker,
                                                     optional_yield y,
                                                     const DoutPrefixProvider *dpp)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  int ret = rgw_delete_system_obj(dpp, svc.sysobj, pool, key, objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  return svc.mdlog->complete_entry(dpp, y, "bucket", key, objv_tracker);
}

int RGWSI_Bucket_SObj::read_bucket_instance_info(const string& key,
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

  int ret = do_read_bucket_instance_info(key, &e.info, &e.mtime, &e.attrs,
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

int RGWSI_Bucket_SObj::do_read_bucket_instance_info(const string& key,
                                                    RGWBucketInfo *info,
                                                    real_time *pmtime, map<string, bufferlist> *pattrs,
                                                    rgw_cache_entry_info *cache_info,
                                                    boost::optional<obj_version> refresh_version,
                                                    optional_yield y,
                                                    const DoutPrefixProvider *dpp)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  const std::string oid = instance_meta_key_to_oid(key);
  bufferlist bl;
  RGWObjVersionTracker objv;

  int ret = rgw_get_system_obj(svc.sysobj, pool, oid, bl, &objv, pmtime, y,
                               dpp, pattrs, cache_info, refresh_version);
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
  info->objv_tracker = objv;
  return 0;
}

int RGWSI_Bucket_SObj::read_bucket_info(const rgw_bucket& bucket,
                                        RGWBucketInfo *info,
                                        real_time *pmtime,
                                        map<string, bufferlist> *pattrs,
                                        boost::optional<obj_version> refresh_version,
                                        optional_yield y,
                                        const DoutPrefixProvider *dpp)
{
  rgw_cache_entry_info cache_info;

  if (!bucket.bucket_id.empty()) {
    return read_bucket_instance_info(get_bi_meta_key(bucket), info,
                                     pmtime, pattrs, y, dpp,
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
  int ret = read_bucket_entrypoint_info(bucket_entry, &entry_point, &ot,
                                        &ep_mtime, pattrs, y, dpp,
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

  ret = read_bucket_instance_info(get_bi_meta_key(entry_point.bucket),
                                  &e.info, &e.mtime, &e.attrs, y, dpp,
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


int RGWSI_Bucket_SObj::store_bucket_instance_info(const string& key,
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
    int r  = read_bucket_instance_info(key, &shared_bucket_info,
                                       nullptr, nullptr, y, dpp,
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

  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  const std::string oid = instance_meta_key_to_oid(key);
  int ret = rgw_put_system_obj(dpp, svc.sysobj, pool, oid, bl, exclusive,
                               &info.objv_tracker, mtime, y, pattrs);
  if (ret >= 0) {
    int r = svc.mdlog->complete_entry(dpp, y, "bucket.instance",
                                      key, &info.objv_tracker);
    if (r < 0) {
      return r;
    }

    r = svc.bucket_sync->handle_bi_update(dpp, info, orig_info.value_or(nullptr), y);
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

int RGWSI_Bucket_SObj::remove_bucket_instance_info(const string& key,
                                                   const RGWBucketInfo& info,
                                                   RGWObjVersionTracker *objv_tracker,
                                                   optional_yield y,
                                                   const DoutPrefixProvider *dpp)
{
  const rgw_pool& pool = svc.zone->get_zone_params().domain_root;
  const std::string oid = instance_meta_key_to_oid(key);
  int ret = rgw_delete_system_obj(dpp, svc.sysobj, pool, oid, objv_tracker, y);
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

int RGWSI_Bucket_SObj::read_bucket_stats(const rgw_bucket& bucket,
                                         RGWBucketEnt *ent,
                                         optional_yield y,
                                         const DoutPrefixProvider *dpp)
{
  RGWBucketInfo bucket_info;
  int ret = read_bucket_info(bucket, &bucket_info, &ent->modification_time, nullptr, boost::none, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return read_bucket_stats(bucket_info, ent, y, dpp);
}

int RGWSI_Bucket_SObj::read_buckets_stats(std::vector<RGWBucketEnt>& buckets,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  for (auto& ent : buckets) {
    int r = read_bucket_stats(ent.bucket, &ent, y, dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): read_bucket_stats returned r=" << r << dendl;
      return r;
    }
  }

  return buckets.size();
}
