

#include "svc_bucket.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_meta.h"
#include "svc_sync_modules.h"

#include "rgw/rgw_bucket.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWSI_Bucket::RGWSI_Bucket(CephContext *cct): RGWServiceInstance(cct) {
}

RGWSI_Bucket::~RGWSI_Bucket() {
}

RGWSI_Bucket::Instance RGWSI_Bucket::instance(RGWSysObjectCtx& _ctx,
                                              const string& _tenant,
                                              const string& _bucket_name) {
  return Instance(this, _ctx, _tenant, _bucket_name);
}

RGWSI_Bucket::Instance RGWSI_Bucket::instance(RGWSysObjectCtx& _ctx,
                                              const rgw_bucket& _bucket) {
  return Instance(this, _ctx, _bucket);
}

void RGWSI_Bucket::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                        RGWSI_SysObj_Cache *_cache_svc, RGWSI_Meta *_meta_svc,
                        RGWSI_SyncModules *_sync_modules_svc)
{
  svc.bucket = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.meta = _meta_svc;
  svc.sync_modules = _sync_modules_svc;
}

int RGWSI_Bucket::do_start()
{
  binfo_cache.reset(new RGWChainedCacheImpl<bucket_info_cache_entry>);
  binfo_cache->init(svc.cache);
  return 0;
}

int RGWSI_Bucket::Instance::read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                                        RGWBucketEntryPoint *entry_point,
                                                        RGWObjVersionTracker *objv_tracker,
                                                        real_time *pmtime,
                                                        map<string, bufferlist> *pattrs,
                                                        rgw_cache_entry_info *cache_info,
                                                        boost::optional<obj_version> refresh_version)
{
  bufferlist bl;
  string bucket_entry;

  rgw_make_bucket_entry_name(bucket.tenant, bucket.name, bucket_entry);
  int ret = rgw_get_system_obj(ctx, svc.zone->get_zone_params().domain_root,
			       bucket_entry, bl, objv_tracker, pmtime, pattrs,
			       cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*entry_point, iter);
  } catch (buffer::error& err) {
    ldout(svc.bucket->cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_Bucket::Instance::read_bucket_info(const rgw_raw_obj& obj,
                                             RGWBucketInfo *info,
                                             real_time *pmtime, map<string, bufferlist> *pattrs,
                                             rgw_cache_entry_info *cache_info,
                                             boost::optional<obj_version> refresh_version)
{
  ldout(svc.bucket->cct, 20) << "reading from " << obj << dendl;

  bufferlist epbl;

  int ret = rgw_get_system_obj(ctx, obj.pool, obj.oid,
			       epbl, &info->objv_tracker, pmtime, pattrs,
			       cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  auto iter = epbl.cbegin();
  try {
    decode(*info, iter);
  } catch (buffer::error& err) {
    ldout(svc.bucket->cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info->bucket.oid = obj.oid;
  return 0;
}

int RGWSI_Bucket::Instance::read_bucket_instance_info(const rgw_bucket& bucket,
                                                      RGWBucketInfo *info,
                                                      real_time *pmtime,
                                                      map<string, bufferlist> *pattrs,
                                                      rgw_cache_entry_info *cache_info,
                                                      boost::optional<obj_version> refresh_version,
                                                      optional_yield y)
{
  auto& domain_root = svc.zone->get_zone_params().domain_root;
  rgw_raw_obj obj(domain_root,
                  (bucket.oid.empty() ?  get_meta_oid(bucket) : bucket.oid));

  return read_bucket_info(obj, info, pmtime, pattrs, cache_info, refresh_version);
}

int RGWSI_Bucket::Instance::read_bucket_info(const rgw_bucket& bucket,
                                             RGWBucketInfo *info,
                                             real_time *pmtime,
                                             map<string, bufferlist> *pattrs,
                                             boost::optional<obj_version> refresh_version)
{
  rgw_cache_entry_info cache_info;

  if (!bucket.bucket_id.empty()) {
    return read_bucket_instance_info(bucket,
                                     info,
                                     pmtime, pattrs,
                                     &cache_info, refresh_version);
  }

  string bucket_entry;

  rgw_make_bucket_entry_name(bucket.tenant, bucket.name, bucket_entry);

  if (auto e = svc.bucket->binfo_cache->find(bucket_entry)) {
    if (refresh_version &&
        e->info.objv_tracker.read_version.compare(&(*refresh_version))) {
      lderr(svc.bucket->cct) << "WARNING: The bucket info cache is inconsistent. This is "
        << "a failure that should be debugged. I am a nice machine, "
        << "so I will try to recover." << dendl;
      svc.bucket->binfo_cache->invalidate(bucket_entry);
    } else {
      *info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  auto& domain_root = svc.zone->get_zone_params().domain_root;

  bucket_info_cache_entry e;
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  rgw_cache_entry_info entry_cache_info;
  int ret = read_bucket_entrypoint_info(bucket,
                                        &entry_point, &ot, &ep_mtime, pattrs,
                                        &entry_cache_info, refresh_version);
  if (ret < 0) {
    /* only init these fields */
    info->bucket = bucket;
    return ret;
  }

  if (entry_point.has_bucket_info) {
    *info = entry_point.old_bucket_info;
    info->bucket.oid = bucket.name;
    info->bucket.tenant = bucket.tenant;
    info->ep_objv = ot.read_version;
    ldout(svc.bucket->cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info->bucket << " owner " << info->owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(svc.bucket->cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  string oid = get_meta_oid(entry_point.bucket);

  rgw_raw_obj obj(domain_root, oid);

  ret = read_bucket_info(obj, &e.info, &e.mtime, &e.attrs,
                         &cache_info, refresh_version, y);
  e.info.ep_objv = ot.read_version;
  *info = e.info;
  if (ret < 0) {
    lderr(svc.bucket->cct) << "ERROR: read_bucket_instance_from_oid failed: " << ret << dendl;
    info->bucket = bucket;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  /* chain to both bucket entry point and bucket instance */
  if (!svc.bucket->binfo_cache->put(svc.cache, bucket_entry, &e, {&entry_cache_info, &cache_info})) {
    ldout(svc.bucket->cct, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info->objv_tracker.read_version)) {
    lderr(svc.bucket->cct) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}

int RGWSI_Bucket::Instance::write_bucket_instance_info(RGWBucketInfo& info,
                                                       bool exclusive,
                                                       real_time mtime,
                                                       map<string, bufferlist> *pattrs)
{
  info.has_instance_obj = true;

  string key = info.bucket.get_key(); /* when we go through meta api, we don't use oid directly */
  int ret = rgw_bucket_instance_store_info(svc.meta, key, info, exclusive, pattrs, &info.objv_tracker, mtime);
  if (ret == -EEXIST) {
    /* well, if it's exclusive we shouldn't overwrite it, because we might race with another
     * bucket operation on this specific bucket (e.g., being synced from the master), but
     * since bucket instace meta object is unique for this specific bucket instace, we don't
     * need to return an error.
     * A scenario where we'd get -EEXIST here, is in a multi-zone config, we're not on the
     * master, creating a bucket, sending bucket creation to the master, we create the bucket
     * locally, while in the sync thread we sync the new bucket.
     */
    ret = 0;
  }
  return ret;
}

int RGWSI_Bucket::Instance::GetOp::exec()
{
  int r = source.read_bucket_info(source.bucket, &source.bucket_info,
                                  pmtime, pattrs,
                                  refresh_version,
                                  y);
  if (r < 0) {
    return r;
  }

  if (pinfo) {
    *pinfo = source.bucket_info;
  }

  return 0;
}

int RGWSI_Bucket::Instance::SetOp::exec()
{
  int r = source.write_bucket_instance_info(source.bucket_info,
                                            exclusive,
                                            mtime, pattrs);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_Bucket::store_bucket_entrypoint_info(const string& tenant, const string& bucket_name,
                                               RGWBucketEntryPoint& be, bool exclusive,
                                               RGWObjVersionTracker *objv_tracker, real_time mtime,
                                               std::map<string, bufferlist>&& attrs)
{
  string entry;
  rgw_make_bucket_entry_name(tenant, bucket_name, entry);
  auto apply_type = (exclusive ? APPLY_EXCLUSIVE : APPLY_ALWAYS);
  RGWBucketEntryMetadataObject mdo(be, objv_tracker->write_version, mtime, std::move(attrs));
  return bucket_meta_handler->put(entry, &mdo, *objv_tracker, apply_type);
}

int RGWSI_Bucket::store_bucket_instance_info(RGWBucketInfo& bucket_info, bool exclusive,
                                             map<string, bufferlist>& attrs,
                                             RGWObjVersionTracker *objv_tracker,
                                             real_time mtime)
{
  string entry = bucket_info.bucket.get_key();
  auto apply_type = (exclusive ? APPLY_EXCLUSIVE : APPLY_ALWAYS);
  RGWBucketCompleteInfo bci{bucket_info, attrs};
  RGWBucketInstanceMetadataObject mdo(bci, objv_tracker->write_version, mtime);
  return bucket_instance_meta_handler->put(entry, &mdo, *objv_tracker, apply_type);
}

int RGWSI_Bucket::remove_bucket_instance_info(const rgw_bucket& bucket,
                                              RGWObjVersionTracker *objv_tracker)
{
  string entry = bucket.get_key();
  return bucket_instance_meta_handler->remove(entry, *objv_tracker);
}

