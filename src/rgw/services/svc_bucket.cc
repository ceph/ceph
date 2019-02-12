

#include "svc_bucket.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"

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

void RGWSI_Bucket::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc, RGWSI_SysObj_Cache *_cache_svc)
{
  zone_svc = _zone_svc;
  sysobj_svc = _sysobj_svc;
  cache_svc = _cache_svc;
}

int RGWSI_Bucket::do_start()
{
  binfo_cache.reset(new RGWChainedCacheImpl<bucket_info_cache_entry>);
  binfo_cache->init(cache_svc);

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
  int ret = rgw_get_system_obj(ctx, bucket_svc->zone_svc->get_zone_params().domain_root,
			       bucket_entry, bl, objv_tracker, pmtime, pattrs,
			       cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*entry_point, iter);
  } catch (buffer::error& err) {
    ldout(bucket_svc->cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
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
  ldout(bucket_svc->cct, 20) << "reading from " << obj << dendl;

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
    ldout(bucket_svc->cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
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
                                                      boost::optional<obj_version> refresh_version)
{
  auto& domain_root = bucket_svc->zone_svc->get_zone_params().domain_root;
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

  if (auto e = bucket_svc->binfo_cache->find(bucket_entry)) {
    if (refresh_version &&
        e->info.objv_tracker.read_version.compare(&(*refresh_version))) {
      lderr(bucket_svc->cct) << "WARNING: The bucket info cache is inconsistent. This is "
        << "a failure that should be debugged. I am a nice machine, "
        << "so I will try to recover." << dendl;
      bucket_svc->binfo_cache->invalidate(bucket_entry);
    } else {
      *info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  auto& domain_root = bucket_svc->zone_svc->get_zone_params().domain_root;

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
    ldout(bucket_svc->cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info->bucket << " owner " << info->owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(bucket_svc->cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  string oid = get_meta_oid(entry_point.bucket);

  rgw_raw_obj obj(domain_root, oid);

  ret = read_bucket_info(obj, &e.info, &e.mtime, &e.attrs,
                         &cache_info, refresh_version);
  e.info.ep_objv = ot.read_version;
  *info = e.info;
  if (ret < 0) {
    lderr(bucket_svc->cct) << "ERROR: read_bucket_instance_from_oid failed: " << ret << dendl;
    info->bucket = bucket;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  /* chain to both bucket entry point and bucket instance */
  if (!bucket_svc->binfo_cache->put(bucket_svc->cache_svc, bucket_entry, &e, {&entry_cache_info, &cache_info})) {
    ldout(bucket_svc->cct, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info->objv_tracker.read_version)) {
    lderr(bucket_svc->cct) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}


int RGWSI_Bucket::Instance::GetOp::exec()
{
  int r = source.read_bucket_info(source.bucket, &source.bucket_info,
                                  pmtime, pattrs,
                                  refresh_version);
  if (r < 0) {
    return r;
  }

  if (pinfo) {
    *pinfo = source.bucket_info;
  }

  return 0;
}
