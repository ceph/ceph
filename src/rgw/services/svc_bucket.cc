

#include "svc_bucket.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sys_obj_cache.h"
#include "svc_bi.h"
#include "svc_meta.h"
#include "svc_meta_be_sobj.h"
#include "svc_sync_modules.h"

#include "rgw/rgw_bucket.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

#define RGW_BUCKET_INSTANCE_MD_PREFIX ".bucket.meta."

class RGWSI_Bucket_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Bucket::Svc& svc;

  const string prefix;
public:
  RGWSI_Bucket_Module(RGWSI_Bucket::Svc& _svc) : RGWSI_MBSObj_Handler_Module("bucket"),
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

class RGWSI_BucketInstance_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Bucket::Svc& svc;

  const string prefix;
public:
  RGWSI_BucketInstance_Module(RGWSI_Bucket::Svc& _svc) : RGWSI_MBSObj_Handler_Module("bucket.instance"),
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

RGWSI_Bucket::RGWSI_Bucket(CephContext *cct): RGWServiceInstance(cct) {
}

RGWSI_Bucket::~RGWSI_Bucket() {
}

void RGWSI_Bucket::init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
                        RGWSI_SysObj_Cache *_cache_svc, RGWSI_BucketIndex *_bi,
                        RGWSI_Meta *_meta_svc, RGWSI_MetaBackend *_meta_be_svc,
                        RGWSI_SyncModules *_sync_modules_svc)
{
  svc.bucket = this;
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.bi = _bi;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sync_modules = _sync_modules_svc;
}

string RGWSI_Bucket::get_entrypoint_meta_key(const rgw_bucket& bucket)
{
  if (bucket.bucket_id.empty()) {
    return bucket.get_key();
  }

  rgw_bucket b(bucket);
  b.bucket_id.clear();

  return b.get_key();
}

string RGWSI_Bucket::get_bi_meta_key(const rgw_bucket& bucket)
{
  return bucket.get_key();
}

int RGWSI_Bucket::do_start()
{
  binfo_cache.reset(new RGWChainedCacheImpl<bucket_info_cache_entry>);
  binfo_cache->init(svc.cache);

  /* create first backend handler for bucket entrypoints */

  RGWSI_MetaBackend_Handler *ep_handler;

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ, &ep_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  ep_be_handler = ep_handler;

  RGWSI_MetaBackend_Handler_SObj *ep_bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(ep_handler);

  auto ep_module = new RGWSI_Bucket_Module(svc);
  ep_be_module.reset(ep_module);
  ep_bh->set_module(ep_module);

  /* create a second backend handler for bucket instance */

  RGWSI_MetaBackend_Handler *bi_handler;

  r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ, &bi_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be handler: r=" << r << dendl;
    return r;
  }

  bi_be_handler = bi_handler;

  RGWSI_MetaBackend_Handler_SObj *bi_bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(bi_handler);

  auto bi_module = new RGWSI_BucketInstance_Module(svc);
  bi_be_module.reset(bi_module);
  bi_bh->set_module(bi_module);

  return 0;
}

int RGWSI_Bucket::read_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                              const string& key,
                                              RGWBucketEntryPoint *entry_point,
                                              RGWObjVersionTracker *objv_tracker,
                                              real_time *pmtime,
                                              map<string, bufferlist> *pattrs,
                                              rgw_cache_entry_info *cache_info,
                                              boost::optional<obj_version> refresh_version)
{
  bufferlist bl;

  auto params = RGWSI_MBSObj_GetParams(&bl, pattrs, pmtime).set_cache_info(cache_info)
                                                           .set_refresh_version(refresh_version);
                                                    
  int ret = svc.meta_be->get_entry(ctx.get(), key, params, objv_tracker);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*entry_point, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_Bucket::store_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                               const string& key,
                                               RGWBucketEntryPoint& info,
                                               bool exclusive,
                                               real_time mtime,
                                               map<string, bufferlist> *pattrs,
                                               RGWObjVersionTracker *objv_tracker)
{
  bufferlist bl;
  encode(info, bl);

  RGWSI_MBSObj_PutParams params(bl, pattrs, mtime, exclusive);

  int ret = svc.meta_be->put_entry(ctx.get(), key, params, objv_tracker);
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

  if (ret < 0) {
    return ret;
  }

  return ret;
}

int RGWSI_Bucket::remove_bucket_entrypoint_info(RGWSI_Bucket_EP_Ctx& ctx,
                                                const string& key,
                                                RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MBSObj_RemoveParams params;
  return svc.meta_be->remove_entry(ctx.get(), key, params, objv_tracker);
}

int RGWSI_Bucket::read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                            const string& key,
                                            RGWBucketInfo *info,
                                            real_time *pmtime, map<string, bufferlist> *pattrs,
                                            rgw_cache_entry_info *cache_info,
                                            boost::optional<obj_version> refresh_version)
{
#warning cache set/get is a mess
  if (auto e = binfo_cache->find(key)) {
    if (refresh_version &&
        e->info.objv_tracker.read_version.compare(&(*refresh_version))) {
      lderr(cct) << "WARNING: The bucket info cache is inconsistent. This is "
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

  int ret = do_read_bucket_instance_info(ctx, key, info, pmtime, pattrs,
                                         cache_info, refresh_version);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWSI_Bucket::do_read_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                               const string& key,
                                               RGWBucketInfo *info,
                                               real_time *pmtime, map<string, bufferlist> *pattrs,
                                               rgw_cache_entry_info *cache_info,
                                               boost::optional<obj_version> refresh_version)
{
  bufferlist bl;
  RGWObjVersionTracker ot;

  auto params = RGWSI_MBSObj_GetParams(&bl, pattrs, pmtime).set_cache_info(cache_info)
                                                           .set_refresh_version(refresh_version);

  int ret = svc.meta_be->get_entry(ctx.get(), key, params, &ot);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(*info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info->objv_tracker = ot;
#warning need to remove field?
#if 0
  info->bucket.oid = obj.oid;
#endif
  return 0;
}

int RGWSI_Bucket::read_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                   const rgw_bucket& bucket,
                                   RGWBucketInfo *info,
                                   real_time *pmtime,
                                   map<string, bufferlist> *pattrs,
                                   boost::optional<obj_version> refresh_version)
{
  rgw_cache_entry_info cache_info;

  string bucket_entry = get_entrypoint_meta_key(bucket);

  if (auto e = binfo_cache->find(bucket_entry)) {
    bool found_version = (bucket.bucket_id.empty() ||
                          bucket.bucket_id == e->info.bucket.bucket_id);

    if (!found_version ||
        (refresh_version &&
         e->info.objv_tracker.read_version.compare(&(*refresh_version)))) {
      lderr(cct) << "WARNING: The bucket info cache is inconsistent. This is "
        << "a failure that should be debugged. I am a nice machine, "
        << "so I will try to recover." << dendl;
      binfo_cache->invalidate(bucket_entry);
    } else {
      *info = e->info;
      if (pattrs)
	*pattrs = e->attrs;
      if (pmtime)
	*pmtime = e->mtime;
      return 0;
    }
  }

  if (!bucket.bucket_id.empty()) {
    return read_bucket_instance_info(ctx.bi, get_bi_meta_key(bucket),
                                     info,
                                     pmtime, pattrs,
                                     &cache_info, refresh_version);
  }

  bucket_info_cache_entry e;
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  rgw_cache_entry_info entry_cache_info;
  int ret = read_bucket_entrypoint_info(ctx.ep, bucket_entry,
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
    ldout(cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info->bucket << " owner " << info->owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  ret = read_bucket_instance_info(ctx.bi, get_bi_meta_key(entry_point.bucket),
                                  &e.info, &e.mtime, &e.attrs,
                                  &cache_info, refresh_version);
  e.info.ep_objv = ot.read_version;
  *info = e.info;
  if (ret < 0) {
    lderr(cct) << "ERROR: read_bucket_instance_from_oid failed: " << ret << dendl;
    info->bucket = bucket;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  /* chain to both bucket entry point and bucket instance */
  if (!binfo_cache->put(svc.cache, bucket_entry, &e, {&entry_cache_info, &cache_info})) {
    ldout(cct, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  if (refresh_version &&
      refresh_version->compare(&info->objv_tracker.read_version)) {
    lderr(cct) << "WARNING: The OSD has the same version I have. Something may "
               << "have gone squirrelly. An administrator may have forced a "
               << "change; otherwise there is a problem somewhere." << dendl;
  }

  return 0;
}


int RGWSI_Bucket::store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                             const string& key,
                                             RGWBucketInfo& info,
                                             bool exclusive,
                                             real_time mtime,
                                             map<string, bufferlist> *pattrs)
{
  bufferlist bl;
  encode(info, bl);

  RGWSI_MBSObj_PutParams params(bl, pattrs, mtime, exclusive);

  int ret = svc.meta_be->put_entry(ctx.get(), key, params, &info.objv_tracker);
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

  if (ret < 0) {
    return ret;
  }

  return ret;
}

int RGWSI_Bucket::remove_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                              const string& key,
                                              RGWObjVersionTracker *objv_tracker)
{
  RGWSI_MBSObj_RemoveParams params;
  return svc.meta_be->remove_entry(ctx.get(), key, params, objv_tracker);
}

int RGWSI_Bucket::read_bucket_stats(const RGWBucketInfo& bucket_info,
                                    RGWBucketEnt *ent)
{
  ent->count = 0;
  ent->size = 0;
  ent->size_rounded = 0;

  vector<rgw_bucket_dir_header> headers;

  int r = svc.bi->read_stats(bucket_info, ent);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): read_stats returned r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_Bucket::read_bucket_stats(RGWSI_Bucket_X_Ctx& ctx,
                                    const rgw_bucket& bucket,
                                    RGWBucketEnt *ent)
{
  RGWBucketInfo bucket_info;
  int ret = read_bucket_info(ctx, bucket, &bucket_info, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }

  return read_bucket_stats(bucket_info, ent);
}

int RGWSI_Bucket::read_buckets_stats(RGWSI_Bucket_X_Ctx& ctx,
                                     map<string, RGWBucketEnt>& m)
{
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    int r = read_bucket_stats(ctx, ent.bucket, &ent);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): read_bucket_stats returned r=" << r << dendl;
      return r;
    }
  }

  return m.size();
}

