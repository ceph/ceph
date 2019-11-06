#include "svc_bucket_sync_sobj.h"
#include "svc_zone.h"
#include "svc_sys_obj_cache.h"
#include "svc_bucket_sobj.h"

#include "rgw/rgw_bucket_sync.h"
#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

static string bucket_sync_sources_oid_prefix = "bucket.sync-source-hints";
static string bucket_sync_targets_oid_prefix = "bucket.sync-target-hints";

RGWSI_Bucket_Sync_SObj::~RGWSI_Bucket_Sync_SObj() {
}

void RGWSI_Bucket_Sync_SObj::init(RGWSI_Zone *_zone_svc,
                                  RGWSI_SysObj *_sysobj_svc,
                                  RGWSI_SysObj_Cache *_cache_svc,
                                  RGWSI_Bucket_SObj *bucket_sobj_svc)
{
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.cache = _cache_svc;
  svc.bucket_sobj = bucket_sobj_svc;

  hint_index_mgr.init(svc.zone,
                      svc.sysobj);
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

  r = e.handler->init(y);
  if (r < 0) {
    ldout(cct, 20) << "ERROR: failed to init bucket sync policy handler: r=" << r << dendl;
    return r;
  }

  if (!sync_policy_cache->put(svc.cache, cache_key, &e, {&cache_info})) {
    ldout(cct, 20) << "couldn't put bucket_sync_policy cache entry, might have raced with data changes" << dendl;
  }

  *handler = e.handler;

  return 0;
}

static bool diff_sets(std::set<rgw_bucket>& orig_set,
                      std::set<rgw_bucket>& new_set,
                      vector<rgw_bucket> *added,
                      vector<rgw_bucket> *removed)
{
  auto oiter = orig_set.begin();
  auto niter = new_set.begin();

  while (oiter != orig_set.end() &&
         niter != new_set.end()) {
    if (*oiter == *niter) {
      ++oiter;
      ++niter;
      continue;
    }
    while (*oiter < *niter) {
      removed->push_back(*oiter);
      ++oiter;
    }
    while (*niter < *oiter) {
      added->push_back(*niter);
      ++niter;
    }
  }
  for (; oiter != orig_set.end(); ++oiter) {
    removed->push_back(*oiter);
  }
  for (; niter != new_set.end(); ++niter) {
    added->push_back(*niter);
  }

  return !(removed->empty() && added->empty());
}


class RGWSI_BS_SObj_HintIndexObj
{
  friend class RGWSI_Bucket_Sync_SObj;

  CephContext *cct;
  struct {
    RGWSI_SysObj *sysobj;
  } svc;

  RGWSysObjectCtx obj_ctx;
  rgw_raw_obj obj;
  RGWSysObj sysobj;

  RGWObjVersionTracker ot;

  bool has_data{false};

public:
  struct bi_entry {
    rgw_bucket bucket;
    map<rgw_bucket /* info_source */, obj_version> sources;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(bucket, bl);
      encode(sources, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(bucket, bl);
      decode(sources, bl);
      DECODE_FINISH(bl);
    }

    bool add(const rgw_bucket& info_source,
             const obj_version& info_source_ver) {
      auto& ver = sources[info_source];

      if (ver == info_source_ver) { /* already updated */
        return false;
      }

      if (info_source_ver.tag == ver.tag &&
          info_source_ver.ver < ver.ver) {
        return false;
      }

      ver = info_source_ver;

      return true;
    }

    bool remove(const rgw_bucket& info_source,
                const obj_version& info_source_ver) {
      auto iter = sources.find(info_source);
      if (iter == sources.end()) {
        return false;
      }

      auto& ver = iter->second;

      if (info_source_ver.tag == ver.tag &&
          info_source_ver.ver < ver.ver) {
        return false;
      }

      sources.erase(info_source);
      return true;
    }

    bool empty() const {
      return sources.empty();
    }
  };

  struct single_instance_info {
    map<rgw_bucket, bi_entry> entries;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(entries, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(entries, bl);
      DECODE_FINISH(bl);
    }

    bool add_entry(const rgw_bucket& info_source,
                   const obj_version& info_source_ver,
                   const rgw_bucket& bucket) {
      auto& entry = entries[bucket];

      if (!entry.add(info_source, info_source_ver)) {
        return false;
      }

      entry.bucket = bucket;

      return true;
    }

    bool remove_entry(const rgw_bucket& info_source,
                      const obj_version& info_source_ver,
                      const rgw_bucket& bucket) {
      auto iter = entries.find(bucket);
      if (iter == entries.end()) {
        return false;
      }

      if (!iter->second.remove(info_source, info_source_ver)) {
        return false;
      }

      if (iter->second.empty()) {
        entries.erase(iter);
      }

      return true;
    }

    void clear() {
      entries.clear();
    }

    bool empty() const {
      return entries.empty();
    }

    void get_entities(std::set<rgw_bucket> *result) const {
      for (auto& iter : entries) {
        result->insert(iter.first);
      }
    }
  };

  struct info_map {
    map<rgw_bucket, single_instance_info> instances;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(instances, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(instances, bl);
      DECODE_FINISH(bl);
    }

    bool empty() const {
      return instances.empty();
    }

    void clear() {
      instances.clear();
    }

    void get_entities(const rgw_bucket& bucket,
                      std::set<rgw_bucket> *result) const {
      auto iter = instances.find(bucket);
      if (iter == instances.end()) {
        return;
      }
      iter->second.get_entities(result);
    }
  } info;

  RGWSI_BS_SObj_HintIndexObj(RGWSI_SysObj *_sysobj_svc,
                             const rgw_raw_obj& _obj) : cct(_sysobj_svc->ctx()),
                                                        obj_ctx(_sysobj_svc->init_obj_ctx()),
                                                        obj(_obj),
                                                        sysobj(obj_ctx.get_obj(obj))
  {
    svc.sysobj = _sysobj_svc;
  }

  int update(const rgw_bucket& entity,
             const RGWBucketInfo& info_source,
             std::optional<std::vector<rgw_bucket> > add,
             std::optional<std::vector<rgw_bucket> > remove,
             optional_yield y);

private:
  void update_entries(const rgw_bucket& info_source,
                      const obj_version& info_source_ver,
                      std::optional<std::vector<rgw_bucket> > add,
                      std::optional<std::vector<rgw_bucket> > remove,
                      single_instance_info *instance);

  int read(optional_yield y);
  int flush(optional_yield y);

  void invalidate() {
    has_data = false;
    info.clear();
  }

  void get_entities(const rgw_bucket& bucket,
                    std::set<rgw_bucket> *result) const {
    info.get_entities(bucket, result);
  }
};
WRITE_CLASS_ENCODER(RGWSI_BS_SObj_HintIndexObj::bi_entry)
WRITE_CLASS_ENCODER(RGWSI_BS_SObj_HintIndexObj::single_instance_info)
WRITE_CLASS_ENCODER(RGWSI_BS_SObj_HintIndexObj::info_map)

int RGWSI_BS_SObj_HintIndexObj::update(const rgw_bucket& entity,
                                       const RGWBucketInfo& info_source,
                                       std::optional<std::vector<rgw_bucket> > add,
                                       std::optional<std::vector<rgw_bucket> > remove,
                                       optional_yield y)
{
  int r = 0;

  auto& info_source_ver = info_source.objv_tracker.read_version;

#define MAX_RETRIES 25

  for (int i = 0; i < MAX_RETRIES; ++i) {
    if (!has_data) {
      r = read(y);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: cannot update hint index: failed to read: r=" << r << dendl;
        return r;
      }
    }

    auto& instance = info.instances[entity];

    update_entries(info_source.bucket,
                   info_source_ver,
                   add, remove,
                   &instance);

    if (instance.empty()) {
      info.instances.erase(entity);
    }

    r = flush(y);
    if (r >= 0) {
      return 0;
    }

    if (r != -ECANCELED) {
      ldout(cct, 0) << "ERROR: failed to flush hint index: obj=" << obj << " r=" << r << dendl;
      return r;
    }
  }
  ldout(cct, 0) << "ERROR: failed to flush hint index: too many retries (obj=" << obj << "), likely a bug" << dendl;

  return -EIO;
}

void RGWSI_BS_SObj_HintIndexObj::update_entries(const rgw_bucket& info_source,
                                                const obj_version& info_source_ver,
                                                std::optional<std::vector<rgw_bucket> > add,
                                                std::optional<std::vector<rgw_bucket> > remove,
                                                single_instance_info *instance)
{
  if (remove) {
    for (auto& bucket : *remove) {
      instance->remove_entry(info_source, info_source_ver, bucket);
    }
  }

  if (add) {
    for (auto& bucket : *add) {
      instance->add_entry(info_source, info_source_ver, bucket);
    }
  }
}

int RGWSI_BS_SObj_HintIndexObj::read(optional_yield y) {
  RGWObjVersionTracker _ot;
  bufferlist bl;
  int r = sysobj.rop()
    .set_objv_tracker(&_ot) /* forcing read of current version */
    .read(&bl, y);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: failed reading data (obj=" << obj << "), r=" << r << dendl;
    return r;
  }

  ot = _ot;

  if (r >= 0) {
    auto iter = bl.cbegin();
    try {
      decode(info, iter);
      has_data = true;
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to decode entries, ignoring" << dendl;
      info.clear();
    }
  } else {
    info.clear();
  }

  return 0;
}

int RGWSI_BS_SObj_HintIndexObj::flush(optional_yield y) {
  int r;

  if (!info.empty()) {
    bufferlist bl;
    encode(info, bl);

    r = sysobj.wop()
      .set_objv_tracker(&ot) /* forcing read of current version */
      .write(bl, y);

  } else { /* remove */
    r = sysobj.wop()
      .set_objv_tracker(&ot)
      .remove(y);
  }

  if (r < 0) {
    return r;
  }

  return 0;
}

rgw_raw_obj RGWSI_Bucket_Sync_SObj::HintIndexManager::get_sources_obj(const rgw_bucket& bucket) const
{
  rgw_bucket b = bucket;
  b.bucket_id.clear();
  return rgw_raw_obj(svc.zone->get_zone_params().log_pool,
                     bucket_sync_sources_oid_prefix + "." + b.get_key());
}

rgw_raw_obj RGWSI_Bucket_Sync_SObj::HintIndexManager::get_dests_obj(const rgw_bucket& bucket) const
{
  rgw_bucket b = bucket;
  b.bucket_id.clear();
  return rgw_raw_obj(svc.zone->get_zone_params().log_pool,
                     bucket_sync_targets_oid_prefix + "." + b.get_key());
}

int RGWSI_Bucket_Sync_SObj::do_update_hints(const RGWBucketInfo& bucket_info,
                                            std::vector<rgw_bucket>& added_dests,
                                            std::vector<rgw_bucket>& removed_dests,
                                            std::vector<rgw_bucket>& added_sources,
                                            std::vector<rgw_bucket>& removed_sources,
                                            optional_yield y)
{
  std::vector<rgw_bucket> self_entity;
  self_entity.push_back(bucket_info.bucket);

  if (!added_dests.empty() ||
      !removed_dests.empty()) {
    /* update our dests */
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr.get_dests_obj(bucket_info.bucket));
    int r = index.update(bucket_info.bucket,
                         bucket_info,
                         added_dests,
                         removed_dests,
                         y);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << bucket_info.bucket << " r=" << r << dendl;
      return r;
    }

    /* update added dest buckets */
    for (auto& dest_bucket : added_dests) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           hint_index_mgr.get_sources_obj(dest_bucket));
      int r = dep_index.update(dest_bucket,
                               bucket_info,
                               self_entity,
                               std::nullopt,
                               y);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << dest_bucket << " r=" << r << dendl;
        return r;
      }
    }
    /* update removed dest buckets */
    for (auto& dest_bucket : removed_dests) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           hint_index_mgr.get_sources_obj(dest_bucket));
      int r = dep_index.update(dest_bucket,
                               bucket_info,
                               std::nullopt,
                               self_entity,
                               y);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << dest_bucket << " r=" << r << dendl;
        return r;
      }
    }
  }

  if (!added_dests.empty() ||
      !removed_dests.empty()) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr.get_sources_obj(bucket_info.bucket));
    /* update our sources */
    int r = index.update(bucket_info.bucket,
                         bucket_info,
                         added_sources,
                         removed_sources,
                         y);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << bucket_info.bucket << " r=" << r << dendl;
      return r;
    }

    /* update added sources buckets */
    for (auto& source_bucket : added_sources) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           hint_index_mgr.get_dests_obj(source_bucket));
      int r = dep_index.update(source_bucket,
                               bucket_info,
                               self_entity,
                               std::nullopt,
                               y);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << source_bucket << " r=" << r << dendl;
        return r;
      }
    }
    /* update removed dest buckets */
    for (auto& source_bucket : removed_sources) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           hint_index_mgr.get_dests_obj(source_bucket));
      int r = dep_index.update(source_bucket,
                               bucket_info,
                               std::nullopt,
                               self_entity,
                               y);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: failed to update targets index for bucket=" << source_bucket << " r=" << r << dendl;
        return r;
      }
    }
  }

  return 0;
}

int RGWSI_Bucket_Sync_SObj::handle_bi_removal(const RGWBucketInfo& bucket_info,
                                              optional_yield y)
{
  std::set<rgw_bucket> sources_set;
  std::set<rgw_bucket> dests_set;

  if (bucket_info.sync_policy) {
    bucket_info.sync_policy->get_potential_related_buckets(bucket_info.bucket,
                                                           &sources_set,
                                                           &dests_set);
  }

  std::vector<rgw_bucket> removed_sources;
  removed_sources.reserve(sources_set.size());
  for (auto& e : sources_set) {
    removed_sources.push_back(e);
  }

  std::vector<rgw_bucket> removed_dests;
  removed_dests.reserve(dests_set.size());
  for (auto& e : dests_set) {
    removed_dests.push_back(e);
  }

  std::vector<rgw_bucket> added_sources;
  std::vector<rgw_bucket> added_dests;

  return do_update_hints(bucket_info,
                         added_dests,
                         removed_dests,
                         added_sources,
                         removed_sources,
                         y);
}

int RGWSI_Bucket_Sync_SObj::handle_bi_update(RGWBucketInfo& bucket_info,
                                             RGWBucketInfo *orig_bucket_info,
                                             optional_yield y)
{
  std::set<rgw_bucket> orig_sources;
  std::set<rgw_bucket> orig_dests;

  if (orig_bucket_info &&
      orig_bucket_info->sync_policy) {
    orig_bucket_info->sync_policy->get_potential_related_buckets(bucket_info.bucket,
                                                                &orig_sources,
                                                                &orig_dests);
  }

  std::set<rgw_bucket> sources;
  std::set<rgw_bucket> dests;
  if (bucket_info.sync_policy) {
    bucket_info.sync_policy->get_potential_related_buckets(bucket_info.bucket,
                                                           &sources,
                                                           &dests);
  }

  std::vector<rgw_bucket> removed_sources;
  std::vector<rgw_bucket> added_sources;
  bool found = diff_sets(orig_sources, sources, &added_sources, &removed_sources);
  ldout(cct, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ": orig_sources=" << orig_sources << " new_sources=" << sources << dendl;
  ldout(cct, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ":  potential sources added=" << added_sources << " removed=" << removed_sources << dendl;
  
  std::vector<rgw_bucket> removed_dests;
  std::vector<rgw_bucket> added_dests;
  found = found || diff_sets(orig_dests, dests, &added_dests, &removed_dests);

  ldout(cct, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ": orig_dests=" << orig_dests << " new_dests=" << dests << dendl;
  ldout(cct, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ":  potential dests added=" << added_dests << " removed=" << removed_dests << dendl;

  if (!found) {
    return 0;
  }

  return do_update_hints(bucket_info,
                         added_dests,
                         removed_dests,
                         added_sources,
                         removed_sources,
                         y);
}

int RGWSI_Bucket_Sync_SObj::get_bucket_sync_hints(const rgw_bucket& bucket,
                                                  std::set<rgw_bucket> *sources,
                                                  std::set<rgw_bucket> *dests,
                                                  optional_yield y)
{
  if (!sources && !dests) {
    return 0;
  }

  if (sources) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr.get_sources_obj(bucket));
    int r = index.read(y);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to update sources index for bucket=" << bucket << " r=" << r << dendl;
      return r;
    }

    index.get_entities(bucket, dests);

    if (!bucket.bucket_id.empty()) {
      rgw_bucket b = bucket;
      b.bucket_id.clear();
      index.get_entities(bucket, dests);
    }
  }

  if (dests) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr.get_dests_obj(bucket));
    int r = index.read(y);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to read targets index for bucket=" << bucket << " r=" << r << dendl;
      return r;
    }

    index.get_entities(bucket, dests);

    if (!bucket.bucket_id.empty()) {
      rgw_bucket b = bucket;
      b.bucket_id.clear();
      index.get_entities(bucket, dests);
    }
  }

  return 0;
}
