#include "svc_bucket_sync_sobj.h"
#include "svc_zone.h"
#include "svc_sys_obj_cache.h"
#include "svc_bucket_sobj.h"

#include "rgw_bucket_sync.h"
#include "rgw_zone.h"
#include "rgw_sync_policy.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static string bucket_sync_sources_oid_prefix = "bucket.sync-source-hints";
static string bucket_sync_targets_oid_prefix = "bucket.sync-target-hints";

class RGWSI_Bucket_Sync_SObj_HintIndexManager {
  CephContext *cct;

  struct {
    RGWSI_Zone *zone;
    RGWSI_SysObj *sysobj;
  } svc;

public:
  RGWSI_Bucket_Sync_SObj_HintIndexManager(RGWSI_Zone *_zone_svc,
                                          RGWSI_SysObj *_sysobj_svc) {
    svc.zone = _zone_svc;
    svc.sysobj = _sysobj_svc;

    cct = svc.zone->ctx();
  }

  rgw_raw_obj get_sources_obj(const rgw_bucket& bucket) const;
  rgw_raw_obj get_dests_obj(const rgw_bucket& bucket) const;

  template <typename C1, typename C2>
  int update_hints(const DoutPrefixProvider *dpp, 
                   const RGWBucketInfo& bucket_info,
                   C1& added_dests,
                   C2& removed_dests,
                   C1& added_sources,
                   C2& removed_sources,
                   optional_yield y);
};

RGWSI_Bucket_Sync_SObj::RGWSI_Bucket_Sync_SObj(CephContext *cct) : RGWSI_Bucket_Sync(cct) {
}
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

  hint_index_mgr.reset(new RGWSI_Bucket_Sync_SObj_HintIndexManager(svc.zone, svc.sysobj));
}

int RGWSI_Bucket_Sync_SObj::do_start(optional_yield, const DoutPrefixProvider *dpp)
{
  sync_policy_cache.reset(new RGWChainedCacheImpl<bucket_sync_policy_cache_entry>);
  sync_policy_cache->init(svc.cache);

  return 0;
}

void RGWSI_Bucket_Sync_SObj::get_hint_entities(RGWSI_Bucket_X_Ctx& ctx,
                                               const std::set<rgw_zone_id>& zones,
                                               const std::set<rgw_bucket>& buckets,
                                               std::set<rgw_sync_bucket_entity> *hint_entities,
                                               optional_yield y, const DoutPrefixProvider *dpp)
{
  vector<rgw_bucket> hint_buckets;

  hint_buckets.reserve(buckets.size());

  for (auto& b : buckets) {
    RGWBucketInfo hint_bucket_info;
    int ret = svc.bucket_sobj->read_bucket_info(ctx, b, &hint_bucket_info,
                                                nullptr, nullptr, boost::none,
                                                y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "could not init bucket info for hint bucket=" << b << " ... skipping" << dendl;
      continue;
    }

    hint_buckets.emplace_back(std::move(hint_bucket_info.bucket));
  }

  for (auto& zone : zones) {
    for (auto& b : hint_buckets) {
      hint_entities->insert(rgw_sync_bucket_entity(zone, b));
    }
  }
}

int RGWSI_Bucket_Sync_SObj::resolve_policy_hints(RGWSI_Bucket_X_Ctx& ctx,
                                                 rgw_sync_bucket_entity& self_entity,
                                                 RGWBucketSyncPolicyHandlerRef& handler,
                                                 RGWBucketSyncPolicyHandlerRef& zone_policy_handler,
                                                 std::map<optional_zone_bucket, RGWBucketSyncPolicyHandlerRef>& temp_map,
                                                 optional_yield y,
                                                 const DoutPrefixProvider *dpp)
{
  set<rgw_zone_id> source_zones;
  set<rgw_zone_id> target_zones;

  zone_policy_handler->reflect(dpp, nullptr, nullptr,
                               nullptr, nullptr,
                               &source_zones,
                               &target_zones,
                               false); /* relaxed: also get all zones that we allow to sync to/from */

  std::set<rgw_sync_bucket_entity> hint_entities;

  get_hint_entities(ctx, source_zones, handler->get_source_hints(), &hint_entities, y, dpp);
  get_hint_entities(ctx, target_zones, handler->get_target_hints(), &hint_entities, y, dpp);

  std::set<rgw_sync_bucket_pipe> resolved_sources;
  std::set<rgw_sync_bucket_pipe> resolved_dests;

  for (auto& hint_entity : hint_entities) {
    if (!hint_entity.zone ||
	!hint_entity.bucket) {
      continue; /* shouldn't really happen */
    }

    auto& zid = *hint_entity.zone;
    auto& hint_bucket = *hint_entity.bucket;

    RGWBucketSyncPolicyHandlerRef hint_bucket_handler;

    auto iter = temp_map.find(optional_zone_bucket(zid, hint_bucket));
    if (iter != temp_map.end()) {
      hint_bucket_handler = iter->second;
    } else {
      int r = do_get_policy_handler(ctx, zid, hint_bucket, temp_map, &hint_bucket_handler, y, dpp);
      if (r < 0) {
        ldpp_dout(dpp, 20) << "could not get bucket sync policy handler for hint bucket=" << hint_bucket << " ... skipping" << dendl;
        continue;
      }
    }

    hint_bucket_handler->get_pipes(&resolved_dests,
                                   &resolved_sources,
                                   self_entity); /* flipping resolved dests and sources as these are
                                                    relative to the remote entity */
  }

  handler->set_resolved_hints(std::move(resolved_sources), std::move(resolved_dests));

  return 0;
}

int RGWSI_Bucket_Sync_SObj::do_get_policy_handler(RGWSI_Bucket_X_Ctx& ctx,
                                                  std::optional<rgw_zone_id> zone,
                                                  std::optional<rgw_bucket> _bucket,
                                                  std::map<optional_zone_bucket, RGWBucketSyncPolicyHandlerRef>& temp_map,
                                                  RGWBucketSyncPolicyHandlerRef *handler,
                                                  optional_yield y,
                                                  const DoutPrefixProvider *dpp)
{
  if (!_bucket) {
    *handler = svc.zone->get_sync_policy_handler(zone);
    return 0;
  }

  auto bucket = *_bucket;

  if (bucket.bucket_id.empty()) {
    RGWBucketEntryPoint ep_info;
    int ret = svc.bucket_sobj->read_bucket_entrypoint_info(ctx.ep,
                                                           RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                           &ep_info,
                                                           nullptr, /* objv_tracker */
                                                           nullptr, /* mtime */
                                                           nullptr, /* attrs */
                                                           y,
                                                           dpp,
                                                           nullptr, /* cache_info */
                                                           boost::none /* refresh_version */);
    if (ret < 0) {
      if (ret != -ENOENT) {
        ldout(cct, 0) << "ERROR: svc.bucket->read_bucket_info(bucket=" << bucket << ") returned r=" << ret << dendl;
      }
      return ret;
    }

    bucket = ep_info.bucket;
  }

  string zone_key;
  string bucket_key;

  if (zone && *zone != svc.zone->zone_id()) {
    zone_key = zone->id;
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
  map<string, bufferlist> attrs;

  int r = svc.bucket_sobj->read_bucket_instance_info(ctx.bi,
                                                     bucket_key,
                                                     &bucket_info,
                                                     nullptr,
                                                     &attrs,
                                                     y,
                                                     dpp,
                                                     &cache_info);
  if (r < 0) {
    if (r != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: svc.bucket->read_bucket_instance_info(key=" << bucket_key << ") returned r=" << r << dendl;
    }
    return r;
  }

  auto zone_policy_handler = svc.zone->get_sync_policy_handler(zone);
  if (!zone_policy_handler) {
    ldpp_dout(dpp, 20) << "ERROR: could not find policy handler for zone=" << zone << dendl;
    return -ENOENT;
  }

  e.handler.reset(zone_policy_handler->alloc_child(bucket_info, std::move(attrs)));

  r = e.handler->init(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "ERROR: failed to init bucket sync policy handler: r=" << r << dendl;
    return r;
  }

  temp_map.emplace(optional_zone_bucket{zone, bucket}, e.handler);

  rgw_sync_bucket_entity self_entity(zone.value_or(svc.zone->zone_id()), bucket);

  r = resolve_policy_hints(ctx, self_entity,
                           e.handler,
                           zone_policy_handler,
                           temp_map, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "ERROR: failed to resolve policy hints: bucket_key=" << bucket_key << ", r=" << r << dendl;
    return r;
  }

  if (!sync_policy_cache->put(dpp, svc.cache, cache_key, &e, {&cache_info})) {
    ldpp_dout(dpp, 20) << "couldn't put bucket_sync_policy cache entry, might have raced with data changes" << dendl;
  }

  *handler = e.handler;

  return 0;
}

int RGWSI_Bucket_Sync_SObj::get_policy_handler(RGWSI_Bucket_X_Ctx& ctx,
                                               std::optional<rgw_zone_id> zone,
                                               std::optional<rgw_bucket> _bucket,
                                               RGWBucketSyncPolicyHandlerRef *handler,
                                               optional_yield y,
                                               const DoutPrefixProvider *dpp)
{
  std::map<optional_zone_bucket, RGWBucketSyncPolicyHandlerRef> temp_map;
  return do_get_policy_handler(ctx, zone, _bucket, temp_map, handler, y, dpp);
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
    } else if (*oiter < *niter) {
      removed->push_back(*oiter);
      ++oiter;
    } else {
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
                                                        obj(_obj),
                                                        sysobj(_sysobj_svc->get_obj(obj))
  {
    svc.sysobj = _sysobj_svc;
  }

  template <typename C1, typename C2>
  int update(const DoutPrefixProvider *dpp, 
             const rgw_bucket& entity,
             const RGWBucketInfo& info_source,
             C1 *add,
             C2 *remove,
             optional_yield y);

private:
  template <typename C1, typename C2>
  void update_entries(const rgw_bucket& info_source,
                      const obj_version& info_source_ver,
                      C1 *add,
                      C2 *remove,
                      single_instance_info *instance);

  int read(const DoutPrefixProvider *dpp, optional_yield y);
  int flush(const DoutPrefixProvider *dpp, optional_yield y);

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

template <typename C1, typename C2>
int RGWSI_BS_SObj_HintIndexObj::update(const DoutPrefixProvider *dpp, 
                                       const rgw_bucket& entity,
                                       const RGWBucketInfo& info_source,
                                       C1 *add,
                                       C2 *remove,
                                       optional_yield y)
{
  int r = 0;

  auto& info_source_ver = info_source.objv_tracker.read_version;

#define MAX_RETRIES 25

  for (int i = 0; i < MAX_RETRIES; ++i) {
    if (!has_data) {
      r = read(dpp, y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: cannot update hint index: failed to read: r=" << r << dendl;
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

    r = flush(dpp, y);
    if (r >= 0) {
      return 0;
    }

    if (r != -ECANCELED) {
      ldpp_dout(dpp, 0) << "ERROR: failed to flush hint index: obj=" << obj << " r=" << r << dendl;
      return r;
    }

    invalidate();
  }
  ldpp_dout(dpp, 0) << "ERROR: failed to flush hint index: too many retries (obj=" << obj << "), likely a bug" << dendl;

  return -EIO;
}

template <typename C1, typename C2>
void RGWSI_BS_SObj_HintIndexObj::update_entries(const rgw_bucket& info_source,
                                                const obj_version& info_source_ver,
                                                C1 *add,
                                                C2 *remove,
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

int RGWSI_BS_SObj_HintIndexObj::read(const DoutPrefixProvider *dpp, optional_yield y) {
  RGWObjVersionTracker _ot;
  bufferlist bl;
  int r = sysobj.rop()
    .set_objv_tracker(&_ot) /* forcing read of current version */
    .read(dpp, &bl, y);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading data (obj=" << obj << "), r=" << r << dendl;
    return r;
  }

  ot = _ot;

  if (r >= 0) {
    auto iter = bl.cbegin();
    try {
      decode(info, iter);
      has_data = true;
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to decode entries, ignoring" << dendl;
      info.clear();
    }
  } else {
    info.clear();
  }

  return 0;
}

int RGWSI_BS_SObj_HintIndexObj::flush(const DoutPrefixProvider *dpp, optional_yield y) {
  int r;

  if (!info.empty()) {
    bufferlist bl;
    encode(info, bl);

    r = sysobj.wop()
      .set_objv_tracker(&ot) /* forcing read of current version */
      .write(dpp, bl, y);

  } else { /* remove */
    r = sysobj.wop()
      .set_objv_tracker(&ot)
      .remove(dpp, y);
  }

  if (r < 0) {
    return r;
  }

  return 0;
}

rgw_raw_obj RGWSI_Bucket_Sync_SObj_HintIndexManager::get_sources_obj(const rgw_bucket& bucket) const
{
  rgw_bucket b = bucket;
  b.bucket_id.clear();
  return rgw_raw_obj(svc.zone->get_zone_params().log_pool,
                     bucket_sync_sources_oid_prefix + "." + b.get_key());
}

rgw_raw_obj RGWSI_Bucket_Sync_SObj_HintIndexManager::get_dests_obj(const rgw_bucket& bucket) const
{
  rgw_bucket b = bucket;
  b.bucket_id.clear();
  return rgw_raw_obj(svc.zone->get_zone_params().log_pool,
                     bucket_sync_targets_oid_prefix + "." + b.get_key());
}

template <typename C1, typename C2>
int RGWSI_Bucket_Sync_SObj_HintIndexManager::update_hints(const DoutPrefixProvider *dpp, 
                                                          const RGWBucketInfo& bucket_info,
                                                          C1& added_dests,
                                                          C2& removed_dests,
                                                          C1& added_sources,
                                                          C2& removed_sources,
                                                          optional_yield y)
{
  C1 self_entity = { bucket_info.bucket };

  if (!added_dests.empty() ||
      !removed_dests.empty()) {
    /* update our dests */
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     get_dests_obj(bucket_info.bucket));
    int r = index.update(dpp, bucket_info.bucket,
                         bucket_info,
                         &added_dests,
                         &removed_dests,
                         y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << bucket_info.bucket << " r=" << r << dendl;
      return r;
    }

    /* update dest buckets */
    for (auto& dest_bucket : added_dests) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           get_sources_obj(dest_bucket));
      int r = dep_index.update(dpp, dest_bucket,
                               bucket_info,
                               &self_entity,
                               static_cast<C2 *>(nullptr),
                               y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << dest_bucket << " r=" << r << dendl;
        return r;
      }
    }
    /* update removed dest buckets */
    for (auto& dest_bucket : removed_dests) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           get_sources_obj(dest_bucket));
      int r = dep_index.update(dpp, dest_bucket,
                               bucket_info,
                               static_cast<C1 *>(nullptr),
                               &self_entity,
                               y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << dest_bucket << " r=" << r << dendl;
        return r;
      }
    }
  }

  if (!added_sources.empty() ||
      !removed_sources.empty()) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     get_sources_obj(bucket_info.bucket));
    /* update our sources */
    int r = index.update(dpp, bucket_info.bucket,
                         bucket_info,
                         &added_sources,
                         &removed_sources,
                         y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << bucket_info.bucket << " r=" << r << dendl;
      return r;
    }

    /* update added sources buckets */
    for (auto& source_bucket : added_sources) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           get_dests_obj(source_bucket));
      int r = dep_index.update(dpp, source_bucket,
                               bucket_info,
                               &self_entity,
                               static_cast<C2 *>(nullptr),
                               y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << source_bucket << " r=" << r << dendl;
        return r;
      }
    }
    /* update removed dest buckets */
    for (auto& source_bucket : removed_sources) {
      RGWSI_BS_SObj_HintIndexObj dep_index(svc.sysobj,
                                           get_dests_obj(source_bucket));
      int r = dep_index.update(dpp, source_bucket,
                               bucket_info,
                               static_cast<C1 *>(nullptr),
                               &self_entity,
                               y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to update targets index for bucket=" << source_bucket << " r=" << r << dendl;
        return r;
      }
    }
  }

  return 0;
}

int RGWSI_Bucket_Sync_SObj::handle_bi_removal(const DoutPrefixProvider *dpp, 
                                              const RGWBucketInfo& bucket_info,
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

  return hint_index_mgr->update_hints(dpp, bucket_info,
                                      added_dests,
                                      removed_dests,
                                      added_sources,
                                      removed_sources,
                                      y);
}

int RGWSI_Bucket_Sync_SObj::handle_bi_update(const DoutPrefixProvider *dpp, 
                                             RGWBucketInfo& bucket_info,
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
  ldpp_dout(dpp, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ": orig_sources=" << orig_sources << " new_sources=" << sources << dendl;
  ldpp_dout(dpp, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ":  potential sources added=" << added_sources << " removed=" << removed_sources << dendl;
  
  std::vector<rgw_bucket> removed_dests;
  std::vector<rgw_bucket> added_dests;
  found = found || diff_sets(orig_dests, dests, &added_dests, &removed_dests);

  ldpp_dout(dpp, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ": orig_dests=" << orig_dests << " new_dests=" << dests << dendl;
  ldpp_dout(dpp, 20) << __func__ << "(): bucket=" << bucket_info.bucket << ":  potential dests added=" << added_dests << " removed=" << removed_dests << dendl;

  if (!found) {
    return 0;
  }

  return hint_index_mgr->update_hints(dpp, bucket_info,
                                      dests, /* set all dests, not just the ones that were added */
                                      removed_dests,
                                      sources, /* set all sources, not just that the ones that were added */
                                      removed_sources,
                                      y);
}

int RGWSI_Bucket_Sync_SObj::get_bucket_sync_hints(const DoutPrefixProvider *dpp,
                                                  const rgw_bucket& bucket,
                                                  std::set<rgw_bucket> *sources,
                                                  std::set<rgw_bucket> *dests,
                                                  optional_yield y)
{
  if (!sources && !dests) {
    return 0;
  }

  if (sources) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr->get_sources_obj(bucket));
    int r = index.read(dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to update sources index for bucket=" << bucket << " r=" << r << dendl;
      return r;
    }

    index.get_entities(bucket, sources);

    if (!bucket.bucket_id.empty()) {
      rgw_bucket b = bucket;
      b.bucket_id.clear();
      index.get_entities(b, sources);
    }
  }

  if (dests) {
    RGWSI_BS_SObj_HintIndexObj index(svc.sysobj,
                                     hint_index_mgr->get_dests_obj(bucket));
    int r = index.read(dpp, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read targets index for bucket=" << bucket << " r=" << r << dendl;
      return r;
    }

    index.get_entities(bucket, dests);

    if (!bucket.bucket_id.empty()) {
      rgw_bucket b = bucket;
      b.bucket_id.clear();
      index.get_entities(b, dests);
    }
  }

  return 0;
}
