// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "topic.h"
#include "common/errno.h"
#include "account.h"
#include "rgw_account.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_notify.h"
#include "rgw_pubsub.h"
#include "rgw_rados.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"
#include "svc_sys_obj_cache.h"
#include "topics.h"

namespace rgwrados::topic {

static const std::string oid_prefix = "topic.";
static constexpr std::string_view buckets_oid_prefix = "buckets.";

static rgw_raw_obj get_topic_obj(const RGWZoneParams& zone,
                                 std::string_view metadata_key)
{
  std::string oid = string_cat_reserve(oid_prefix, metadata_key);
  return {zone.topics_pool, std::move(oid)};
}

static rgw_raw_obj get_buckets_obj(const RGWZoneParams& zone,
                                   std::string_view metadata_key)
{
  std::string oid = string_cat_reserve(buckets_oid_prefix, metadata_key);
  return {zone.topics_pool, std::move(oid)};
}


int read(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, RGWSI_SysObj_Cache* cache_svc,
         const RGWZoneParams& zone, const std::string& topic_key,
         rgw_pubsub_topic& info, RGWChainedCacheImpl<cache_entry>& cache,
         ceph::real_time* pmtime, RGWObjVersionTracker* pobjv)
{
  if (auto e = cache.find(topic_key)) {
    if (pmtime) {
      *pmtime = e->mtime;
    }
    if (pobjv) {
      *pobjv = std::move(e->objv);
    }
    info = std::move(e->info);
    return 0;
  }

  const rgw_raw_obj obj = get_topic_obj(zone, topic_key);

  bufferlist bl;
  cache_entry entry;
  rgw_cache_entry_info cache_info;
  int r = rgw_get_system_obj(&sysobj, obj.pool, obj.oid, bl, &entry.objv,
                             &entry.mtime, y, dpp, nullptr, &cache_info);
  if (r < 0) {
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(entry.info, p);
  } catch (const buffer::error&) {
    return -EIO;
  }

  cache.put(dpp, cache_svc, topic_key, &entry, {&cache_info});

  if (pmtime) {
    *pmtime = entry.mtime;
  }
  if (pobjv) {
    *pobjv = std::move(entry.objv);
  }
  info = std::move(entry.info);
  return 0;
}

int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          librados::Rados& rados, const RGWZoneParams& zone,
          const rgw_pubsub_topic& info, RGWObjVersionTracker& objv,
          ceph::real_time mtime, bool exclusive)
{
  const std::string topic_key = get_topic_metadata_key(info);
  const rgw_raw_obj obj = get_topic_obj(zone, topic_key);

  bufferlist bl;
  encode(info, bl);

  int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid,
                             bl, exclusive, &objv, mtime, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write topic obj " << obj.oid
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (const auto* id = std::get_if<rgw_account_id>(&info.owner); id) {
    // link the topic to its account
    const auto& topics = account::get_topics_obj(zone, *id);
    r = topics::add(dpp, y, rados, topics, info, false,
                    std::numeric_limits<uint32_t>::max());
    if (r < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not link topic to account "
          << *id << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "topic", topic_key, &objv);
  }
  return 0;
}

int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           librados::Rados& rados, const RGWZoneParams& zone,
           const std::string& tenant, const std::string& name,
           RGWObjVersionTracker& objv)
{
  const std::string topic_key = get_topic_metadata_key(tenant, name);

  // delete topic info
  const rgw_raw_obj topic = get_topic_obj(zone, topic_key);
  int r = rgw_delete_system_obj(dpp, &sysobj, topic.pool, topic.oid, &objv, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove topic obj "
        << topic.oid << " with: " << cpp_strerror(r) << dendl;
    return r;
  }

  // delete the buckets object
  const rgw_raw_obj buckets = get_buckets_obj(zone, topic_key);
  r = rgw_delete_system_obj(dpp, &sysobj, buckets.pool,
                            buckets.oid, nullptr, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "WARNING: failed to remove topic buckets obj "
        << buckets.oid << " with: " << cpp_strerror(r) << dendl;
  } // not fatal

  if (rgw::account::validate_id(tenant)) {
    // unlink the name from its account
    const auto& topics = account::get_topics_obj(zone, tenant);
    r = topics::remove(dpp, y, rados, topics, name);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not unlink from account "
          << tenant << ": " << cpp_strerror(r) << dendl;
    } // not fatal
  }

  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "topic", topic_key, &objv);
  }
  return 0;
}


int link_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                librados::Rados& rados, const RGWZoneParams& zone,
                const std::string& topic_key,
                const std::string& bucket_key)
{
  const rgw_raw_obj obj = get_buckets_obj(zone, topic_key);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  op.omap_set({{bucket_key, bufferlist{}}});

  return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
}

int unlink_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                  librados::Rados& rados, const RGWZoneParams& zone,
                  const std::string& topic_key,
                  const std::string& bucket_key)
{
  const rgw_raw_obj obj = get_buckets_obj(zone, topic_key);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  op.omap_rm_keys({{bucket_key}});

  return rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, y);
}

int list_buckets(const DoutPrefixProvider* dpp, optional_yield y,
                 librados::Rados& rados, const RGWZoneParams& zone,
                 const std::string& topic_key,
                 const std::string& marker, int max_items,
                 std::set<std::string>& bucket_keys,
                 std::string& next_marker)
{
  const rgw_raw_obj obj = get_buckets_obj(zone, topic_key);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  std::set<std::string> keys;
  bool more = false;
  int rval = 0;
  op.omap_get_keys2(marker, max_items, &keys, &more, &rval);
  r = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, nullptr, y);
  if (r == -ENOENT) {
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (rval < 0) {
    return rval;
  }

  if (more && !keys.empty()) {
    next_marker = *keys.rbegin();
  } else {
    next_marker.clear();
  }
  bucket_keys.merge(std::move(keys));

  return 0;
}


class MetadataObject : public RGWMetadataObject {
  rgw_pubsub_topic info;
public:
  MetadataObject(const rgw_pubsub_topic& info, const obj_version& v, real_time m)
    : RGWMetadataObject(v, m), info(info) {}

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  rgw_pubsub_topic& get_topic_info() {
    return info;
  }
};

class MetadataLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  virtual void filter_transform(std::vector<std::string>& oids,
                                std::list<std::string>& keys) {
    // remove the oid prefix from keys
    constexpr auto trim = [] (const std::string& oid) {
      return oid.substr(oid_prefix.size());
    };
    std::transform(oids.begin(), oids.end(),
                   std::back_inserter(keys),
                   trim);
  }
};

class MetadataHandler : public RGWMetadataHandler {
  RGWSI_SysObj& sysobj;
  RGWSI_SysObj_Cache* cache_svc;
  RGWSI_MDLog& mdlog;
  librados::Rados& rados;
  const RGWZoneParams& zone;
  RGWChainedCacheImpl<cache_entry>& cache;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, RGWSI_SysObj_Cache* cache_svc,
                  RGWSI_MDLog& mdlog, librados::Rados& rados,
                  const RGWZoneParams& zone,
                  RGWChainedCacheImpl<cache_entry>& cache)
    : sysobj(sysobj), cache_svc(cache_svc), mdlog(mdlog),
      rados(rados), zone(zone), cache(cache)
  {}

  std::string get_type() final { return "topic";  }

  RGWMetadataObject* get_meta_obj(JSONObj *jo,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    rgw_pubsub_topic info;

    try {
      info.decode_json(jo);
    } catch (JSONDecoder:: err& e) {
      return nullptr;
    }

    return new MetadataObject(info, objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj,
          optional_yield y, const DoutPrefixProvider* dpp) override
  {
    cache_entry e;
    int ret = read(dpp, y, sysobj, cache_svc, zone, entry,
                   e.info, cache, &e.mtime, &e.objv);
    if (ret < 0) {
      return ret;
    }

    *obj = new MetadataObject(e.info, e.objv.read_version, e.mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    auto robj = static_cast<MetadataObject*>(obj);
    auto& info = robj->get_topic_info();
    auto mtime = robj->get_mtime();

    constexpr bool exclusive = false;
    int r = write(dpp, y, sysobj, &mdlog, rados, zone,
                  info, objv_tracker, mtime, exclusive);
    if (r < 0) {
      return r;
    }
    if (!info.dest.push_endpoint.empty() && info.dest.persistent &&
        !info.dest.persistent_queue.empty()) {
      librados::IoCtx ioctx;
      r = rgw_init_ioctx(dpp, &rados, zone.notif_pool, ioctx, true, false);
      if (r >= 0) {
        r = rgw::notify::add_persistent_topic(dpp, ioctx, info.dest.persistent_queue, y);
      }
      if (r < 0) {
        ldpp_dout(dpp, 1) << "ERROR: failed to create queue for persistent topic "
            << info.dest.persistent_queue << " with: " << cpp_strerror(r) << dendl;
        return r;
      }
    }
    return STATUS_APPLIED;
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    std::string name;
    std::string tenant;
    parse_topic_metadata_key(entry, tenant, name);

    rgw_pubsub_topic info;
    int r = read(dpp, y, sysobj, cache_svc, zone, entry,
                 info, cache, nullptr, &objv_tracker);
    if (r < 0) {
      return r;
    }

    r = topic::remove(dpp, y, sysobj, &mdlog, rados, zone,
                      tenant, name, objv_tracker);
    if (r < 0) {
      return r;
    }

    const rgw_pubsub_dest& dest = info.dest;
    if (!dest.push_endpoint.empty() && dest.persistent &&
        !dest.persistent_queue.empty()) {
      // delete persistent topic queue
      librados::IoCtx ioctx;
      r = rgw_init_ioctx(dpp, &rados, zone.notif_pool, ioctx, true, false);
      if (r >= 0) {
        r = rgw::notify::remove_persistent_topic(dpp, ioctx, dest.persistent_queue, y);
      }
      if (r < 0 && r != -ENOENT) {
        ldpp_dout(dpp, 1) << "Failed to delete queue for persistent topic: "
                          << name << " with error: " << r << dendl;
      } // not fatal
    }
    return 0;
  }

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override
  {
    return -ENOTSUP; // unused
  }

  int list_keys_init(const DoutPrefixProvider* dpp,
                     const std::string& marker,
                     void** phandle) override
  {
    const auto& pool = zone.topics_pool;
    auto lister = std::make_unique<MetadataLister>(sysobj.get_pool(pool));
    int ret = lister->init(dpp, marker, oid_prefix);
    if (ret < 0) {
      return ret;
    }
    *phandle = lister.release(); // release ownership
    return 0;
  }

  int list_keys_next(const DoutPrefixProvider* dpp,
                     void* handle, int max,
                     std::list<std::string>& keys,
                     bool* truncated) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_next(dpp, max, keys, truncated);
  }

  void list_keys_complete(void *handle) override
  {
    delete static_cast<RGWMetadataLister*>(handle);
  }

  std::string get_marker(void *handle) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_marker();
  }
};


auto create_metadata_handler(RGWSI_SysObj& sysobj,
                             RGWSI_SysObj_Cache* cache_svc,
                             RGWSI_MDLog& mdlog, librados::Rados& rados,
                             const RGWZoneParams& zone,
                             RGWChainedCacheImpl<cache_entry>& cache)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, cache_svc, mdlog,
                                           rados, zone, cache);
}

} // rgwrados::topic
