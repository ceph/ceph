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

#include "oidc.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"
#include "rgw_oidc_provider.h"

namespace rgwrados::oidc {

static constexpr std::string_view oidc_url_oid_prefix = "oidc_url.";
static const std::string oid_prefix = "oidc.";

static std::string oidc_provider_oid(std::string_view account,
                                     std::string_view prefix,
                                     std::string_view url)
{
  return string_cat_reserve(account, prefix, url);
}

int read(const DoutPrefixProvider* dpp, optional_yield y,
         RGWSI_SysObj& sysobj, const RGWZoneParams& zone,
         const std::string_view account, std::string_view url,
         const std::string& id, RGWOIDCProviderInfo& info,
         ceph::real_time* mtime,
         RGWObjVersionTracker* objv,
         rgw_cache_entry_info* cache_info)
{
  std::string oid = oidc_provider_oid(account, oidc_url_oid_prefix, url);
  bufferlist bl;

  int ret = rgw_get_system_obj(&sysobj, zone.oidc_pool, oid, bl,
                               objv, mtime, y, dpp, nullptr, cache_info);
  if (ret < 0) {
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode oidc provider info from pool: " << zone.oidc_pool.name <<
                  ": " << url << dendl;
    return -EIO;
  }

  return 0;
}

int write(const DoutPrefixProvider* dpp, optional_yield y,
          RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
          const RGWZoneParams& zone, const RGWOIDCProviderInfo& info,
          RGWObjVersionTracker& objv, ceph::real_time mtime,
          bool exclusive)
{
  std::string oid = oidc_provider_oid(info.tenant, oidc_url_oid_prefix,
                                        url_remove_prefix(info.provider_url));
  bufferlist bl;
  using ceph::encode;
  encode(info, bl);
  int ret = rgw_put_system_obj(dpp, &sysobj, zone.oidc_pool, oid, bl, exclusive, &objv, mtime, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write oidc provider obj " << oid
        << " with: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "oidc", info.id, &objv);
  }

  return 0;
}

int remove(const DoutPrefixProvider* dpp, optional_yield y,
           RGWSI_SysObj& sysobj, RGWSI_MDLog* mdlog,
           const RGWZoneParams& zone, const std::string_view tenant,
           std::string_view url,
           RGWObjVersionTracker& objv,
           std::string& id)
{
  std::string oid = oidc_provider_oid(tenant, oidc_url_oid_prefix, url);

  int ret = rgw_delete_system_obj(dpp, &sysobj, zone.oidc_pool, oid, &objv, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting oidc provider url from pool: " << zone.oidc_pool.name << ": "
                  << url << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "oidc", id, &objv);
  }

  return 0;
}


class MetadataObject : public RGWMetadataObject {
  RGWOIDCProviderInfo info;
public:
  MetadataObject(const RGWOIDCProviderInfo& info, const obj_version& v, real_time m)
    : RGWMetadataObject(v, m), info(info) {}

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  RGWOIDCProviderInfo& get_oidc_info() {
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
  RGWSI_MDLog& mdlog;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, RGWSI_MDLog& mdlog,
                  const RGWZoneParams& zone)
    : sysobj(sysobj), mdlog(mdlog), zone(zone) {}

  std::string get_type() final { return "oidc";  }

  RGWMetadataObject* get_meta_obj(JSONObj *jo,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    RGWOIDCProviderInfo info;

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
    RGWOIDCProviderInfo info;
    std::string_view account;
    std::string_view url;
    int ret = read(dpp, y, sysobj,
                   zone, account, url,
                   info.id, info,
                   &info.mtime, &info.objv_tracker);
    if (ret < 0) {
      return ret;
    }

    *obj = new MetadataObject(info, info.objv_tracker.read_version, info.mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    auto robj = static_cast<MetadataObject*>(obj);
    auto& info = robj->get_oidc_info();
    info.mtime = robj->get_mtime();

    constexpr bool exclusive = false;
    int ret = write(dpp, y, sysobj, &mdlog,
                    zone, info, objv_tracker,
                    info.mtime, exclusive);
    return ret < 0 ? ret : STATUS_APPLIED;
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    RGWOIDCProviderInfo info;
    std::string_view account;
    std::string_view url;
    int ret = read(dpp, y, sysobj,
                   zone, account, url,
                   info.id, info,
                   &info.mtime, &info.objv_tracker);
    if (ret < 0) {
      return ret;
    }

    ret = oidc::remove(dpp, y, sysobj, &mdlog,
                 zone, account, url,
                 info.objv_tracker, info.id);
    if (ret < 0) {
      return ret;
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
    const auto& pool = zone.oidc_pool;
    auto lister = std::make_unique<RGWMetadataLister>(sysobj.get_pool(pool));
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
                             RGWSI_MDLog& mdlog,
                             const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, mdlog, zone);
}

}