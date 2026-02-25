// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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
#include "rgw_oidc_provider.h"
#include "rgw_string.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"

namespace rgwrados::oidc {

// OIDCProviderInfo is stored in rados objects named "{tenant}oidc_url.{url}"
static constexpr std::string_view oidc_url_oid_prefix = "oidc_url.";

// Metadata keys use '$' as separator: "{tenant}${url}"
static constexpr char metadata_key_separator = '$';

// Build the rados oid from tenant and url
static std::string
get_oidc_oid(std::string_view tenant, std::string_view url)
{
  return string_cat_reserve(tenant, oidc_url_oid_prefix, url);
}

// Build the metadata key from tenant and url
std::string
get_oidc_metadata_key(std::string_view tenant, std::string_view url)
{
  return string_cat_reserve(
      tenant, std::string_view(&metadata_key_separator, 1), url);
}

// Parse metadata key "{tenant}${url}" into tenant and url
void
parse_oidc_metadata_key(
    const std::string& key,
    std::string& tenant,
    std::string& url)
{
  auto pos = key.find(metadata_key_separator);
  if (pos == std::string::npos) {
    tenant.clear();
    url = key;
  } else {
    tenant = key.substr(0, pos);
    url = key.substr(pos + 1);
  }
}

// Convert rados oid "{tenant}oidc_url.{url}" to metadata key "{tenant}${url}"
static std::string
oid_to_metadata_key(const std::string& oid)
{
  auto pos = oid.find(oidc_url_oid_prefix);
  if (pos == std::string::npos) {
    return oid; // shouldn't happen
  }
  std::string tenant = oid.substr(0, pos);
  std::string url = oid.substr(pos + oidc_url_oid_prefix.size());
  return get_oidc_metadata_key(tenant, url);
}

static rgw_raw_obj
get_oidc_obj(
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::string_view url)
{
  return {zone.oidc_pool, get_oidc_oid(tenant, url)};
}


int
read(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::string_view url,
    RGWOIDCProviderInfo& info,
    ceph::real_time* pmtime,
    RGWObjVersionTracker* pobjv)
{
  const rgw_raw_obj& obj = get_oidc_obj(zone, tenant, url);

  bufferlist bl;
  int r = rgw_get_system_obj(
      &sysobj, obj.pool, obj.oid, bl, pobjv,
      pmtime, y, dpp);
  if (r < 0) {
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (const buffer::error&) {
    ldpp_dout(
        dpp, 0) << "ERROR: failed to decode OIDC provider info from pool: "
                      << obj.pool << ": " << obj.oid << dendl;
    return -EIO;
  }

  return 0;
}

int
write(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    const RGWOIDCProviderInfo& info,
    RGWObjVersionTracker& objv,
    ceph::real_time mtime,
    bool exclusive)
{
  const std::string url = url_remove_prefix(info.provider_url);
  const rgw_raw_obj obj = get_oidc_obj(zone, info.tenant, url);

  bufferlist bl;
  encode(info, bl);

  int r = rgw_put_system_obj(
      dpp, &sysobj, obj.pool, obj.oid,
      bl, exclusive, &objv, mtime, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write OIDC provider obj " << obj.oid
        << " with: " << cpp_strerror(r) << dendl;
    return r;
  }


  // record in the mdlog on success
  if (mdlog) {
    const std::string oidc_key = get_oidc_metadata_key(info.tenant, url);
    return mdlog->complete_entry(dpp, y, "oidc", oidc_key, &objv);
  }
  return 0;
}

int
remove(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::string_view url,
    RGWObjVersionTracker& objv)
{
  const std::string oidc_key = get_oidc_metadata_key(tenant, url);
  const rgw_raw_obj obj = get_oidc_obj(zone, tenant, url);

  // delete OIDC provider info
  int r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, &objv, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove OIDC provider obj "
        << obj.oid << " with: " << cpp_strerror(r) << dendl;
    return r;
  }


  // record in the mdlog on success
  if (mdlog) {
    return mdlog->complete_entry(dpp, y, "oidc", oidc_key, &objv);
  }
  return 0;
}

int
list(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    librados::Rados& rados,
    const RGWZoneParams& zone,
    std::string_view tenant,
    std::vector<RGWOIDCProviderInfo>& providers)
{
  // Prefix format: "{tenant}oidc_url." to match original implementation
  std::string prefix = string_cat_reserve(tenant, oidc_url_oid_prefix);
  auto& pool = zone.oidc_pool;

  // List all objects with the tenant prefix
  auto listing = sysobj.get_pool(pool);
  auto op = listing.op();
  int r = op.init(dpp, "", prefix);
  if (r < 0) {
    return r;
  }

  std::vector<std::string> oids;
  bool truncated = false;
  do {
    r = op.get_next(dpp, 1000, &oids, &truncated);
    if (r == -ENOENT) {
      return 0;
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: listing OIDC providers in pool "
          << pool << " with prefix " << prefix << ": " << cpp_strerror(r) <<
 dendl;
      return r;
    }

    for (const auto& oid : oids) {
      bufferlist bl;
      r = rgw_get_system_obj(&sysobj, pool, oid, bl, nullptr, nullptr, y, dpp);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to read OIDC provider " << oid
            << ": " << cpp_strerror(r) << dendl;
        return r;
      }

      RGWOIDCProviderInfo info;
      try {
        auto iter = bl.cbegin();
        decode(info, iter);
      } catch (const buffer::error& err) {
        ldpp_dout(dpp, 0) << "ERROR: failed to decode OIDC provider info from "
            << oid << dendl;
        return -EIO;
      }

      providers.push_back(std::move(info));
    }
  } while (truncated);

  return 0;
}


class MetadataObject : public RGWMetadataObject {
  RGWOIDCProviderInfo info;

public:
  MetadataObject(
      const RGWOIDCProviderInfo& info,
      const obj_version& v,
      real_time m)
    : RGWMetadataObject(v, m), info(info)
  {
  }

  void
  dump(Formatter* f) const override
  {
    info.dump(f);
  }

  RGWOIDCProviderInfo&
  get_oidc_info()
  {
    return info;
  }
};

class MetadataLister : public RGWMetadataLister {
public:
  using RGWMetadataLister::RGWMetadataLister;

  void
  filter_transform(
      std::vector<std::string>& oids,
      std::list<std::string>& keys) override
  {
    // convert oid format "{tenant}oidc_url.{url}" to metadata key "{tenant}${url}"
    // filter out any objects that don't contain "oidc_url."
    for (const auto& oid : oids) {
      if (oid.find(oidc_url_oid_prefix) != std::string::npos) {
        keys.push_back(oid_to_metadata_key(oid));
      }
    }
  }
};

class MetadataHandler : public RGWMetadataHandler {
  librados::Rados& rados;
  RGWSI_SysObj& sysobj;
  RGWSI_MDLog& mdlog;
  const RGWZoneParams& zone;

public:
  MetadataHandler(
      librados::Rados& rados,
      RGWSI_SysObj& sysobj,
      RGWSI_MDLog& mdlog,
      const RGWZoneParams& zone)
    : rados(rados), sysobj(sysobj), mdlog(mdlog), zone(zone)
  {
  }

  std::string
  get_type() final { return "oidc"; }

  RGWMetadataObject*
  get_meta_obj(
      JSONObj* jo,
      const obj_version& objv,
      const ceph::real_time& mtime) override
  {
    RGWOIDCProviderInfo info;

    try {
      info.decode_json(jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new MetadataObject(info, objv, mtime);
  }

  int
  get(
      std::string& entry,
      RGWMetadataObject** obj,
      optional_yield y,
      const DoutPrefixProvider* dpp) override
  {
    std::string tenant;
    std::string url;
    parse_oidc_metadata_key(entry, tenant, url);

    RGWOIDCProviderInfo info;
    ceph::real_time mtime;
    RGWObjVersionTracker objv;
    int ret = read(dpp, y, sysobj, zone, tenant, url, info, &mtime, &objv);
    if (ret < 0) {
      return ret;
    }

    *obj = new MetadataObject(info, objv.read_version, mtime);
    return 0;
  }

  int
  put(
      std::string& entry,
      RGWMetadataObject* obj,
      RGWObjVersionTracker& objv_tracker,
      optional_yield y,
      const DoutPrefixProvider* dpp,
      RGWMDLogSyncType type,
      bool from_remote_zone) override
  {
    auto robj = static_cast<MetadataObject*>(obj);
    auto& info = robj->get_oidc_info();
    auto mtime = robj->get_mtime();

    constexpr bool exclusive = false;
    int ret = write(
        dpp, y, sysobj, &mdlog, rados, zone,
        info, objv_tracker, mtime, exclusive);
    return ret < 0 ? ret : STATUS_APPLIED;
  }

  int
  remove(
      std::string& entry,
      RGWObjVersionTracker& objv_tracker,
      optional_yield y,
      const DoutPrefixProvider* dpp) override
  {
    std::string tenant;
    std::string url;
    parse_oidc_metadata_key(entry, tenant, url);

    return oidc::remove(
        dpp, y, sysobj, &mdlog, rados, zone,
        tenant, url, objv_tracker);
  }

  int
  mutate(
      const std::string& entry,
      const ceph::real_time& mtime,
      RGWObjVersionTracker* objv_tracker,
      optional_yield y,
      const DoutPrefixProvider* dpp,
      RGWMDLogStatus op_type,
      std::function<int()> f) override
  {
    return -ENOTSUP; // unused
  }

  int
  list_keys_init(
      const DoutPrefixProvider* dpp,
      const std::string& marker,
      void** phandle) override
  {
    const auto& pool = zone.oidc_pool;
    auto lister = std::make_unique<MetadataLister>(sysobj.get_pool(pool));
    // Use empty prefix - we filter by "oidc_url." presence in filter_transform
    // This is needed because OID format is "{tenant}oidc_url.{url}"
    int ret = lister->init(dpp, marker, "");
    if (ret < 0) {
      return ret;
    }
    *phandle = lister.release(); // release ownership
    return 0;
  }

  int
  list_keys_next(
      const DoutPrefixProvider* dpp,
      void* handle,
      int max,
      std::list<std::string>& keys,
      bool* truncated) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_next(dpp, max, keys, truncated);
  }

  void
  list_keys_complete(void* handle) override
  {
    delete static_cast<RGWMetadataLister*>(handle);
  }

  std::string
  get_marker(void* handle) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_marker();
  }
};


auto
create_metadata_handler(
    librados::Rados& rados,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog& mdlog,
    const RGWZoneParams& zone)
  -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(rados, sysobj, mdlog, zone);
}

} // rgwrados::oidc

