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

#include "ext_authz.h"

#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_ext_authz.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_mdlog.h"

#define dout_subsys ceph_subsys_rgw

namespace rgwrados::ext_authz {

// The config is a single object in the zone's oidc pool, next to the OIDC
// providers. The prefix cannot collide with "{tenant}oidc_url.{url}".
static constexpr std::string_view oid_prefix = "ext_authz.";
static constexpr std::string_view metadata_section = "ext_authz";

static rgw_raw_obj
get_obj(const RGWZoneParams& zone)
{
  return {zone.oidc_pool, std::string(oid_prefix) + std::string(metadata_key)};
}

int
read(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    const RGWZoneParams& zone,
    RGWExtAuthzConfig& config,
    ceph::real_time* pmtime,
    RGWObjVersionTracker* pobjv)
{
  const rgw_raw_obj obj = get_obj(zone);

  bufferlist bl;
  int r = rgw_get_system_obj(&sysobj, obj.pool, obj.oid, bl, pobjv, pmtime,
                             y, dpp);
  if (r < 0) {
    return r;
  }

  try {
    auto p = bl.cbegin();
    decode(config, p);
  } catch (const buffer::error&) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode the external authorizer "
        "config from " << obj.pool << ":" << obj.oid << dendl;
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
    const RGWZoneParams& zone,
    const RGWExtAuthzConfig& config,
    ceph::real_time mtime,
    RGWObjVersionTracker* objv)
{
  const rgw_raw_obj obj = get_obj(zone);

  bufferlist bl;
  encode(config, bl);

  constexpr bool exclusive = false;
  int r = rgw_put_system_obj(dpp, &sysobj, obj.pool, obj.oid, bl, exclusive,
                             objv, mtime, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to write the external authorizer "
        "config: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (mdlog) {
    return mdlog->complete_entry(dpp, y, std::string(metadata_section),
                                 std::string(metadata_key), objv);
  }
  return 0;
}

int
remove(
    const DoutPrefixProvider* dpp,
    optional_yield y,
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog* mdlog,
    const RGWZoneParams& zone,
    RGWObjVersionTracker* objv)
{
  const rgw_raw_obj obj = get_obj(zone);

  int r = rgw_delete_system_obj(dpp, &sysobj, obj.pool, obj.oid, objv, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to remove the external authorizer "
        "config: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (mdlog) {
    return mdlog->complete_entry(dpp, y, std::string(metadata_section),
                                 std::string(metadata_key), objv);
  }
  return 0;
}


class MetadataObject : public RGWMetadataObject {
  RGWExtAuthzConfig config;

 public:
  MetadataObject(const RGWExtAuthzConfig& config,
                 const obj_version& v, real_time m)
    : RGWMetadataObject(v, m), config(config) {}

  // metadata is read by admins and by sync, which needs the real credential.
  // Access is limited to the 'metadata' capability, as for user keys.
  void dump(Formatter* f) const override { config.dump(f, false); }

  RGWExtAuthzConfig& get_config() { return config; }
};

class MetadataLister : public RGWMetadataLister {
 public:
  using RGWMetadataLister::RGWMetadataLister;

  void filter_transform(std::vector<std::string>& oids,
                        std::list<std::string>& keys) override
  {
    for (const auto& oid : oids) {
      keys.push_back(oid.substr(oid_prefix.size()));
    }
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

  std::string get_type() final { return std::string(metadata_section); }

  RGWMetadataObject* get_meta_obj(JSONObj* jo, const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    RGWExtAuthzConfig config;
    try {
      config.decode_json(jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }
    return new MetadataObject(config, objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj, optional_yield y,
          const DoutPrefixProvider* dpp) override
  {
    if (entry != metadata_key) {
      return -ENOENT;
    }
    RGWExtAuthzConfig config;
    ceph::real_time mtime;
    RGWObjVersionTracker objv;
    int ret = read(dpp, y, sysobj, zone, config, &mtime, &objv);
    if (ret < 0) {
      return ret;
    }
    *obj = new MetadataObject(config, objv.read_version, mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker, optional_yield y,
          const DoutPrefixProvider* dpp, RGWMDLogSyncType type,
          bool from_remote_zone) override
  {
    if (entry != metadata_key) {
      return -EINVAL;
    }
    auto robj = static_cast<MetadataObject*>(obj);
    int ret = write(dpp, y, sysobj, &mdlog, zone, robj->get_config(),
                    robj->get_mtime(), &objv_tracker);
    return ret < 0 ? ret : STATUS_APPLIED;
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider* dpp) override
  {
    if (entry != metadata_key) {
      return -ENOENT;
    }
    return ext_authz::remove(dpp, y, sysobj, &mdlog, zone, &objv_tracker);
  }

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override
  {
    return -ENOTSUP; // unused
  }

  int list_keys_init(const DoutPrefixProvider* dpp, const std::string& marker,
                     void** phandle) override
  {
    auto lister = std::make_unique<MetadataLister>(
        sysobj.get_pool(zone.oidc_pool));
    int ret = lister->init(dpp, marker, std::string(oid_prefix));
    if (ret < 0) {
      return ret;
    }
    *phandle = lister.release();
    return 0;
  }

  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override
  {
    return static_cast<RGWMetadataLister*>(handle)->get_next(
        dpp, max, keys, truncated);
  }

  void list_keys_complete(void* handle) override
  {
    delete static_cast<RGWMetadataLister*>(handle);
  }

  std::string get_marker(void* handle) override
  {
    return static_cast<RGWMetadataLister*>(handle)->get_marker();
  }
};

auto
create_metadata_handler(
    RGWSI_SysObj& sysobj,
    RGWSI_MDLog& mdlog,
    const RGWZoneParams& zone)
  -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, mdlog, zone);
}

} // rgwrados::ext_authz
