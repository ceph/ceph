// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_otp.h"
#include <list>
#include <fmt/format.h>
#include "services/svc_cls.h"
#include "services/svc_mdlog.h"
#include "services/svc_sys_obj.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_zone.h"


class MetadataObject : public RGWMetadataObject {
public:
  std::list<rados::cls::otp::otp_info_t> devices;

  MetadataObject(std::list<rados::cls::otp::otp_info_t> devices,
                 const obj_version& v, ceph::real_time m)
    : RGWMetadataObject(v, m), devices(std::move(devices))
  {}

  void dump(Formatter* f) const override {
    encode_json("devices", devices, f);
  }
};

class MetadataHandler : public RGWMetadataHandler {
  RGWSI_SysObj& sysobj;
  RGWSI_Cls::MFA& mfa;
  RGWSI_MDLog& mdlog;
  const RGWZoneParams& zone;
 public:
  MetadataHandler(RGWSI_SysObj& sysobj, RGWSI_Cls::MFA& mfa,
                  RGWSI_MDLog& mdlog, const RGWZoneParams& zone)
    : sysobj(sysobj), mfa(mfa), mdlog(mdlog), zone(zone) {}

  std::string get_type() override { return "otp"; }

  RGWMetadataObject* get_meta_obj(JSONObj* obj,
                                  const obj_version& objv,
                                  const ceph::real_time& mtime) override
  {
    std::list<rados::cls::otp::otp_info_t> devices;
    try {
      JSONDecoder::decode_json("devices", devices, obj);
    } catch (const JSONDecoder::err&) {
      return nullptr;
    }
    return new MetadataObject(std::move(devices), objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj,
          optional_yield y, const DoutPrefixProvider* dpp) override
  {
    std::list<rados::cls::otp::otp_info_t> devices;
    RGWObjVersionTracker objv;
    ceph::real_time mtime;

    int r = mfa.list_mfa(dpp, entry, &devices, &objv, &mtime, y);
    if (r < 0) {
      return r;
    }

    *obj = new MetadataObject(std::move(devices), objv.read_version, mtime);
    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv, optional_yield y,
          const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    auto otp_obj = static_cast<MetadataObject*>(obj);
    int r = mfa.set_mfa(dpp, entry, otp_obj->devices, true,
                        &objv, obj->get_mtime(), y);
    if (r < 0) {
      return r;
    }
    return mdlog.complete_entry(dpp, y, "otp", entry, &objv);
  }

  int remove(std::string& entry, RGWObjVersionTracker& objv,
             optional_yield y, const DoutPrefixProvider* dpp) override
  {
    int r = rgw_delete_system_obj(dpp, &sysobj, zone.otp_pool, entry, &objv, y);
    if (r < 0) {
      return r;
    }
    return mdlog.complete_entry(dpp, y, "otp", entry, &objv);
  }

  int mutate(const std::string& entry,
             const ceph::real_time& mtime,
             RGWObjVersionTracker* objv,
             optional_yield y,
             const DoutPrefixProvider* dpp,
             RGWMDLogStatus op_type,
             std::function<int()> f) override
  {
    int r = f();
    if (r < 0) {
      return r;
    }
    return mdlog.complete_entry(dpp, y, "otp", entry, objv);
  }


  int list_keys_init(const DoutPrefixProvider* dpp,
                     const std::string& marker, void** phandle) override
  {
    auto lister = std::make_unique<RGWMetadataLister>(sysobj.get_pool(zone.otp_pool));
    int r = lister->init(dpp, marker, ""); // no prefix
    if (r < 0) {
      return r;
    }
    *phandle = lister.release();
    return 0;
  }

  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_next(dpp, max, keys, truncated);
  }

  void list_keys_complete(void* handle) override
  {
    delete static_cast<RGWMetadataLister*>(handle);
  }

  std::string get_marker(void* handle) override
  {
    auto lister = static_cast<RGWMetadataLister*>(handle);
    return lister->get_marker();
  }
};


// public interface
namespace rgwrados::otp {

std::string get_meta_key(const rgw_user& user)
{
  return fmt::format("otp:user:{}", user.to_str());
}

auto create_metadata_handler(RGWSI_SysObj& sysobj, RGWSI_Cls& cls,
                             RGWSI_MDLog& mdlog, const RGWZoneParams& zone)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<MetadataHandler>(sysobj, cls.mfa, mdlog, zone);
}

} // namespace rgwrados::otp
